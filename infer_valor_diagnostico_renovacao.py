"""
Inferência de valores em deals de renovação (E6-bis Onda B frente 3).

Detecta deals abertos criados ≤30 dias em Companies que têm deal Ganho anterior
e pre-populate 14 campos (valor_total_do_diagnostico + 11 valor_lei_* +
lei_principal + linha_de_imposto_categoria) a partir do último Ganho.

Origem: ata backlog Ivan 20/04/2026 (Gap B — valor do diagnóstico em renovação).
"Todo cliente que ajuda renovando esse ano ou faz um diagnóstico novo ou vai
se basear pelo passado, mas tem que estar preenchido esse valor e preenchido
por linha (IR/ICMS/ISS)" — Ivan.

Regra clássica "não sobrescrever": se o deal novo já tem valor em algum campo,
esse campo fica como está. Executivo sempre pode validar/ajustar.

Uso:
    # dry-run (default): imprime o que seria patched, não aplica
    python infer_valor_diagnostico_renovacao.py

    # aplica PATCHs reais
    python infer_valor_diagnostico_renovacao.py --execute

    # teste num deal específico (dry-run a menos que passe --execute)
    python infer_valor_diagnostico_renovacao.py --deal-id 12345 --execute

    # janela customizada (default 30 dias)
    python infer_valor_diagnostico_renovacao.py --lookback-days 60
"""

import argparse
import csv
import datetime
import io
import json
import os
import sys
import time

import requests

# Windows console: forçar utf-8 pra labels com acento não virarem mojibake
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

# ===================================================
# CONFIG
# ===================================================

BASE = "https://api.hubapi.com"
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

# Stage IDs de fechamento (Ganho/Perdido nos 2 pipelines da Brada)
STAGE_GANHO_INCENTIVADOR = "1253324968"
STAGE_GANHO_PROPONENTE = "1253441207"
STAGE_PERDIDO_INCENTIVADOR = "closedlost"
STAGE_PERDIDO_PROPONENTE = "1246571364"
STAGES_FECHADOS = {
    STAGE_GANHO_INCENTIVADOR,
    STAGE_GANHO_PROPONENTE,
    STAGE_PERDIDO_INCENTIVADOR,
    STAGE_PERDIDO_PROPONENTE,
}
STAGES_GANHO = {STAGE_GANHO_INCENTIVADOR, STAGE_GANHO_PROPONENTE}

# 14 campos copiados do Ganho -> deal novo
CAMPOS_RENOVACAO = [
    "valor_total_do_diagnostico",
    "valor_lei_rouanet",
    "valor_lei_do_esporte",
    "valor_lei_do_esporte_estadual",
    "valor_lei_do_bem",
    "valor_lei_da_cultura",
    "valor_lei_da_cultura_municipal",
    "valor_lei_da_crianca_e_do_adolescente",
    "valor_lei_do_idoso",
    "valor_lei_da_reciclagem",
    "valor_pronas",
    "valor_pronon",
    "lei_principal",
    "linha_de_imposto_categoria",
]

# Props extras só pra log/contexto
PROPS_CONTEXTO = ["dealname", "createdate", "closedate", "dealstage", "pipeline"]

LOGS_DIR = os.path.join(os.path.dirname(__file__) or ".", "logs")


# ===================================================
# HTTP
# ===================================================

def req(method, path, **kwargs):
    """HTTP com retry exponencial em 429 e 5xx (padrão enrich_once.py)."""
    url = f"{BASE}{path}" if path.startswith("/") else path
    for attempt in range(4):
        r = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
        if r.status_code == 429 or 500 <= r.status_code < 600:
            if attempt < 3:
                time.sleep(2 ** attempt)
                continue
        return r
    return r


# ===================================================
# HUBSPOT FETCH
# ===================================================

def fetch_deals_candidatos(lookback_days, deal_id=None):
    """Deals candidatos a pre-populate: abertos (não fechados), criados ≤lookback_days
    e com valor_total_do_diagnostico vazio. Se deal_id passado, força esse deal específico."""
    if deal_id:
        r = req(
            "GET",
            f"/crm/v3/objects/deals/{deal_id}",
            params={"properties": ",".join(CAMPOS_RENOVACAO + PROPS_CONTEXTO)},
        )
        if r.status_code != 200:
            print(f"ERRO fetch deal {deal_id}: {r.status_code} {r.text[:200]}")
            return []
        return [r.json()]

    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=lookback_days)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    deals = []
    after = None
    while True:
        body = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "valor_total_do_diagnostico", "operator": "NOT_HAS_PROPERTY"},
                    {"propertyName": "createdate", "operator": "GTE", "value": str(cutoff_ms)},
                ],
            }],
            "properties": CAMPOS_RENOVACAO + PROPS_CONTEXTO,
            "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
            "limit": 100,
        }
        if after:
            body["after"] = after
        r = req("POST", "/crm/v3/objects/deals/search", json=body)
        if r.status_code != 200:
            print(f"ERRO search: {r.status_code} {r.text[:300]}")
            break
        data = r.json()
        for d in data.get("results", []):
            stage = d.get("properties", {}).get("dealstage", "")
            if stage not in STAGES_FECHADOS:
                deals.append(d)
        paging = data.get("paging", {}).get("next")
        if not paging:
            break
        after = paging.get("after")

    return deals


def fetch_deal_to_company(deal_ids):
    """Retorna {deal_id: company_id (primary)}. API v4 pra pegar Primary."""
    if not deal_ids:
        return {}
    result = {}
    for i in range(0, len(deal_ids), 100):
        chunk = deal_ids[i:i + 100]
        r = req(
            "POST",
            "/crm/v4/associations/deals/companies/batch/read",
            json={"inputs": [{"id": did} for did in chunk]},
        )
        if r.status_code not in (200, 207):
            print(f"ERRO assoc: {r.status_code} {r.text[:200]}")
            continue
        for entry in r.json().get("results", []):
            did = str(entry.get("from", {}).get("id"))
            tos = entry.get("to", []) or []
            primary = None
            for t in tos:
                for at in t.get("associationTypes", []) or []:
                    if at.get("label") == "Primary":
                        primary = str(t.get("toObjectId"))
                        break
                if primary:
                    break
            if not primary and tos:
                primary = str(tos[0].get("toObjectId"))
            if primary:
                result[did] = primary
    return result


def fetch_company_name(company_id):
    """Nome da Company pro log."""
    r = req("GET", f"/crm/v3/objects/companies/{company_id}", params={"properties": "name"})
    if r.status_code == 200:
        return r.json().get("properties", {}).get("name", "")
    return ""


def fetch_last_ganho_for_company(company_id):
    """Retorna props do último deal Ganho da Company (closedate DESC LIMIT 1).

    HubSpot search por association não é direta; fallback: listar todos deals
    da Company via /crm/v4 + batch/read pra pegar props + filtrar localmente.
    """
    r = req("GET", f"/crm/v4/objects/companies/{company_id}/associations/deals")
    if r.status_code != 200:
        return None
    deal_ids = [str(x["toObjectId"]) for x in r.json().get("results", [])]
    if not deal_ids:
        return None

    # batch read com props necessárias
    props_all = CAMPOS_RENOVACAO + PROPS_CONTEXTO
    all_deals = []
    for i in range(0, len(deal_ids), 100):
        chunk = deal_ids[i:i + 100]
        br = req(
            "POST",
            "/crm/v3/objects/deals/batch/read",
            json={"properties": props_all, "inputs": [{"id": d} for d in chunk]},
        )
        if br.status_code not in (200, 207):
            continue
        all_deals.extend(br.json().get("results", []))

    ganhos = [d for d in all_deals if d.get("properties", {}).get("dealstage") in STAGES_GANHO]
    if not ganhos:
        return None

    def closedate_key(d):
        cd = d.get("properties", {}).get("closedate") or ""
        return cd

    ganhos.sort(key=closedate_key, reverse=True)
    return ganhos[0]


# ===================================================
# CORE
# ===================================================

def build_patch_payload(novo_props, fonte_props):
    """Retorna {campo: valor_novo} só pros campos onde:
    - novo está vazio E
    - fonte tem valor não-vazio.
    Regra clássica "não sobrescrever"."""
    payload = {}
    for campo in CAMPOS_RENOVACAO:
        novo_val = (novo_props.get(campo) or "")
        if isinstance(novo_val, str):
            novo_val = novo_val.strip()
        if novo_val:
            continue
        fonte_val = (fonte_props.get(campo) or "")
        if isinstance(fonte_val, str):
            fonte_val = fonte_val.strip()
        if not fonte_val:
            continue
        payload[campo] = fonte_val
    return payload


def aplicar_patch(deal_id, payload):
    r = req(
        "PATCH",
        f"/crm/v3/objects/deals/{deal_id}",
        json={"properties": payload},
    )
    return r.status_code in (200, 201), r.status_code, r.text[:200] if r.status_code not in (200, 201) else ""


def setup_csv_writer():
    """Lazy CSV writer em HubSpot/logs/renovacao_YYYYMMDD.csv."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    today = datetime.datetime.now().strftime("%Y%m%d")
    path = os.path.join(LOGS_DIR, f"renovacao_{today}.csv")
    is_new = not os.path.exists(path)
    f = open(path, "a", newline="", encoding="utf-8")
    w = csv.writer(f)
    if is_new:
        w.writerow([
            "timestamp", "deal_id", "deal_name", "company_id", "company_name",
            "fonte_deal_id", "fonte_closedate", "fonte_pipeline",
            "novo_pipeline", "pipeline_mismatch",
            "fields_patched", "patch_body", "status", "skipped_reason",
        ])
    return f, w, path


# ===================================================
# MAIN
# ===================================================

def run(args):
    if not HUBSPOT_TOKEN:
        print("ERRO: HUBSPOT_TOKEN não configurado")
        return 1

    modo = "EXECUTE" if args.execute else "DRY-RUN"
    print(f"=== infer_valor_diagnostico_renovacao ({modo}) ===")
    print(f"lookback: {args.lookback_days}d | deal_id: {args.deal_id or '(todos elegíveis)'}")

    deals = fetch_deals_candidatos(args.lookback_days, deal_id=args.deal_id)
    print(f"Deals candidatos: {len(deals)}")
    if not deals:
        print("Nada a processar.")
        return 0

    deal_ids = [d["id"] for d in deals]
    deal_to_company = fetch_deal_to_company(deal_ids)
    print(f"Com Company associada: {len(deal_to_company)}")

    csv_file, csv_writer, csv_path = setup_csv_writer()
    print(f"Log CSV: {csv_path}")

    stats = {"patched": 0, "skipped_no_company": 0, "skipped_no_source_deal": 0,
             "skipped_source_empty": 0, "skipped_already_filled": 0, "errors": 0}
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Cache source Ganho por Company (várias deals podem apontar pra mesma Company)
    ganho_cache = {}

    for deal in deals:
        did = deal["id"]
        props = deal.get("properties", {}) or {}
        dname = props.get("dealname", "")
        novo_pipeline = props.get("pipeline", "")

        cid = deal_to_company.get(did)
        if not cid:
            stats["skipped_no_company"] += 1
            csv_writer.writerow([now_iso, did, dname, "", "", "", "", "", novo_pipeline, "", 0, "", "skipped", "no_company"])
            continue

        if cid not in ganho_cache:
            ganho_cache[cid] = {
                "company_name": fetch_company_name(cid),
                "ganho": fetch_last_ganho_for_company(cid),
            }
        company_name = ganho_cache[cid]["company_name"]
        fonte = ganho_cache[cid]["ganho"]

        if not fonte:
            stats["skipped_no_source_deal"] += 1
            csv_writer.writerow([now_iso, did, dname, cid, company_name, "", "", "", novo_pipeline, "", 0, "", "skipped", "no_source_deal"])
            continue

        fonte_props = fonte.get("properties", {}) or {}
        fonte_id = fonte["id"]
        fonte_closedate = fonte_props.get("closedate", "")
        fonte_pipeline = fonte_props.get("pipeline", "")
        pipeline_mismatch = "true" if fonte_pipeline != novo_pipeline else "false"

        payload = build_patch_payload(props, fonte_props)

        if not payload:
            # Não tem nada pra copiar: ou fonte vazia, ou deal novo já preenchido
            fonte_tem_algo = any((fonte_props.get(c) or "").strip() for c in CAMPOS_RENOVACAO)
            if not fonte_tem_algo:
                stats["skipped_source_empty"] += 1
                reason = "source_empty"
            else:
                stats["skipped_already_filled"] += 1
                reason = "already_filled"
            csv_writer.writerow([now_iso, did, dname, cid, company_name, fonte_id, fonte_closedate, fonte_pipeline, novo_pipeline, pipeline_mismatch, 0, "", "skipped", reason])
            continue

        fields_patched = len(payload)
        patch_json = json.dumps(payload, ensure_ascii=False)

        if args.execute:
            ok, status, err = aplicar_patch(did, payload)
            if ok:
                stats["patched"] += 1
                status_label = "patched"
                print(f"  PATCH ok deal={did} campos={fields_patched} fonte={fonte_id} company={company_name[:30]}")
            else:
                stats["errors"] += 1
                status_label = f"error_{status}"
                print(f"  PATCH ERRO deal={did}: {status} {err}")
            csv_writer.writerow([now_iso, did, dname, cid, company_name, fonte_id, fonte_closedate, fonte_pipeline, novo_pipeline, pipeline_mismatch, fields_patched, patch_json, status_label, ""])
        else:
            stats["patched"] += 1  # conta o que SERIA patched
            print(f"  DRY deal={did} campos={fields_patched} fonte={fonte_id} company={company_name[:30]} payload={patch_json[:150]}")
            csv_writer.writerow([now_iso, did, dname, cid, company_name, fonte_id, fonte_closedate, fonte_pipeline, novo_pipeline, pipeline_mismatch, fields_patched, patch_json, "dry_run", ""])

    csv_file.close()
    print(f"\n=== STATS ({modo}) ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print(f"Log: {csv_path}")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Pre-populate campos de valor em deals de renovação.")
    parser.add_argument("--execute", action="store_true", help="Aplica PATCHs reais (default: dry-run)")
    parser.add_argument("--deal-id", help="Processa apenas este deal (teste)")
    parser.add_argument("--lookback-days", type=int, default=30, help="Janela de deals candidatos (default 30)")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
