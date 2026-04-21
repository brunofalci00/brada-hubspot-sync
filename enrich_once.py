"""
E2 - Enrichment one-shot (2026-04-19).

Roda 1x manualmente. Quando Automatize entrar em producao, ela preenche na
origem; este script so tampa o passivo de dados antigos.

Tres frentes:
  1. Companies via BrasilAPI: popula state / municipio / razao_social /
     cnae_descricao quando vazios.
  2. Deals: deriva lei_principal (argmax dos 11 valor_lei_*) e
     linha_de_imposto_categoria (IR/ICMS/ISS) sem lookback - pega deals
     antigos nunca tocados pelo sync.py horario.
  3. Backfill textarea legacy linha_de_imposto -> picklist
     linha_de_imposto_categoria.

Uso:
  python enrich_once.py --verify   # so conta quantos deals/companies seriam
  python enrich_once.py --dry-run  # mostra PATCHs sem executar
  python enrich_once.py            # executa de verdade

Gera:
  HubSpot/enrichment_report_19abr.md
  HubSpot/github-actions/errors.csv
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests

# Windows console UTF-8
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)


# ===================================================
# CONFIG
# ===================================================

BASE = "https://api.hubapi.com"
BRASILAPI = "https://brasilapi.com.br/api/cnpj/v1"

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
if not HUBSPOT_TOKEN:
    print("ERRO: HUBSPOT_TOKEN nao definido no ambiente", file=sys.stderr)
    sys.exit(2)

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

# Constantes copiadas de sync.py (evita import que puxa gspread como dep).
LEI_PROPS = [
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
]

LEIS_MAP = {
    "valor_lei_rouanet": "Rouanet",
    "valor_lei_do_esporte": "Esporte Federal",
    "valor_lei_do_esporte_estadual": "Esporte Estadual",
    "valor_lei_do_bem": "Lei do Bem",
    "valor_lei_da_cultura": "Cultura Estadual",
    "valor_lei_da_cultura_municipal": "Cultura Municipal",
    "valor_lei_da_crianca_e_do_adolescente": "FIA (Crianca e Adolescente)",
    "valor_lei_do_idoso": "Fundo do Idoso",
    "valor_lei_da_reciclagem": "Reciclagem",
    "valor_pronas": "PRONAS",
    "valor_pronon": "PRONON",
}

LEI_TO_CATEGORIA = {
    "Rouanet": "IR", "Esporte Federal": "IR", "Lei do Bem": "IR",
    "FIA (Crianca e Adolescente)": "IR", "Fundo do Idoso": "IR",
    "PRONAS": "IR", "PRONON": "IR",
    "Esporte Estadual": "ICMS", "Cultura Estadual": "ICMS", "Reciclagem": "ICMS",
    "Cultura Municipal": "ISS",
}

# Picklist value == label no E1 (setup_hubspot_fields.py usa _opts(labels) que
# seta value=label). sync.py tem mapa errado (lowercase) e PATCHs falhavam
# silenciosamente no cron. Aqui usamos label direto.
LEI_LABEL_TO_PICKLIST_VALUE = {lbl: lbl for lbl in LEIS_MAP.values()}
CATEGORIA_LABEL_TO_PICKLIST_VALUE = {"IR": "IR", "ICMS": "ICMS", "ISS": "ISS"}

# Mapa backfill: texto legacy -> categoria IR/ICMS/ISS.
# Lookup via lower().strip() + fallback por prefixo.
TEXTO_LEGACY_TO_CATEGORIA = {
    "ir esporte": "IR",
    "ir cultura": "IR",
    "ir cultura sp": "IR",
    "ir cultura sp - egp": "IR",
    "fumcad": "IR",
    "iss sp": "ISS",
    "iss cultura": "ISS",
    "iss cultura sp": "ISS",
    "icms/ rs / cultural": "ICMS",
    "icms rs": "ICMS",
    "icms/rs/cultural": "ICMS",
    "lei estadual da cultura": "ICMS",
    "lei estadual cultura": "ICMS",
}


# ===================================================
# HTTP
# ===================================================

def req(method, path, **kwargs):
    """HubSpot HTTP com retry exponencial em 429 e 5xx."""
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
# ERRORS CSV
# ===================================================

ERRORS_PATH = os.path.join(os.path.dirname(__file__) or ".", "errors.csv")
_errors_file = None
_errors_writer = None


def log_error(frente, identificador, motivo, detalhe=""):
    global _errors_file, _errors_writer
    if _errors_file is None:
        _errors_file = open(ERRORS_PATH, "w", newline="", encoding="utf-8")
        _errors_writer = csv.writer(_errors_file)
        _errors_writer.writerow(["frente", "id", "motivo", "detalhe"])
    _errors_writer.writerow([frente, identificador, motivo, detalhe[:300]])
    _errors_file.flush()


def close_errors():
    global _errors_file
    if _errors_file:
        _errors_file.close()
        _errors_file = None


# ===================================================
# FRENTE 0: setup custom property cnae_descricao
# ===================================================

def ensure_property(name, label, description, dry_run=False):
    """Cria company property (string/text) se nao existir. Idempotente."""
    r = req("GET", f"/crm/v3/properties/companies/{name}")
    if r.status_code == 200:
        print(f"[frente 0] {name} ja existe")
        return
    if r.status_code != 404:
        print(f"[frente 0] erro inesperado ao checar {name}: {r.status_code} {r.text[:200]}")
        return
    body = {
        "name": name, "label": label,
        "type": "string", "fieldType": "text",
        "groupName": "companyinformation",
        "description": description,
    }
    if dry_run:
        print(f"[frente 0] DRY-RUN criaria property: {name}")
        return
    r = req("POST", "/crm/v3/properties/companies", json=body)
    if r.status_code in (200, 201):
        print(f"[frente 0] {name} criada")
    elif r.status_code == 409:
        print(f"[frente 0] {name} ja existe (409)")
    else:
        print(f"[frente 0] FALHOU criar {name}: {r.status_code} {r.text[:300]}")
        sys.exit(3)


def setup_cnae_property(dry_run=False):
    """Cria properties custom necessarias pro enrichment E2.

    HubSpot tem 'city' e 'state' nativos, mas nao 'municipio' nem 'razao_social'.
    Criamos as 3 customs (cnae_descricao, razao_social) e mapeamos municipio->city
    na frente 1.
    """
    ensure_property(
        "cnae_descricao", "CNAE (descricao)",
        "Atividade principal (CNAE fiscal) vinda da BrasilAPI. Populado em E2 (19/04).",
        dry_run,
    )
    ensure_property(
        "razao_social", "Razao Social",
        "Razao social oficial (Receita Federal) vinda da BrasilAPI. Populado em E2 (19/04).",
        dry_run,
    )


# ===================================================
# FRENTE 1: Companies via BrasilAPI
# ===================================================

COMPANY_PROPS_FETCH = [
    "name", "cnpj", "state", "industry", "city", "razao_social", "cnae_descricao",
    "phone", "address", "zip",  # E6 Onda A — payload BrasilAPI expandido
]


def fetch_companies_com_cnpj(lookback_hours=None):
    """Busca paginada de companies com cnpj preenchido.

    Se lookback_hours é setado (modo incremental), filtra por hs_lastmodifieddate
    nas últimas N horas pra reduzir volume no cron contínuo.
    """
    results = []
    after = None
    filters = [{"propertyName": "cnpj", "operator": "HAS_PROPERTY"}]
    if lookback_hours:
        cutoff_ms = int((datetime.utcnow() - timedelta(hours=lookback_hours)).timestamp() * 1000)
        filters.append({
            "propertyName": "hs_lastmodifieddate",
            "operator": "GT",
            "value": cutoff_ms,
        })
    while True:
        body = {
            "filterGroups": [{"filters": filters}],
            "properties": COMPANY_PROPS_FETCH,
            "limit": 100,
        }
        if after:
            body["after"] = after
        r = req("POST", "/crm/v3/objects/companies/search", json=body)
        if r.status_code != 200:
            print(f"[frente 1] ERRO search companies: {r.status_code} {r.text[:300]}")
            break
        data = r.json()
        results.extend(data.get("results", []))
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
    return results


def precisa_enrich(props):
    """Company precisa de enrichment se algum dos 7 campos esta vazio (E6 Onda A)."""
    for k in ("state", "city", "razao_social", "cnae_descricao", "phone", "address", "zip"):
        if not (props.get(k) or "").strip():
            return True
    return False


def brasilapi_lookup(cnpj_limpo):
    """GET BrasilAPI com retry em 429 (backoff 2/4/8/16s).

    BrasilAPI eh publica e tem rate limit dinamico. Precisa retry alem do
    sleep fixo entre calls.
    """
    for attempt in range(5):
        try:
            r = requests.get(f"{BRASILAPI}/{cnpj_limpo}", timeout=20)
        except requests.RequestException as e:
            if attempt < 4:
                time.sleep(2 ** (attempt + 1))
                continue
            return None, f"timeout/net: {e}"
        if r.status_code == 429:
            if attempt < 4:
                time.sleep(2 ** (attempt + 1))  # 2,4,8,16s
                continue
            return None, "429 rate limit (esgotou retries)"
        if r.status_code == 404:
            return None, "404 cnpj nao encontrado"
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        try:
            return r.json(), None
        except ValueError:
            return None, "json invalido"
    return None, "esgotou tentativas"


def frente_1_companies(dry_run=False, limit=None, lookback_hours=None):
    companies = fetch_companies_com_cnpj(lookback_hours=lookback_hours)
    label = f"[frente 1{' incremental' if lookback_hours else ''}]"
    print(f"{label} companies com cnpj: {len(companies)}"
          + (f" (lookback {lookback_hours}h)" if lookback_hours else ""))
    candidatas = [c for c in companies if precisa_enrich(c.get("properties", {}))]
    print(f"[frente 1] candidatas a enrichment: {len(candidatas)}")

    if limit:
        candidatas = candidatas[:limit]

    enriched = 0
    skipped_cnpj = 0
    failed = 0
    for idx, comp in enumerate(candidatas, 1):
        cid = comp["id"]
        props = comp.get("properties", {}) or {}
        cnpj_raw = props.get("cnpj") or ""
        cnpj_limpo = re.sub(r"\D", "", cnpj_raw)
        if len(cnpj_limpo) != 14:
            log_error("companies", cid, "cnpj_invalido", cnpj_raw)
            skipped_cnpj += 1
            continue

        info, err = brasilapi_lookup(cnpj_limpo)
        time.sleep(2.0)  # rate limit self-imposed (BrasilAPI 429 frequente a 1/s)
        if err:
            log_error("companies", cid, "brasilapi_falhou", f"cnpj={cnpj_limpo} {err}")
            failed += 1
            continue

        patch = {}
        if not (props.get("state") or "").strip() and info.get("uf"):
            patch["state"] = info["uf"]
        if not (props.get("city") or "").strip() and info.get("municipio"):
            patch["city"] = info["municipio"]
        if not (props.get("razao_social") or "").strip() and info.get("razao_social"):
            patch["razao_social"] = info["razao_social"]
        if not (props.get("cnae_descricao") or "").strip() and info.get("cnae_fiscal_descricao"):
            patch["cnae_descricao"] = info["cnae_fiscal_descricao"]

        # E6 Onda A — payload BrasilAPI expandido (regra "nao sobrescrever" mantida)
        if not (props.get("phone") or "").strip() and info.get("ddd_telefone_1"):
            patch["phone"] = info["ddd_telefone_1"]
        logradouro = (info.get("logradouro") or "").strip()
        numero = (info.get("numero") or "").strip()
        if not (props.get("address") or "").strip() and logradouro:
            if numero and numero.upper() != "S/N":
                patch["address"] = f"{logradouro}, {numero}"
            else:
                patch["address"] = logradouro
        if not (props.get("zip") or "").strip() and info.get("cep"):
            patch["zip"] = info["cep"]  # BrasilAPI devolve sem dash; HubSpot aceita
        # name fallback: SÓ se name atual vazio E fantasia preenchido
        if not (props.get("name") or "").strip() and (info.get("nome_fantasia") or "").strip():
            patch["name"] = info["nome_fantasia"]

        if not patch:
            continue

        if dry_run:
            print(f"[frente 1] DRY company {cid}: {patch}")
            enriched += 1
            continue

        r = req("PATCH", f"/crm/v3/objects/companies/{cid}", json={"properties": patch})
        if r.status_code in (200, 201):
            enriched += 1
            if idx % 20 == 0:
                print(f"[frente 1] progresso: {idx}/{len(candidatas)} ({enriched} enriched)")
        else:
            log_error("companies", cid, "patch_falhou", f"{r.status_code} {r.text[:150]}")
            failed += 1

    return {
        "scanned": len(companies),
        "candidatas": len(candidatas),
        "enriched": enriched,
        "skipped_cnpj_invalido": skipped_cnpj,
        "failed": failed,
    }


# ===================================================
# FRENTE 2: Deals - derivacao lei_principal + categoria
# ===================================================

DEAL_PROPS_FRENTE2 = LEI_PROPS + ["lei_principal", "linha_de_imposto_categoria"]


def fetch_deals_com_valor_lei():
    """Busca deals com qualquer valor_lei_* > 0 (OR filter).

    HubSpot search limita filterGroups a 5, mas temos 11 leis. Rodamos
    searches em chunks de 5 e deduplicamos por deal id.
    """
    chunk_size = 5
    seen = {}
    for i in range(0, len(LEI_PROPS), chunk_size):
        chunk = LEI_PROPS[i:i + chunk_size]
        filter_groups = [
            {"filters": [{"propertyName": p, "operator": "GT", "value": "0"}]}
            for p in chunk
        ]
        after = None
        while True:
            body = {
                "filterGroups": filter_groups,
                "properties": DEAL_PROPS_FRENTE2,
                "limit": 100,
            }
            if after:
                body["after"] = after
            r = req("POST", "/crm/v3/objects/deals/search", json=body)
            if r.status_code != 200:
                print(f"[frente 2] ERRO search deals (chunk {i}): {r.status_code} {r.text[:300]}")
                break
            data = r.json()
            for d in data.get("results", []):
                seen[d["id"]] = d  # dedup
            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break
    return list(seen.values())


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def frente_2_deals(dry_run=False):
    deals = fetch_deals_com_valor_lei()
    print(f"[frente 2] deals com valor_lei_*>0: {len(deals)}")
    lei_updated = 0
    categoria_updated = 0
    both_patched = 0
    no_change = 0
    failed = 0

    for deal in deals:
        did = deal["id"]
        props = deal.get("properties", {}) or {}

        # argmax
        melhor_prop = None
        melhor_valor = 0.0
        for p in LEI_PROPS:
            v = _to_float(props.get(p))
            if v > melhor_valor:
                melhor_valor = v
                melhor_prop = p
        if melhor_prop is None:
            continue  # edge: search retornou mas tudo vazio

        lei_label = LEIS_MAP[melhor_prop]
        categoria_label = LEI_TO_CATEGORIA[lei_label]
        lei_value = LEI_LABEL_TO_PICKLIST_VALUE[lei_label]
        categoria_value = CATEGORIA_LABEL_TO_PICKLIST_VALUE[categoria_label]

        lei_atual = (props.get("lei_principal") or "").strip()
        categoria_atual = (props.get("linha_de_imposto_categoria") or "").strip()

        patch = {}
        if lei_value != lei_atual:
            patch["lei_principal"] = lei_value
        if categoria_value != categoria_atual:
            patch["linha_de_imposto_categoria"] = categoria_value

        if not patch:
            no_change += 1
            continue

        if dry_run:
            print(f"[frente 2] DRY deal {did}: {patch}")
            if "lei_principal" in patch: lei_updated += 1
            if "linha_de_imposto_categoria" in patch: categoria_updated += 1
            if len(patch) == 2: both_patched += 1
            continue

        r = req("PATCH", f"/crm/v3/objects/deals/{did}", json={"properties": patch})
        if r.status_code in (200, 201):
            if "lei_principal" in patch: lei_updated += 1
            if "linha_de_imposto_categoria" in patch: categoria_updated += 1
            if len(patch) == 2: both_patched += 1
        else:
            log_error("deals_lei", did, "patch_falhou", f"{r.status_code} {r.text[:150]}")
            failed += 1

    return {
        "scanned": len(deals),
        "lei_updated": lei_updated,
        "categoria_updated": categoria_updated,
        "both_patched": both_patched,
        "no_change": no_change,
        "failed": failed,
    }


# ===================================================
# FRENTE 3: Backfill textarea legacy linha_de_imposto
# ===================================================

def mapear_texto_legacy(texto):
    """Retorna 'IR' / 'ICMS' / 'ISS' ou None."""
    if not texto:
        return None
    chave = texto.strip().lower()
    if chave in TEXTO_LEGACY_TO_CATEGORIA:
        return TEXTO_LEGACY_TO_CATEGORIA[chave]
    # Fallback por prefixo
    if chave.startswith("ir "): return "IR"
    if chave.startswith("iss "): return "ISS"
    if chave.startswith("icms"): return "ICMS"
    return None


def fetch_deals_legacy():
    """Deals com categoria vazia - filtro em linha_de_imposto eh client-side.

    linha_de_imposto foi arquivado em E1; HubSpot search retorna 400 ao
    filtrar nele. Estrategia: search soh por categoria vazia, pedir
    linha_de_imposto em properties (archived=true), filtrar in-memory.
    """
    results = []
    after = None
    while True:
        body = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "linha_de_imposto_categoria", "operator": "NOT_HAS_PROPERTY"},
                ]
            }],
            "properties": ["linha_de_imposto", "linha_de_imposto_categoria"],
            "limit": 100,
        }
        if after:
            body["after"] = after
        r = req("POST", "/crm/v3/objects/deals/search", json=body)
        if r.status_code != 200:
            print(f"[frente 3] search retornou {r.status_code}: {r.text[:300]}")
            break
        data = r.json()
        results.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    # Filtro client-side: so deals onde linha_de_imposto esta preenchida.
    filtered = [
        d for d in results
        if (d.get("properties", {}).get("linha_de_imposto") or "").strip()
    ]
    print(f"[frente 3] search retornou {len(results)} deals c/ categoria vazia; "
          f"{len(filtered)} tem linha_de_imposto legacy preenchido")
    return filtered


def frente_3_backfill(dry_run=False):
    deals = fetch_deals_legacy()
    print(f"[frente 3] deals com linha_de_imposto legacy preenchido: {len(deals)}")
    backfilled = 0
    unmapped = 0
    failed = 0
    valores_unicos = {}

    for deal in deals:
        did = deal["id"]
        props = deal.get("properties", {}) or {}
        texto = props.get("linha_de_imposto") or ""
        valores_unicos[texto] = valores_unicos.get(texto, 0) + 1
        categoria = mapear_texto_legacy(texto)
        if not categoria:
            log_error("legacy_textarea", did, "texto_nao_mapeado", texto)
            unmapped += 1
            continue

        patch = {"linha_de_imposto_categoria": CATEGORIA_LABEL_TO_PICKLIST_VALUE[categoria]}

        if dry_run:
            print(f"[frente 3] DRY deal {did}: {texto!r} -> {categoria}")
            backfilled += 1
            continue

        r = req("PATCH", f"/crm/v3/objects/deals/{did}", json={"properties": patch})
        if r.status_code in (200, 201):
            backfilled += 1
        else:
            log_error("legacy_textarea", did, "patch_falhou", f"{r.status_code} {r.text[:150]}")
            failed += 1

    return {
        "scanned": len(deals),
        "backfilled": backfilled,
        "unmapped": unmapped,
        "failed": failed,
        "valores_unicos": valores_unicos,
    }


# ===================================================
# FRENTE 4 (bis 21/04): backfill cnpj_do_incentivador
# ===================================================
#
# Property criada em E1-bis (Deal, string/text). Backfill pega Company.cnpj
# da matriz associada como default — executivo sobrescreve manualmente se
# for filial. Usa Batch API (100 por call) pra evitar 570 * 3 calls
# individuais.

def fetch_deals_sem_cnpj_incentivador():
    """Lista IDs de deals com cnpj_do_incentivador vazio."""
    ids = []
    after = None
    while True:
        body = {
            "filterGroups": [{"filters": [
                {"propertyName": "cnpj_do_incentivador", "operator": "NOT_HAS_PROPERTY"},
            ]}],
            "properties": ["dealname"],
            "limit": 100,
        }
        if after:
            body["after"] = after
        r = req("POST", "/crm/v3/objects/deals/search", json=body)
        if r.status_code != 200:
            print(f"[frente 4] ERRO search: {r.status_code} {r.text[:300]}")
            break
        data = r.json()
        ids.extend(d["id"] for d in data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return ids


def batch_read_deal_properties(deal_ids, properties):
    """Batch read deals. Retorna {deal_id: {prop: value}}."""
    result = {}
    for i in range(0, len(deal_ids), 100):
        chunk = deal_ids[i:i + 100]
        body = {
            "inputs": [{"id": did} for did in chunk],
            "properties": properties,
        }
        r = req("POST", "/crm/v3/objects/deals/batch/read", json=body)
        if r.status_code not in (200, 207):
            print(f"[frente 4] batch_read deals falhou (chunk {i}): {r.status_code} {r.text[:200]}")
            continue
        data = r.json()
        for d in data.get("results", []):
            result[d["id"]] = d.get("properties", {}) or {}
        time.sleep(1)
    return result


def batch_read_deal_to_company(deal_ids):
    """Batch read associations deal -> companies (API v4).

    Retorna {deal_id: company_id_primary} ou deal_id ausente se não houver
    associação. Escolhe a Primary quando há múltiplas; senão, a primeira.

    `batch/read` do CRM v3 ignora o parâmetro `associations` silenciosamente
    — por isso usamos a API v4 de associations que é a que realmente aceita
    batch.
    """
    result = {}
    for i in range(0, len(deal_ids), 100):
        chunk = deal_ids[i:i + 100]
        body = {"inputs": [{"id": did} for did in chunk]}
        r = req("POST", "/crm/v4/associations/deals/companies/batch/read", json=body)
        if r.status_code not in (200, 207):
            print(f"[frente 4] batch v4 associations falhou (chunk {i}): {r.status_code} {r.text[:200]}")
            continue
        data = r.json()
        for entry in data.get("results", []):
            did = str(entry.get("from", {}).get("id"))
            tos = entry.get("to", []) or []
            if not tos:
                continue
            # Primary = associationTypes contém {label: "Primary"}
            primary_id = None
            for t in tos:
                for at in t.get("associationTypes", []) or []:
                    if at.get("label") == "Primary":
                        primary_id = str(t.get("toObjectId"))
                        break
                if primary_id:
                    break
            if not primary_id:
                primary_id = str(tos[0].get("toObjectId"))
            result[did] = primary_id
        time.sleep(1)
    return result


def batch_read_companies_cnpj(company_ids):
    """Batch read companies. Retorna {company_id: cnpj}."""
    result = {}
    ids = list(company_ids)
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        body = {
            "inputs": [{"id": cid} for cid in chunk],
            "properties": ["cnpj"],
        }
        r = req("POST", "/crm/v3/objects/companies/batch/read", json=body)
        if r.status_code not in (200, 207):
            print(f"[frente 4] batch_read companies falhou (chunk {i}): {r.status_code} {r.text[:200]}")
            continue
        data = r.json()
        for c in data.get("results", []):
            cid = c["id"]
            cnpj = (c.get("properties", {}).get("cnpj") or "").strip()
            result[cid] = cnpj
        time.sleep(1)
    return result


def frente_4_backfill_cnpj_incentivador(dry_run=False):
    deal_ids = fetch_deals_sem_cnpj_incentivador()
    print(f"[frente 4] deals sem cnpj_do_incentivador: {len(deal_ids)}")

    if not deal_ids:
        return {
            "scanned": 0, "patched": 0,
            "skipped_sem_company": 0, "skipped_company_sem_cnpj": 0,
            "skipped_ja_preenchido": 0, "failed": 0,
        }

    deal_props = batch_read_deal_properties(deal_ids, ["cnpj_do_incentivador"])
    deal_to_company = batch_read_deal_to_company(deal_ids)
    print(f"[frente 4] deals lidos: {len(deal_props)}   deals com Company associada: {len(deal_to_company)}")

    company_ids = set(deal_to_company.values())
    print(f"[frente 4] companies unicas associadas: {len(company_ids)}")
    company_cnpjs = batch_read_companies_cnpj(company_ids)

    patches = []
    skipped_sem_company = 0
    skipped_company_sem_cnpj = 0
    skipped_ja_preenchido = 0
    for did in deal_ids:
        props = deal_props.get(did, {})
        if (props.get("cnpj_do_incentivador") or "").strip():
            skipped_ja_preenchido += 1
            continue
        cid = deal_to_company.get(did)
        if not cid:
            skipped_sem_company += 1
            log_error("frente_4", did, "sem_company_associada", "")
            continue
        cnpj = (company_cnpjs.get(cid) or "").strip()
        if not cnpj:
            skipped_company_sem_cnpj += 1
            log_error("frente_4", did, "company_sem_cnpj", f"company_id={cid}")
            continue
        patches.append({"id": did, "properties": {"cnpj_do_incentivador": cnpj}})

    print(f"[frente 4] patches a aplicar: {len(patches)} "
          f"(skip_sem_company={skipped_sem_company}, "
          f"skip_company_sem_cnpj={skipped_company_sem_cnpj}, "
          f"skip_ja_preenchido={skipped_ja_preenchido})")

    patched = 0
    failed = 0
    for i in range(0, len(patches), 100):
        chunk = patches[i:i + 100]
        if dry_run:
            for p in chunk[:3]:
                print(f"[frente 4] DRY deal {p['id']} <- cnpj={p['properties']['cnpj_do_incentivador']}")
            if len(chunk) > 3:
                print(f"[frente 4] DRY ...+{len(chunk) - 3} outros no chunk")
            patched += len(chunk)
            continue
        r = req("POST", "/crm/v3/objects/deals/batch/update", json={"inputs": chunk})
        if r.status_code in (200, 207):
            data = r.json()
            errs = data.get("errors", []) or []
            for err in errs:
                ctx_id = (err.get("context", {}) or {}).get("id", ["?"])
                eid = ctx_id[0] if isinstance(ctx_id, list) and ctx_id else "?"
                log_error("frente_4", eid, "batch_patch_falhou", err.get("message", "")[:200])
            patched += len(chunk) - len(errs)
            failed += len(errs)
        else:
            log_error("frente_4", f"batch_{i}", "batch_falhou", f"{r.status_code} {r.text[:200]}")
            failed += len(chunk)
        time.sleep(1)

    return {
        "scanned": len(deal_ids),
        "patched": patched,
        "skipped_sem_company": skipped_sem_company,
        "skipped_company_sem_cnpj": skipped_company_sem_cnpj,
        "skipped_ja_preenchido": skipped_ja_preenchido,
        "failed": failed,
    }


def append_frente4_report(res4, dry_run):
    """Append seção ao relatório existente (nao sobrescreve)."""
    path = os.path.join(os.path.dirname(__file__) or ".", "..", "enrichment_report_19abr.md")
    path = os.path.normpath(path)
    hoje = datetime.now().strftime("%Y-%m-%d %H:%M")
    modo = "DRY-RUN" if dry_run else "EXECUCAO REAL"
    sec = f"""
---

## Frente 4 — bis 21/04 (backfill `cnpj_do_incentivador`)

**Data:** {hoje}  |  **Modo:** {modo}

| Metrica | Valor |
|---|---|
| Deals sem `cnpj_do_incentivador` (search inicial) | {res4['scanned']} |
| Patched (cnpj_do_incentivador <- Company.cnpj da matriz) | {res4['patched']} |
| Skipped — sem Company associada | {res4['skipped_sem_company']} |
| Skipped — Company sem cnpj | {res4['skipped_company_sem_cnpj']} |
| Skipped — ja preenchido (race cond) | {res4['skipped_ja_preenchido']} |
| Falhas PATCH | {res4['failed']} |

Racional: default = matriz (Company.cnpj). Executivo sobrescreve manualmente quando souber que é filial/PDV. Detalhes em `plano_E2_enrichment_oneshot.md` seção "Ajuste bis — 20/04".
"""
    with open(path, "a", encoding="utf-8") as f:
        f.write(sec)
    print(f"\n[frente 4] seção appendada a: {path}")


# ===================================================
# RELATORIO
# ===================================================

def gerar_relatorio(res1, res2, res3, dry_run):
    hoje = datetime.now().strftime("%Y-%m-%d %H:%M")
    modo = "DRY-RUN" if dry_run else "EXECUCAO REAL"
    path = os.path.join(os.path.dirname(__file__) or ".", "..", "enrichment_report_19abr.md")
    path = os.path.normpath(path)

    linhas_legacy = "\n".join(
        f"  - `{t[:60]}`: {n}" for t, n in sorted(
            res3["valores_unicos"].items(), key=lambda x: -x[1]
        )
    ) or "  (nenhum)"

    conteudo = f"""# Relatorio E2 - Enrichment one-shot

**Data:** {hoje}  |  **Modo:** {modo}

## Frente 1 - Companies via BrasilAPI

| Metrica | Valor |
|---|---|
| Companies com CNPJ preenchido | {res1['scanned']} |
| Candidatas (ao menos 1 campo vazio) | {res1['candidatas']} |
| Enriquecidas (patch aplicado) | {res1['enriched']} |
| CNPJ invalido (skip) | {res1['skipped_cnpj_invalido']} |
| Falhas (BrasilAPI/PATCH) | {res1['failed']} |

Campos populados: `state` (nativo), `city` (nativo, recebe `municipio` da BrasilAPI), `razao_social` (custom E2), `cnae_descricao` (custom E2).

## Frente 2 - Deals: lei_principal + linha_de_imposto_categoria

| Metrica | Valor |
|---|---|
| Deals com `valor_lei_*` > 0 | {res2['scanned']} |
| `lei_principal` atualizada | {res2['lei_updated']} |
| `linha_de_imposto_categoria` atualizada | {res2['categoria_updated']} |
| Ambos patcheados (mesmo deal) | {res2['both_patched']} |
| Sem mudanca (ja estava correto) | {res2['no_change']} |
| Falhas | {res2['failed']} |

Derivacao deterministica: argmax dos 11 `valor_lei_*` -> lei -> categoria (IR/ICMS/ISS).

## Frente 3 - Backfill textarea legacy

| Metrica | Valor |
|---|---|
| Deals com `linha_de_imposto` legacy + categoria vazia | {res3['scanned']} |
| Backfilled para categoria picklist | {res3['backfilled']} |
| Nao mapeados (ver errors.csv) | {res3['unmapped']} |
| Falhas PATCH | {res3['failed']} |

Valores encontrados:
{linhas_legacy}

## Proximos passos

- Spot-check no HubSpot UI (3 companies + 3 deals de cada frente).
- Atualizar `reference_hubspot.md` com fill rates pos-enrichment.
- Marcar E2 completo em `contexto_brada.md` (bump v10).
- Erros em `errors.csv`: reprocessar manualmente ou aceitar como aceitavel.

---
Gerado por `enrich_once.py`.
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(conteudo)
    print(f"\nRelatorio escrito em: {path}")


# ===================================================
# MAIN
# ===================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="mostra PATCHs sem executar")
    parser.add_argument("--verify", action="store_true", help="so conta candidatas, sem chamar BrasilAPI nem PATCHs")
    parser.add_argument("--limit-companies", type=int, default=None, help="limita frente 1 a N companies (teste)")
    parser.add_argument("--skip-frente", choices=["1", "2", "3", "4"], action="append", default=[], help="pula uma frente")
    parser.add_argument("--mode", choices=["one-shot", "incremental"], default="one-shot",
                        help="one-shot roda todas frentes; incremental só frente 1 filtrando por hs_lastmodifieddate")
    parser.add_argument("--lookback-hours", type=int, default=2,
                        help="janela de hs_lastmodifieddate no modo incremental (horas)")
    args = parser.parse_args()

    dry = args.dry_run or args.verify
    print(f"=== enrich (mode={args.mode} dry={dry} verify={args.verify}) ===")

    # Modo incremental (E6 Onda A) — só frente 1 com filtro lookback.
    # Skip setup_cnae_property (já aplicado em E2; a property existe).
    if args.mode == "incremental":
        res1 = frente_1_companies(
            dry_run=dry,
            limit=args.limit_companies,
            lookback_hours=args.lookback_hours,
        )
        print(
            f"[incremental] scanned={res1['scanned']} cand={res1['candidatas']} "
            f"enriched={res1['enriched']} skipped_cnpj={res1['skipped_cnpj_invalido']} "
            f"failed={res1['failed']}"
        )
        close_errors()
        return

    setup_cnae_property(dry_run=dry)

    if "1" not in args.skip_frente:
        if args.verify:
            companies = fetch_companies_com_cnpj()
            cand = [c for c in companies if precisa_enrich(c.get("properties", {}))]
            res1 = {"scanned": len(companies), "candidatas": len(cand),
                    "enriched": 0, "skipped_cnpj_invalido": 0, "failed": 0}
        else:
            res1 = frente_1_companies(dry_run=dry, limit=args.limit_companies)
    else:
        res1 = {"scanned": 0, "candidatas": 0, "enriched": 0, "skipped_cnpj_invalido": 0, "failed": 0}

    # Ordem proposital: frente 3 (texto legacy) antes da frente 2 (valor_lei_*>0).
    # Dado estruturado (valor monetario) eh mais confiavel que texto livre
    # historico; frente 2 sobrescreve frente 3 quando ambas incidem no mesmo deal.
    if "3" not in args.skip_frente:
        res3 = frente_3_backfill(dry_run=dry)
    else:
        res3 = {"scanned": 0, "backfilled": 0, "unmapped": 0, "failed": 0, "valores_unicos": {}}

    if "2" not in args.skip_frente:
        res2 = frente_2_deals(dry_run=dry)
    else:
        res2 = {"scanned": 0, "lei_updated": 0, "categoria_updated": 0,
                "both_patched": 0, "no_change": 0, "failed": 0}

    # Frente 4 (bis 21/04): backfill cnpj_do_incentivador.
    # Nao chama gerar_relatorio (reescreveria o arquivo e perderia historico);
    # em vez disso, faz append de seção propria.
    frentes_12_3_skipped = {"1", "2", "3"}.issubset(set(args.skip_frente))
    if "4" not in args.skip_frente:
        res4 = frente_4_backfill_cnpj_incentivador(dry_run=dry)
    else:
        res4 = None

    close_errors()

    # Só regera o relatório completo quando as frentes 1-3 rodaram (não é
    # execução bis isolada). No caso bis (só frente 4), preserva relatório
    # original e só appenda a seção nova.
    if not frentes_12_3_skipped:
        gerar_relatorio(res1, res2, res3, dry_run=dry)
    if res4 is not None:
        append_frente4_report(res4, dry_run=dry)

    print("\n=== DONE ===")
    if not frentes_12_3_skipped:
        print(f"Companies enriquecidas: {res1['enriched']}/{res1['candidatas']}")
        print(f"Deals lei_principal: {res2['lei_updated']}   categoria: {res2['categoria_updated']}")
        print(f"Legacy backfilled: {res3['backfilled']}   unmapped: {res3['unmapped']}")
    if res4 is not None:
        print(f"Frente 4 patched: {res4['patched']}/{res4['scanned']} "
              f"(skip_sem_company={res4['skipped_sem_company']}, "
              f"skip_company_sem_cnpj={res4['skipped_company_sem_cnpj']}, "
              f"failed={res4['failed']})")
    print(f"Errors.csv: {ERRORS_PATH}")


if __name__ == "__main__":
    main()
