"""
Backfill one-shot do campo `origem_lead` no HubSpot, usando `Company.origem`
como fonte quando o Deal não tem origem_lead preenchido.

Regra (decisao Bruno 23/04 tarde, pós-unificacao dos picklists):
  Se Deal.origem_lead esta vazio (NOT_HAS_PROPERTY) ou eh "(em preenchimento)"
  E o Deal tem Company associada
  E Company.origem esta em PASSTHROUGH_VALUES
  -> PATCH Deal.origem_lead = Company.origem (passthrough exato)

Picklist unificado (9 valores — iguais nos dois campos):
  LinkedIn · WhatsApp · Site · Feira/Evento · Indicação Interna ·
  Indicação Externa · Automatize direto · DigiSAC (Proponente) · Outros

Valor legado ambiguo "Linkedin / Whatsapp / Site" (Company.origem) ->
SKIP. Decisao: Ivan/Bruno classificam caso a caso depois.

Uso:
    python backfill_origem_lead.py --dry-run     # default, mostra plano
    python backfill_origem_lead.py --execute     # PATCH real via batch/update

Reusa padroes existentes:
- sync.py::fetch_deal_company_associations (v4 batch associations)
- sync.py::fetch_companies (v3 batch read)
- backfill_produto.py (dry-run + batch update 100/100)
"""
import argparse
import os
import sys
import time
from collections import Counter

import requests

BASE = "https://api.hubapi.com"
TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Valores canonicos do picklist origem_lead (unificado 23/04 tarde — 9 valores)
PICKLIST = {
    "LinkedIn", "WhatsApp", "Site", "Feira/Evento",
    "Indicação Interna", "Indicação Externa",
    "Automatize direto", "DigiSAC (Proponente)", "Outros",
}

# Map de Company.origem -> Deal.origem_lead.
# Valores canonicos batem 1:1 (passthrough exato).
# Valor ambiguo "Linkedin / Whatsapp / Site" -> SKIP (preenchimento manual,
# decisao Bruno 23/04).
PASSTHROUGH_VALUES = {
    "LinkedIn", "WhatsApp", "Site", "Feira/Evento",
    "Indicação Interna", "Indicação Externa",
    "Automatize direto", "DigiSAC (Proponente)", "Outros",
}
AMBIGUOUS_VALUES = {"Linkedin / Whatsapp / Site"}

SENTINEL_VAZIO = {"", "(em preenchimento)"}


def fetch_deals_sem_origem_lead():
    """Search API: deals com origem_lead vazio OU igual ao sentinel.

    Filter group OR:
      - origem_lead NOT_HAS_PROPERTY
      - origem_lead = "(em preenchimento)"
    """
    deals = []
    after = None
    while True:
        body = {
            "limit": 100,
            "properties": ["dealname", "origem_lead", "pipeline"],
            "filterGroups": [
                {"filters": [{"propertyName": "origem_lead", "operator": "NOT_HAS_PROPERTY"}]},
                {"filters": [{"propertyName": "origem_lead", "operator": "EQ", "value": "(em preenchimento)"}]},
            ],
        }
        if after:
            body["after"] = after
        r = requests.post(
            f"{BASE}/crm/v3/objects/deals/search",
            headers=H, json=body, timeout=30,
        )
        if r.status_code != 200:
            print(f"ERRO search: {r.status_code} {r.text[:300]}", file=sys.stderr)
            sys.exit(1)
        d = r.json()
        deals.extend(d.get("results", []))
        nxt = d.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt.get("after")
        time.sleep(0.2)
    return deals


def fetch_deal_company_map(deal_ids):
    """Pra cada deal_id, retorna a primeira company associada (ou None).

    Usa v4 batch associations (100 IDs por call).
    """
    deal_to_company = {}
    ids = [str(did) for did in deal_ids]
    for i in range(0, len(ids), 100):
        batch = ids[i:i + 100]
        r = requests.post(
            f"{BASE}/crm/v4/associations/deals/companies/batch/read",
            headers=H, json={"inputs": [{"id": did} for did in batch]}, timeout=60,
        )
        if r.status_code not in (200, 207):
            print(f"ERRO assoc: {r.status_code} {r.text[:200]}", file=sys.stderr)
            continue
        for result in r.json().get("results", []):
            did = result.get("from", {}).get("id")
            tos = result.get("to", [])
            if did and tos:
                deal_to_company[did] = str(tos[0].get("toObjectId"))
        time.sleep(0.2)
    return deal_to_company


def fetch_companies_origem(company_ids):
    """Pra cada company_id, retorna o valor cru de `origem` (ou '')."""
    out = {}
    unique = list({cid for cid in company_ids if cid})
    for i in range(0, len(unique), 100):
        batch = unique[i:i + 100]
        r = requests.post(
            f"{BASE}/crm/v3/objects/companies/batch/read",
            headers=H,
            json={"properties": ["origem"], "inputs": [{"id": cid} for cid in batch]},
            timeout=60,
        )
        if r.status_code != 200:
            print(f"ERRO companies: {r.status_code} {r.text[:200]}", file=sys.stderr)
            continue
        for comp in r.json().get("results", []):
            cid = comp.get("id")
            origem = (comp.get("properties", {}) or {}).get("origem", "") or ""
            out[cid] = origem
        time.sleep(0.2)
    return out


def build_patch_plan(deals, deal_to_company, company_origem_map):
    """Monta inputs batch pra PATCH + stats de cobertura.

    Agora que os picklists sao unificados (23/04 tarde), o mapeamento eh
    passthrough direto. Valor legado ambiguo "Linkedin / Whatsapp / Site"
    eh skippado (executivo preenche manual depois).
    """
    inputs = []
    stats = Counter()
    unknown_values = Counter()  # pra log: valores fora do picklist
    for d in deals:
        deal_id = d["id"]
        cid = deal_to_company.get(deal_id)
        if not cid:
            stats["skip_sem_company"] += 1
            continue
        raw_origem = (company_origem_map.get(cid, "") or "").strip()
        if raw_origem == "":
            stats["skip_company_sem_origem"] += 1
            continue
        if raw_origem in AMBIGUOUS_VALUES:
            stats["skip_ambiguo_linkedin_whatsapp_site"] += 1
            continue
        if raw_origem not in PASSTHROUGH_VALUES:
            stats["skip_valor_fora_do_picklist"] += 1
            unknown_values[raw_origem] += 1
            continue
        inputs.append({
            "id": deal_id,
            "properties": {"origem_lead": raw_origem},
        })
        stats[f"patch_{raw_origem}"] += 1
    return inputs, stats, unknown_values


def execute_batches(inputs):
    """Batch update 100/100."""
    ok = 0
    erros = 0
    for i in range(0, len(inputs), 100):
        chunk = inputs[i:i + 100]
        r = requests.post(
            f"{BASE}/crm/v3/objects/deals/batch/update",
            headers=H, json={"inputs": chunk}, timeout=60,
        )
        if r.status_code in (200, 207):
            ok += len(chunk)
        else:
            erros += len(chunk)
            print(f"BATCH ERRO chunk {i}: {r.status_code} {r.text[:300]}",
                  file=sys.stderr)
        time.sleep(0.3)
    return ok, erros


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--execute", action="store_true",
                        help="DESATIVA dry-run. PATCH real.")
    args = parser.parse_args()
    if not args.execute:
        args.dry_run = True

    if not TOKEN:
        print("ERRO: HUBSPOT_TOKEN nao setado no ambiente", file=sys.stderr)
        sys.exit(1)

    mode = "DRY-RUN (sem PATCH)" if args.dry_run else "EXECUTE — PATCH REAL"
    print(f"=== Backfill origem_lead — {mode} ===")
    print()

    print("[1/3] Fetching deals sem origem_lead...")
    deals = fetch_deals_sem_origem_lead()
    print(f"      -> {len(deals)} deals")
    print()

    if not deals:
        print("Nenhum deal pra backfill. Fim.")
        return

    print("[2/3] Fetching associations deal->company...")
    d2c = fetch_deal_company_map([d["id"] for d in deals])
    print(f"      -> {len(d2c)} deals com Company associada "
          f"(out of {len(deals)})")
    print()

    print("[3/3] Fetching Company.origem dos associados...")
    origem_map = fetch_companies_origem(d2c.values())
    com_origem = sum(1 for v in origem_map.values() if (v or "").strip())
    print(f"      -> {com_origem} Companies com `origem` preenchida "
          f"(out of {len(origem_map)})")
    print()

    inputs, stats, unknown = build_patch_plan(deals, d2c, origem_map)

    print("Plano de PATCH:")
    for k, v in stats.most_common():
        print(f"  {k:30s} {v}")
    print()

    if unknown:
        print("Valores de Company.origem NAO mapeados (para revisao):")
        for val, cnt in unknown.most_common():
            print(f"  {val!r:40s} {cnt}")
        print("  (Adicionar ao NORMALIZE_MAP se quiser cobrir, ou ignorar.)")
        print()

    if args.dry_run:
        print(f"Dry-run: {len(inputs)} deals seriam patchados. Rode com --execute.")
        return

    print(f"Executando batch update em {len(inputs)} deals...")
    t0 = time.time()
    ok, erros = execute_batches(inputs)
    elapsed = time.time() - t0
    print(f"PATCH: {ok} ok, {erros} erros ({elapsed:.1f}s)")


if __name__ == "__main__":
    main()
