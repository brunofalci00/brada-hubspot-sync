"""
Backfill one-shot do campo `produto` no HubSpot.

Regra (decisao Bruno 22/04 — alinhada com PIPELINE_TO_PRODUTO do sync.py):
  pipeline == "default"     -> produto = "Match"
  pipeline == "839644419"   -> produto = "Elaboracao"
  outros                     -> skip (log)

So patcha deals com produto VAZIO (regra "nao sobrescrever" via
Search filter NOT_HAS_PROPERTY).

Uso:
    python backfill_produto.py --dry-run     # contagem sem PATCH (default)
    python backfill_produto.py --execute     # PATCH real via batch update

Reusa padroes existentes:
- sync.py::patch_default_trabalhado_por (batch update 100/100)
- sync.py::PIPELINE_TO_PRODUTO (mapeamento canonico)
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

# Mapping alinhado com sync.py:468
PIPELINE_TO_PRODUTO = {
    "default": "Match",
    "839644419": "Elaboração",
}


def fetch_deals_sem_produto():
    """Search API: deals com produto NOT_HAS_PROPERTY (vazio)."""
    deals = []
    after = None
    while True:
        body = {
            "limit": 100,
            "properties": ["pipeline", "dealname", "produto"],
            "filterGroups": [{
                "filters": [
                    {"propertyName": "produto", "operator": "NOT_HAS_PROPERTY"},
                ]
            }],
        }
        if after:
            body["after"] = after
        r = requests.post(
            f"{BASE}/crm/v3/objects/deals/search",
            headers=H, json=body, timeout=30,
        )
        if r.status_code != 200:
            print(f"ERRO search: {r.status_code} {r.text[:200]}", file=sys.stderr)
            sys.exit(1)
        d = r.json()
        deals.extend(d.get("results", []))
        nxt = d.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt.get("after")
        time.sleep(0.2)
    return deals


def build_patch_plan(deals):
    """Para cada deal, determina produto pelo pipeline.

    Retorna (inputs_batch, stats_counter).
    """
    inputs = []
    stats = Counter()
    for d in deals:
        pid = (d.get("properties", {}) or {}).get("pipeline", "")
        produto = PIPELINE_TO_PRODUTO.get(pid)
        if not produto:
            stats["skip_pipeline_desconhecido"] += 1
            print(f"  SKIP pipeline desconhecido: deal_id={d['id']} pipeline={pid!r}")
            continue
        inputs.append({"id": d["id"], "properties": {"produto": produto}})
        stats[f"patch_{produto}"] += 1
    return inputs, stats


def execute_batches(inputs):
    """Batch update 100/100 via /crm/v3/objects/deals/batch/update."""
    ok = 0
    erros = 0
    for i in range(0, len(inputs), 100):
        chunk = inputs[i:i + 100]
        body = {"inputs": chunk}
        r = requests.post(
            f"{BASE}/crm/v3/objects/deals/batch/update",
            headers=H, json=body, timeout=60,
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
        print("ERRO: HUBSPOT_TOKEN nao setado", file=sys.stderr)
        sys.exit(1)

    mode = "DRY-RUN (sem PATCH)" if args.dry_run else "EXECUTE — PATCH REAL"
    print(f"=== Backfill produto — {mode} ===")
    print()

    print("Fetching deals com produto vazio...")
    deals = fetch_deals_sem_produto()
    print(f"Total encontrados: {len(deals)}")
    print()

    inputs, stats = build_patch_plan(deals)
    print("Distribuicao:")
    for k, v in stats.most_common():
        print(f"  {k:30s} {v}")
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
