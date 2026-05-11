"""
One-shot pra reclassificar Companies com `origem = "Linkedin / Whatsapp / Site"`
(valor legado ambíguo) como `origem = "LinkedIn"`.

Decisão Bruno 11/05/2026: tradeoff aceito de granularidade < velocidade.
Os 75 cos legadas viram LinkedIn em batch. Após próximo cron, auto-herança
em `patch_derived_back` (sync.py:730-872) propaga pros Deals associados
que estejam vazios.

Após este script rodar com --execute, a Fase A do plano (arquivar a option
"Linkedin / Whatsapp / Site" do picklist Company.origem na UI HubSpot)
fica desbloqueada — nenhuma Company vai mais ter esse valor.

Uso:
    python backfill_origem_linkedin_legado.py          # dry-run (default)
    python backfill_origem_linkedin_legado.py --execute  # PATCH real

Reusa padrão de backfill_origem_lead.py (search + batch update 100/100).
"""
import argparse
import io
import os
import sys
import time

import requests

# Windows console: forcar utf-8 pra labels com acento nao virarem mojibake
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

BASE = "https://api.hubapi.com"
TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

VALOR_LEGADO = "Linkedin / Whatsapp / Site"
VALOR_NOVO = "LinkedIn"


def fetch_companies_com_legado():
    """Search Companies com origem == valor legado ambiguo."""
    cos = []
    after = None
    while True:
        body = {
            "limit": 100,
            "properties": ["name", "origem"],
            "filterGroups": [
                {"filters": [{"propertyName": "origem", "operator": "EQ", "value": VALOR_LEGADO}]},
            ],
        }
        if after:
            body["after"] = after
        r = requests.post(
            f"{BASE}/crm/v3/objects/companies/search",
            headers=H, json=body, timeout=30,
        )
        if r.status_code != 200:
            print(f"ERRO search: {r.status_code} {r.text[:300]}", file=sys.stderr)
            sys.exit(1)
        d = r.json()
        cos.extend(d.get("results", []))
        nxt = d.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt.get("after")
        time.sleep(0.2)
    return cos


def execute_batches(inputs):
    """Batch update 100/100."""
    ok = 0
    erros = 0
    for i in range(0, len(inputs), 100):
        chunk = inputs[i:i + 100]
        r = requests.post(
            f"{BASE}/crm/v3/objects/companies/batch/update",
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
    parser.add_argument("--execute", action="store_true",
                        help="DESATIVA dry-run. PATCH real.")
    args = parser.parse_args()
    dry_run = not args.execute

    if not TOKEN:
        print("ERRO: HUBSPOT_TOKEN nao setado no ambiente", file=sys.stderr)
        sys.exit(1)

    mode = "DRY-RUN (sem PATCH)" if dry_run else "EXECUTE — PATCH REAL"
    print(f"=== Backfill Company.origem legado → LinkedIn — {mode} ===\n")

    print("[1/2] Fetching Companies com origem = legado ambiguo...")
    cos = fetch_companies_com_legado()
    print(f"      -> {len(cos)} companies\n")

    if not cos:
        print("Nenhuma company com valor legado. Fim.")
        return

    print("Sample dos 10 primeiros:")
    for c in cos[:10]:
        cid = c["id"]
        nome = c["properties"].get("name") or "(sem nome)"
        print(f"  {cid}  {nome[:60]}")
    if len(cos) > 10:
        print(f"  ... +{len(cos) - 10} mais\n")
    else:
        print()

    inputs = [
        {"id": c["id"], "properties": {"origem": VALOR_NOVO}}
        for c in cos
    ]
    print(f"Plano: PATCH em {len(inputs)} companies (origem → '{VALOR_NOVO}').\n")

    if dry_run:
        print(f"Dry-run: nada aplicado. Rode com --execute pra patchar.")
        return

    print("Executando batch update...")
    t0 = time.time()
    ok, erros = execute_batches(inputs)
    elapsed = time.time() - t0
    print(f"PATCH: {ok} ok, {erros} erros ({elapsed:.1f}s)")
    print(f"\nProximo passo: arquivar option '{VALOR_LEGADO}' do picklist "
          f"Company.origem na UI HubSpot (Fase A do plano).")


if __name__ == "__main__":
    main()
