"""
Aplica PATCH dos CNPJs ALTA confianca do Sprint 2 (Casa dos Dados via WebSearch).

Le sprint2_alta.csv (output do process_sprint2.py) e PATCH no HubSpot.
Reusa validar_cnpj e patch_cnpj_hubspot do scrape_cnpj_from_domain.

Uso:
    python apply_sprint2.py                # dry-run (default)
    python apply_sprint2.py --execute      # PATCH real
"""

import argparse
import csv
import os
import sys

from scrape_cnpj_from_domain import patch_cnpj_hubspot, validar_cnpj, TOKEN

CSV_PATH = os.path.join(os.path.dirname(__file__), "sprint2_alta.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--csv", default=CSV_PATH)
    args = parser.parse_args()

    if not TOKEN:
        sys.exit("HUBSPOT_TOKEN nao setado.")

    with open(args.csv, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"=== Apply Sprint 2 ALTA ({'EXECUTE' if args.execute else 'DRY-RUN'}) ===")
    print(f"{len(rows)} candidatos")
    print()

    ok = fail = skip = 0
    for r in rows:
        cid = r["company_id"]
        name = r["name_hubspot"]
        cnpj = r["cnpj_escolhido"]
        if not cnpj or not validar_cnpj(cnpj):
            print(f"  SKIP {cid:18s} {name[:40]:40s} cnpj_invalido")
            skip += 1
            continue

        if not args.execute:
            print(f"  WOULD-PATCH {cid:18s} {name[:40]:40s} cnpj={cnpj}")
            ok += 1
            continue

        success, code = patch_cnpj_hubspot(cid, cnpj)
        if success:
            print(f"  OK {code} {cid:18s} {name[:40]:40s} cnpj={cnpj}")
            ok += 1
        else:
            print(f"  FAIL {code} {cid:18s} {name[:40]:40s} cnpj={cnpj}")
            fail += 1

    print()
    print(f"=== Resumo: ok={ok} fail={fail} skip={skip} ===")


if __name__ == "__main__":
    main()
