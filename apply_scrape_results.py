"""
Sprint 1A (29/04) - Aplica PATCH no HubSpot lendo CSV de sucessos do
scrape_cnpj_from_domain.py em modo dry-run.

Evita re-rodar o scraper inteiro (gastou 9h pra 327 Companies).
Reusa validar_cnpj e patch_cnpj_hubspot do scrape_cnpj_from_domain.

Uso:
    python apply_scrape_results.py CSV_PATH --dry-run
    python apply_scrape_results.py CSV_PATH --execute
"""

import argparse
import csv
import sys

from scrape_cnpj_from_domain import patch_cnpj_hubspot, validar_cnpj, TOKEN


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path")
    parser.add_argument("--execute", action="store_true",
                        help="DESATIVA dry-run. PATCH real no HubSpot.")
    args = parser.parse_args()

    if not TOKEN:
        sys.exit("ERRO: HUBSPOT_TOKEN nao setado.")

    with open(args.csv_path, encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r["status"].startswith("ok_dry_run")]

    print(f"=== Apply scrape results ({'EXECUTE' if args.execute else 'DRY-RUN'}) ===")
    print(f"Encontrou {len(rows)} candidatos com status=ok_dry_run")
    print()

    ok, fail, skip = 0, 0, 0
    for r in rows:
        cid = r["company_id"]
        name = r["company_name"]
        cnpj = r["cnpj_encontrado"]
        if not validar_cnpj(cnpj):
            print(f"  SKIP {cid:18s} {name:40s} cnpj invalido: {cnpj}")
            skip += 1
            continue

        if not args.execute:
            print(f"  WOULD-PATCH {cid:18s} {name:40s} cnpj={cnpj}")
            ok += 1
            continue

        success, code = patch_cnpj_hubspot(cid, cnpj)
        if success:
            print(f"  OK {code:3d} {cid:18s} {name:40s} cnpj={cnpj}")
            ok += 1
        else:
            print(f"  FAIL {code:3d} {cid:18s} {name:40s} cnpj={cnpj}")
            fail += 1

    print()
    print(f"=== Resumo: ok={ok} fail={fail} skip={skip} ===")


if __name__ == "__main__":
    main()
