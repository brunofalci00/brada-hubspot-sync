"""
Retry pos-scraper (22/04): os 84 CNPJs descobertos mas que deram
patch_failed_400 por causa do bug de formato (format_cnpj enviava
com mascara, HubSpot so aceita so digitos).

Le o CSV do scraper, filtra patch_failed_400, normaliza CNPJ e
faz PATCH. Sem re-scraping.

Uso:
    python retry_patch_cnpj.py <csv_log>
"""
import csv
import os
import sys
import time

import requests

BASE = "https://api.hubapi.com"
TOKEN = os.environ["HUBSPOT_TOKEN"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def only_digits(s):
    return "".join(c for c in s if c.isdigit())


def patch_cnpj(cid, cnpj_digits):
    body = {"properties": {"cnpj": cnpj_digits}}
    r = requests.patch(
        f"{BASE}/crm/v3/objects/companies/{cid}",
        headers=HEADERS, json=body, timeout=30,
    )
    return r.status_code, r.text[:200] if r.status_code not in (200, 201) else ""


def main():
    if len(sys.argv) < 2:
        print("uso: python retry_patch_cnpj.py <csv_log>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    with open(csv_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Os casos que tem CNPJ descoberto mas PATCH falhou por formato
    targets = [r for r in rows if r["status"] == "patch_failed_400" and r["cnpj_encontrado"]]
    print(f"Targets (patch_failed_400 com CNPJ descoberto): {len(targets)}")
    print()

    ok = 0
    errs = 0
    for i, r in enumerate(targets, 1):
        cid = r["company_id"]
        name = r["company_name"][:40]
        cnpj_digits = only_digits(r["cnpj_encontrado"])
        if len(cnpj_digits) != 14:
            print(f"  [{i:3d}/{len(targets)}] {name:40s} | SKIP cnpj invalido: {r['cnpj_encontrado']}")
            errs += 1
            continue
        code, err = patch_cnpj(cid, cnpj_digits)
        if code in (200, 201):
            ok += 1
            if i % 10 == 0 or i <= 5:
                print(f"  [{i:3d}/{len(targets)}] {name:40s} | OK   ({cnpj_digits})")
        else:
            errs += 1
            print(f"  [{i:3d}/{len(targets)}] {name:40s} | ERR {code}: {err}")
        time.sleep(0.3)

    print()
    print(f"=== RESUMO ===")
    print(f"  OK      : {ok}/{len(targets)}")
    print(f"  Errors  : {errs}/{len(targets)}")


if __name__ == "__main__":
    main()
