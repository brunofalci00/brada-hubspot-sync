"""Cria a property `percentual_brada` no Deal (HubSpot) — idempotente.

S3 (27/06, PROMPT_S3_Percentual_Real_Match_Brada): % real que a Brada recebe por
deal, preenchido pelo executivo no momento do match. Substitui o 10/15 fixo do
consolidado (sync.py) por valor real; fallback 10/15 quando vazio.

Co-locada no grupo de `nome_do_proponente` (dealinformation) -> secao Financeiro
do card. NAO popula nenhum deal (forward-fill = executivo; backfill = outro script).

Uso: python sb3_criar_property.py            # dry-run
     python sb3_criar_property.py --apply    # cria de fato
"""
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import requests

ENV_PATH = r"C:\Users\bruno\.brada-secrets\hubspot.env"
with open(ENV_PATH, encoding="utf-8-sig") as fh:
    for line in fh:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

H = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
BASE = "https://api.hubapi.com/crm/v3/properties/deals"

NAME = "percentual_brada"
LABEL = "% Brada (real)"
DESC = ("Percentual real que a Brada recebe nessa venda (inteiro: 15 = 15%, 8 = 8%). "
        "Executivo preenche no momento do match. Match interno default 15%, externo "
        "varia caso a caso. CRIAPE/Elaboracao fora do escopo.")


def group_of(prop, fallback="dealinformation"):
    r = requests.get(f"{BASE}/{prop}", headers=H, timeout=30)
    return r.json().get("groupName") if r.status_code == 200 else fallback


def main():
    apply = "--apply" in sys.argv
    group = group_of("nome_do_proponente")
    print(f"grupo destino: {group!r}\n")

    r = requests.get(f"{BASE}/{NAME}", headers=H, timeout=30)
    if r.status_code == 200:
        j = r.json()
        print(f"[JA EXISTE] {NAME} | type={j.get('type')}/{j.get('fieldType')} group={j.get('groupName')}")
        return
    if r.status_code != 404:
        print(f"[ERRO] GET {NAME} status={r.status_code}: {r.text[:200]}")
        return

    payload = {
        "name": NAME,
        "label": LABEL,
        "type": "number",
        "fieldType": "number",
        "groupName": group,
        "description": DESC,
        "displayOrder": 99,
        "showCurrencySymbol": False,
        "formField": False,
    }
    print(f"[CRIAR] {NAME} ({LABEL}) type=number/number group={group}")
    print(f"  payload: {payload}")
    if not apply:
        print("\nDRY-RUN. Rode com --apply pra criar de fato.")
        return

    cr = requests.post(BASE, headers=H, json=payload, timeout=30)
    if cr.status_code in (200, 201):
        print(f"  OK criada: {cr.json().get('name')}")
    elif cr.status_code == 400 and "showCurrencySymbol" in cr.text:
        payload.pop("showCurrencySymbol", None)
        print("  400 em showCurrencySymbol -> retry sem o campo")
        cr2 = requests.post(BASE, headers=H, json=payload, timeout=30)
        if cr2.status_code in (200, 201):
            print(f"  OK criada (sem showCurrencySymbol): {cr2.json().get('name')}")
        else:
            print(f"  FALHA status={cr2.status_code}: {cr2.text[:300]}")
    else:
        print(f"  FALHA status={cr.status_code}: {cr.text[:300]}")


if __name__ == "__main__":
    main()
