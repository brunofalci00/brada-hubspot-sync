"""Cria 3 properties de CONTATO DO PROPONENTE no Deal (HubSpot) — idempotente.

Reuniao Ivan 12/06 ([[ata_ivan_financeiro_12jun]]): o financeiro precisa cobrar o
PROPONENTE (sobretudo projeto externo). Hoje o deal so tem contato do INCENTIVADOR
(`email`/`telefone`, criados 08/06). Estas sao do proponente, distintas:

  nome_contato_proponente  (texto)            Nome do contato do proponente
  email_proponente         (texto)            E-mail do proponente
  telefone_proponente      (phonenumber)      Telefone do proponente

Co-locadas no mesmo grupo do `nome_do_proponente` -> aparecem na secao "Financeiro"
do card, ao lado do proponente. NAO e o publico "proponente plataforma/elaboracao"
(que nao vai pro HubSpot) — e o proponente do deal Incentivador (entidade da lei).

Idempotente: pula as que ja existem. NAO popula nenhum deal (forward-fill = executivo).

Uso: python ops/criar_props_contato_proponente.py            # dry-run
     python ops/criar_props_contato_proponente.py --apply    # cria de fato
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

# (name, label, fieldType, descricao). type='string' nos 3; fieldType varia.
PROPS = [
    ("nome_contato_proponente", "Nome do contato do proponente", "text",
     "Nome da pessoa de contato do PROPONENTE (quem o financeiro aciona p/ cobranca). "
     "Distinto do contato do incentivador. Preenchido pelo executivo."),
    ("email_proponente", "E-mail do proponente", "text",
     "E-mail do contato do PROPONENTE p/ cobranca pelo financeiro. Distinto do e-mail do incentivador."),
    ("telefone_proponente", "Telefone do proponente", "phonenumber",
     "Telefone do contato do PROPONENTE p/ cobranca pelo financeiro. Distinto do telefone do incentivador."),
]


def group_of(prop, fallback="dealinformation"):
    r = requests.get(f"{BASE}/{prop}", headers=H, timeout=30)
    return r.json().get("groupName") if r.status_code == 200 else fallback


def main():
    apply = "--apply" in sys.argv
    # co-locar com nome_do_proponente (secao Financeiro do card)
    group = group_of("nome_do_proponente")
    print(f"grupo destino: {group!r}\n")

    for name, label, field_type, desc in PROPS:
        r = requests.get(f"{BASE}/{name}", headers=H, timeout=30)
        if r.status_code == 200:
            j = r.json()
            print(f"[JA EXISTE] {name} | type={j.get('type')}/{j.get('fieldType')} group={j.get('groupName')}")
            continue
        if r.status_code != 404:
            print(f"[ERRO] GET {name} status={r.status_code}: {r.text[:200]}")
            continue
        payload = {"name": name, "label": label, "type": "string", "fieldType": field_type,
                   "description": desc, "groupName": group, "formField": False}
        print(f"[CRIAR] {name} ({label}) type=string/{field_type} group={group}")
        if not apply:
            continue
        cr = requests.post(BASE, headers=H, json=payload, timeout=30)
        if cr.status_code in (200, 201):
            print(f"  OK criada: {cr.json().get('name')}")
        else:
            print(f"  FALHA status={cr.status_code}: {cr.text[:300]}")

    if not apply:
        print("\nDRY-RUN. Rode com --apply pra criar de fato.")


if __name__ == "__main__":
    main()
