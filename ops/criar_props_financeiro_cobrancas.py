# -*- coding: utf-8 -*-
"""Cria/read-back das quatro propriedades financeiras de Deal.

Dry-run por padrao; --apply efetiva. Idempotente e aborta em conflito de schema.
O limite inteiro >=1 e validado pela automacao; no HubSpot a obrigatoriedade por
etapa sera configurada manualmente pelo Bruno.
"""
import argparse
import os
import sys

import requests

BASE = "https://api.hubapi.com/crm/v3/properties/deals"
ENV_PATH = r"C:\Users\bruno\.brada-secrets\hubspot.env"

PROPS = [
    {"name": "numero_contrato_financeiro", "label": "Número do contrato", "type": "string", "fieldType": "text",
     "groupName": "dealinformation", "formField": False,
     "description": "Número alfanumérico do contrato para cobrança/financeiro."},
    {"name": "documento_cobranca", "label": "Documento de cobrança", "type": "enumeration", "fieldType": "select",
     "groupName": "dealinformation", "formField": False,
     "description": "Documento que o financeiro deve emitir.",
     "options": [
         {"label": "Recibo", "value": "recibo", "displayOrder": 0, "hidden": False},
         {"label": "Nota Fiscal", "value": "nota_fiscal", "displayOrder": 1, "hidden": False},
     ]},
    {"name": "condicoes_pagamento_financeiro", "label": "Cronograma de pagamento (Financeiro)", "type": "string", "fieldType": "textarea",
     "groupName": "dealinformation", "formField": False,
     "description": "Valores, datas e detalhamento das parcelas para cobrança. Não substitui condicao_de_pagamento de Elaboração."},
    {"name": "numero_parcelas_financeiro", "label": "Número de parcelas", "type": "number", "fieldType": "number",
     "groupName": "dealinformation", "formField": False,
     "description": "Número inteiro de parcelas, mínimo 1. Use 1 para pagamento à vista."},
]


def load_env():
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, encoding="utf-8-sig") as fh:
            for line in fh:
                if "=" in line and not line.lstrip().startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())
    token = os.environ.get("HUBSPOT_TOKEN")
    if not token:
        raise SystemExit("HUBSPOT_TOKEN ausente")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def schema_conflicts(actual, expected):
    conflicts = []
    for field in ("name", "type", "fieldType", "groupName"):
        if actual.get(field) != expected.get(field):
            conflicts.append(f"{field}: atual={actual.get(field)!r} esperado={expected.get(field)!r}")
    if expected["type"] == "enumeration":
        actual_options = {(o.get("label"), o.get("value")) for o in actual.get("options", []) if not o.get("hidden")}
        expected_options = {(o["label"], o["value"]) for o in expected["options"]}
        if actual_options != expected_options:
            conflicts.append(f"options: atual={sorted(actual_options)} esperado={sorted(expected_options)}")
    return conflicts


def run(apply=False, session=requests):
    headers = load_env()
    created = 0
    for spec in PROPS:
        response = session.get(f"{BASE}/{spec['name']}", headers=headers, timeout=30)
        if response.status_code == 200:
            conflicts = schema_conflicts(response.json(), spec)
            if conflicts:
                raise SystemExit(f"[CONFLITO] {spec['name']}: {'; '.join(conflicts)}")
            print(f"[OK existente] {spec['name']} {spec['type']}/{spec['fieldType']}")
            continue
        if response.status_code != 404:
            raise SystemExit(f"GET {spec['name']} falhou: {response.status_code} {response.text[:300]}")
        print(f"[CRIAR] {spec['name']} {spec['type']}/{spec['fieldType']}")
        if not apply:
            continue
        created_response = session.post(BASE, headers=headers, json=spec, timeout=30)
        if created_response.status_code not in (200, 201):
            raise SystemExit(f"POST {spec['name']} falhou: {created_response.status_code} {created_response.text[:500]}")
        readback = session.get(f"{BASE}/{spec['name']}", headers=headers, timeout=30)
        if readback.status_code != 200:
            raise SystemExit(f"read-back {spec['name']} falhou: {readback.status_code}")
        conflicts = schema_conflicts(readback.json(), spec)
        if conflicts:
            raise SystemExit(f"[READ-BACK CONFLITO] {spec['name']}: {'; '.join(conflicts)}")
        created += 1
        print(f"  [OK read-back] {spec['name']}")
    print(f"resultado: criadas={created} modo={'apply' if apply else 'dry-run'}")
    return created


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    run(apply=args.apply)


if __name__ == "__main__":
    main()
