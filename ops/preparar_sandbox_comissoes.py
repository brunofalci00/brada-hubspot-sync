# -*- coding: utf-8 -*-
"""Cria sandbox das duas abas e fonte congelada sintética para integração."""
import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENV_PATH = r"C:\Users\bruno\.brada-secrets\hubspot.env"
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, encoding="utf-8-sig") as fh:
        for line in fh:
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())

import sync
from financeiro_match_common import is_match_won

SOURCE_SHEET = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"
TABS = ("Controle de Vendas", "Controle de Cobranças - Bia")


def hubspot_rows(fill_test_data=False):
    stages = sync.load_stages()
    owners = sync.load_owner_map()
    deals = sync.fetch_all_deals(sync.DEAL_PROPERTIES)
    associations = sync.fetch_associated_companies([d["id"] for d in deals])
    companies = sync.fetch_companies(associations.values())
    enriched = [sync.enrich(d, stages, associations, companies, owners=owners) for d in deals]
    rows = sync.build_consolidado_layer(enriched, stages=stages)
    if fill_test_data:
        for row in rows:
            if not is_match_won(row):
                continue
            defaults = {
                "cliente": "Empresa sandbox", "lei_principal": "Rouanet", "numero_projeto": f"SBX-{row['deal_id']}",
                "nome_projeto": "Projeto sandbox", "proponente": "Proponente sandbox",
                "nome_contato_proponente": "Contato sandbox", "email_proponente": "sandbox@example.com",
                "telefone_proponente": "11999999999", "numero_contrato_financeiro": f"CT-{row['deal_id']}",
                "documento_cobranca": "recibo", "condicoes_pagamento_financeiro": "À vista",
                "numero_parcelas_financeiro": "1", "closedate": "2026-08-10", "valor_bruto": 1000,
            }
            for key, value in defaults.items():
                if not str(row.get(key) or "").strip():
                    row[key] = value
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--fill-test-data", action="store_true")
    ap.add_argument("--no-copy", action="store_true")
    args = ap.parse_args()
    rows = hubspot_rows(args.fill_test_data)
    payload = {"source_ts": dt.datetime.now(dt.timezone.utc).isoformat(), "rows": rows}
    Path(args.output_json).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    if args.no_copy:
        print(f"source_json={args.output_json} rows={len(rows)}")
        return
    gc = sync.get_sheets_client()
    source = gc.open_by_key(SOURCE_SHEET)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        sandbox = gc.create(f"SANDBOX Comissoes 2026 {stamp}")
        names = {}
        for title in TABS:
            copied = source.worksheet(title).copy_to(sandbox.id)
            names[title] = copied["title"]
        for ws in sandbox.worksheets():
            if ws.title == "Sheet1":
                sandbox.del_worksheet(ws)
    except Exception as exc:
        if "storage quota" not in str(exc).lower():
            raise
        sandbox = source
        names = {}
        for title, short in ((TABS[0], "Vendas"), (TABS[1], "BIA")):
            name = f"_SANDBOX_{short}_{stamp}"
            source.duplicate_sheet(source.worksheet(title).id, new_sheet_name=name)
            names[title] = name
    print(f"sandbox_sheet_id={sandbox.id}")
    print(f"vendas_ws={names[TABS[0]]}")
    print(f"bia_ws={names[TABS[1]]}")
    print(f"source_json={args.output_json} rows={len(rows)}")


if __name__ == "__main__":
    main()
