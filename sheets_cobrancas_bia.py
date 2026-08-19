# -*- coding: utf-8 -*-
"""Upsert HubSpot -> Controle de Cobranças - Bia (dry-run por padrao)."""
import argparse
import json
from pathlib import Path
import re
import sys

from gspread.utils import rowcol_to_a1

from sync import get_sheets_client
from sheets_reporting_financeiro_mensal import MIN_ROWS_GUARD, current_cycle, fmt_date_br, load_consolidado, map_lei, parse_closedate
from financeiro_match_common import (
    assert_fresh_source, changed_cells, completeness_gaps, deal_link, document_label,
    digits, integer_at_least_one, interno_externo, money, reconcile, select_cycle, select_match_won, sheet_date, text_id,
)

OFICIAL_ID_DEFAULT = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"
WS_DEFAULT = "Controle de Cobranças - Bia"
HEADER = [
    "Cliente", "Fonte de recurso", "Contrato", "RECIBO/NOTA", "CONDIÇÕES",
    "Proponente", "Interno/Externo", "Projeto", "Numero do projeto", "Valor",
    "VALOR A COBRAR", "PARCELAS", "Data do aporte", "Data de cobrança",
    "Nome do contato", "Telefone do proponente", "E-mail do proponente",
    "Data do ultimo contato", "resposta",
]
TECH_IDX = 28  # AC
TECH_A1 = "AC"
TECH_HEADER = "hubspot_deal_id"
AUTO = {
    "cliente": 0, "fonte": 1, "contrato": 2, "documento": 3, "condicoes": 4,
    "proponente": 5, "interno": 6, "projeto": 7, "numero": 8, "valor": 9,
    "parcelas": 11, "data": 12, "contato": 14, "telefone": 15, "email": 16,
    "tech": TECH_IDX,
}
SCHEMA = {"cliente": 0, "projeto": 7, "numero": 8, "valor": 9, "data": 12, "tech": TECH_IDX}
AUTO_INDICES = sorted(AUTO.values())


def _pad(row, width=29):
    return list(row) + [""] * max(0, width - len(row))


def read_state(sh, ws_name):
    resp = sh.values_get(f"'{ws_name}'!A1:AC2000", params={"valueRenderOption": "UNFORMATTED_VALUE"})
    vals = resp.get("values", [])
    if not vals:
        raise SystemExit(f"[abort] aba {ws_name!r} vazia")
    actual = [str(x).strip() for x in _pad(vals[0], len(HEADER))[:len(HEADER)]]
    if actual != HEADER:
        diff = [(i + 1, actual[i], HEADER[i]) for i in range(len(HEADER)) if actual[i] != HEADER[i]]
        raise SystemExit(f"[abort] header A:S divergiu do contrato: {diff}")
    tech_value = str(_pad(vals[0])[TECH_IDX]).strip()
    if tech_value and tech_value != TECH_HEADER:
        raise SystemExit(f"[abort] coluna técnica contém header inesperado: {tech_value!r}")
    has_tech = tech_value == TECH_HEADER
    records, last = [], 1
    for row_number, raw in enumerate(vals[1:], 2):
        cells = _pad(raw)
        if not any(str(c).strip() for c in cells[:19]):
            continue
        last = row_number
        records.append({"row_number": row_number, "cells": cells,
                        "deal_id": str(cells[TECH_IDX]).strip() if has_tech else ""})
    return {"records": records, "last": last, "has_tech": has_tech}


def build_row(deal):
    out = [""] * 29
    out[AUTO["cliente"]] = deal.get("cliente", "")
    out[AUTO["fonte"]] = map_lei(deal.get("lei_principal", ""))
    out[AUTO["contrato"]] = deal.get("numero_contrato_financeiro", "")
    out[AUTO["documento"]] = document_label(deal.get("documento_cobranca", ""))
    out[AUTO["condicoes"]] = deal.get("condicoes_pagamento_financeiro", "")
    out[AUTO["proponente"]] = deal.get("proponente", "")
    out[AUTO["interno"]] = interno_externo(deal)
    out[AUTO["projeto"]] = deal.get("nome_projeto", "")
    out[AUTO["numero"]] = deal.get("numero_projeto", "")
    out[AUTO["valor"]] = money(deal.get("valor_bruto")) or ""
    out[AUTO["parcelas"]] = integer_at_least_one(deal.get("numero_parcelas_financeiro")) or ""
    out[AUTO["data"]] = fmt_date_br(parse_closedate(deal.get("closedate", "")))
    out[AUTO["contato"]] = deal.get("nome_contato_proponente", "")
    out[AUTO["telefone"]] = deal.get("telefone_proponente", "")
    out[AUTO["email"]] = deal.get("email_proponente", "")
    out[TECH_IDX] = str(deal.get("deal_id", ""))
    return out


def ensure_tech(sh, ws):
    ws.update([[TECH_HEADER]], f"{TECH_A1}1", value_input_option="USER_ENTERED")
    sh.batch_update({"requests": [{"updateDimensionProperties": {
        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": TECH_IDX, "endIndex": TECH_IDX + 1},
        "properties": {"hiddenByUser": True}, "fields": "hiddenByUser"}}]})


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--sheet-id", default=OFICIAL_ID_DEFAULT)
    ap.add_argument("--ws", default=WS_DEFAULT)
    ap.add_argument("--cycle", default=None)
    ap.add_argument("--all-pending", action="store_true")
    ap.add_argument("--source-json", help="fonte congelada somente para integração/sandbox")
    args = ap.parse_args()
    cycle = args.cycle or current_cycle()
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", cycle):
        raise SystemExit("--cycle deve ser YYYY-MM")

    gc = get_sheets_client()
    if args.source_json:
        payload = json.loads(Path(args.source_json).read_text(encoding="utf-8"))
        source, source_ts = payload["rows"], payload["source_ts"]
    else:
        source, source_ts = load_consolidado(gc)
    if len(source) < MIN_ROWS_GUARD:
        raise SystemExit(f"[abort] consolidado parcial: {len(source)} < {MIN_ROWS_GUARD}")
    all_deals = select_match_won(source)
    cycle_deals = select_cycle(all_deals, cycle, all_pending=args.all_pending)
    sh = gc.open_by_key(args.sheet_id)
    ws = sh.worksheet(args.ws)
    state = read_state(sh, args.ws)
    matches, ambiguous, unmatched = reconcile(state["records"], all_deals, SCHEMA)
    cycle_ids = {str(d["deal_id"]) for d in cycle_deals}
    to_append = [d for d in unmatched if str(d["deal_id"]) in cycle_ids]

    changes, incomplete = [], []
    for match in matches:
        deal, record = match["deal"], match["row"]
        gaps = completeness_gaps(deal)
        if gaps:
            incomplete.append((deal, gaps, record["row_number"]))
            continue
        new = build_row(deal)
        for idx, old, value in changed_cells(record["cells"], new, AUTO_INDICES, {AUTO["valor"]: money, AUTO["parcelas"]: integer_at_least_one, AUTO["data"]: sheet_date, AUTO["telefone"]: digits, AUTO["numero"]: text_id, AUTO["tech"]: text_id}):
            changes.append((record["row_number"], idx, old, value, deal))
    append_ok = []
    for deal in to_append:
        gaps = completeness_gaps(deal)
        if gaps:
            incomplete.append((deal, gaps, None))
        else:
            append_ok.append(deal)

    print(f"Controle de Cobranças - Bia | ciclo={cycle} | fonte={source_ts} | MATCH won={len(all_deals)}")
    print(f"existentes={len(state['records'])} matches={len(matches)} ambiguos={len(ambiguous)} updates={len(changes)} append={len(append_ok)} incompletos={len(incomplete)}")
    for item in ambiguous:
        print(f"[AMBIGUO] linha {item['row']['row_number']}: {[d['deal_id'] for d in item['candidates']]}")
    for deal, gaps, row in incomplete:
        print(f"[PENDENTE] linha={row or 'nova'} deal={deal['deal_id']} faltam={','.join(gaps)} {deal_link(deal)}")
    for row, idx, old, value, deal in changes[:100]:
        print(f"[DIFF] {rowcol_to_a1(row, idx+1)} {old!r} -> {value!r} deal={deal['deal_id']}")

    if not args.write:
        print("[dry-run] nada escrito")
        return
    assert_fresh_source(source_ts)
    ensure_tech(sh, ws)
    if changes:
        ws.batch_update([{"range": rowcol_to_a1(row, idx + 1), "values": [[value]]}
                         for row, idx, _old, value, _deal in changes], value_input_option="USER_ENTERED")
    if append_ok:
        start = state["last"] + 1
        rows = [build_row(deal) for deal in append_ok]
        ws.update(rows, f"A{start}:AC{start + len(rows) - 1}", value_input_option="USER_ENTERED")
    print(f"[write] OK updates={len(changes)} append={len(append_ok)}")


if __name__ == "__main__":
    main()
