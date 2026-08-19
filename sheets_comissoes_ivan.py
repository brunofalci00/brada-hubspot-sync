# -*- coding: utf-8 -*-
"""Upsert HubSpot -> aba oficial Controle de Vendas (dry-run por padrao)."""
import argparse
import json
from pathlib import Path
import re
import sys

from gspread.utils import rowcol_to_a1

from sync import get_sheets_client
from sheets_reporting_financeiro_mensal import (
    MIN_ROWS_GUARD, current_cycle, fmt_date_br, load_consolidado, map_lei, parse_closedate,
)
from financeiro_match_common import (
    assert_fresh_source, changed_cells, completeness_gaps, deal_link,
    digits as _digits, interno_externo, money, norm as _norm, reconcile, select_cycle, select_match_won, sheet_date, text_id,
)

from hubspot_financeiro import enriquecer as enriquecer_financeiras

OFICIAL_ID_DEFAULT = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"
CV_WS = "Controle de Vendas"
HEADER = [
    "CLIENTE", "Fonte de recurso", "Nome do contato", "Telefone do proponente",
    "E-mail do proponente", "Proponente", "Projeto", "Numero do projeto",
    "Nº conta M", "Nº conta C", "Valor", "Data do aporte",
    "DATA na Conta Movimentação", "Valor que caiu na conta", "CONDIÇÕES",
    "Interno ou externo?", "Comissão BRADA", "Líquido Brada", "Comissão Ivan 8%",
    "Comissão Jaque 4%", "Comissão externo 3%", "Comissão externo 3%",
    "Comissão 10%", "Comissão 15%  Sergio", "Comissão 20% Grant Thorton",
    "Comissão GT 80% Lei do Bem",
]
TECH_IDX = 31  # AF
TECH_A1 = "AF"
TECH_HEADER = "hubspot_deal_id"
AUTO = {
    "cliente": 0, "fonte": 1, "contato": 2, "telefone": 3, "email": 4,
    "proponente": 5, "projeto": 6, "numero": 7, "valor": 10, "data": 11,
    "condicoes": 14, "interno": 15, "tech": TECH_IDX,
}
SCHEMA = {k: AUTO[k] for k in ("cliente", "projeto", "numero", "valor", "data", "tech")}
AUTO_INDICES = sorted(AUTO.values())
PROTECTED_REQUIRED = [(15, 19)]  # P:S, end exclusive

def utf8_stdout():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _pad(row, width=32):
    return list(row) + [""] * max(0, width - len(row))


def read_state(sh, ws_name):
    resp = sh.values_get(f"'{ws_name}'!A1:AF2000", params={"valueRenderOption": "UNFORMATTED_VALUE"})
    vals = resp.get("values", [])
    if not vals:
        raise SystemExit(f"[abort] aba {ws_name!r} vazia")
    actual = [str(x).strip() for x in _pad(vals[0], len(HEADER))[:len(HEADER)]]
    if actual != HEADER:
        diff = [(i + 1, actual[i], HEADER[i]) for i in range(len(HEADER)) if actual[i] != HEADER[i]]
        raise SystemExit(f"[abort] header A:Z divergiu do contrato: {diff}")
    tech_value = str(_pad(vals[0])[TECH_IDX]).strip()
    if tech_value and tech_value != TECH_HEADER:
        raise SystemExit(f"[abort] coluna técnica contém header inesperado: {tech_value!r}")
    has_tech = tech_value == TECH_HEADER
    records, last = [], 1
    for row_number, raw in enumerate(vals[1:], 2):
        cells = _pad(raw)
        if not any(str(c).strip() for c in cells[:16]):
            continue
        last = row_number
        records.append({"row_number": row_number, "cells": cells,
                        "deal_id": str(cells[TECH_IDX]).strip() if has_tech else ""})
    return {"records": records, "last": last, "has_tech": has_tech}


def build_row(deal, row_number=None):
    out = [""] * 32
    out[AUTO["cliente"]] = deal.get("cliente", "")
    out[AUTO["fonte"]] = map_lei(deal.get("lei_principal", ""))
    out[AUTO["contato"]] = deal.get("nome_contato_proponente", "")
    out[AUTO["telefone"]] = deal.get("telefone_proponente", "")
    out[AUTO["email"]] = deal.get("email_proponente", "")
    out[AUTO["proponente"]] = deal.get("proponente", "")
    out[AUTO["projeto"]] = deal.get("nome_projeto", "")
    out[AUTO["numero"]] = deal.get("numero_projeto", "")
    out[AUTO["valor"]] = money(deal.get("valor_bruto")) or ""
    out[AUTO["data"]] = fmt_date_br(deal.get("_date")) if deal.get("_date") else fmt_date_br(parse_closedate(deal.get("closedate", "")))
    out[AUTO["condicoes"]] = deal.get("condicoes_pagamento_financeiro", "")
    out[AUTO["interno"]] = interno_externo(deal)
    out[TECH_IDX] = str(deal.get("deal_id", ""))
    if row_number:
        out[16] = f'=IF(P{row_number}="Externo";K{row_number}*10%;IF(P{row_number}="Interno";K{row_number}*15%;0))'
        out[17] = f'=IF(OR(P{row_number}="Externo";P{row_number}="Interno");Q{row_number}*(1-12%);0)'
        out[18] = f'=IF(OR(P{row_number}="Externo";P{row_number}="Interno");R{row_number}*8%;0)'
        out[19] = f'=IF(OR(P{row_number}="Externo";P{row_number}="Interno");R{row_number}*4%;0)'
    return out


def preflight_protections(sh, ws, service_email):
    meta = sh.fetch_sheet_metadata(params={"includeGridData": False})
    sheet = next(s for s in meta["sheets"] if s["properties"]["sheetId"] == ws.id)
    blocked = []
    for protected in sheet.get("protectedRanges", []):
        if protected.get("warningOnly"):
            continue
        grid = protected.get("range", {})
        start, end = grid.get("startColumnIndex", 0), grid.get("endColumnIndex", 10**6)
        if not any(max(start, a) < min(end, b) for a, b in PROTECTED_REQUIRED):
            continue
        editors = protected.get("editors", {})
        users = {u.lower() for u in editors.get("users", [])}
        if service_email.lower() not in users and not editors.get("domainUsersCanEdit", False):
            blocked.append(protected.get("description") or protected.get("protectedRangeId"))
    if blocked:
        raise SystemExit(f"[abort] service account sem permissao explicita nas protecoes P:S: {blocked}")


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
    ap.add_argument("--ws", default=CV_WS)
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
    # As 4 properties financeiras nao existem no consolidado de producao; a coluna
    # CONDICOES sai delas. Mesma fonte que a aba da Bia usa, para as duas nao
    # divergirem. Ver hubspot_financeiro.
    enriquecer_financeiras(all_deals)
    cycle_deals = select_cycle(all_deals, cycle, all_pending=args.all_pending)
    sh = gc.open_by_key(args.sheet_id)
    ws = sh.worksheet(args.ws)
    state = read_state(sh, args.ws)
    matches, ambiguous, unmatched = reconcile(state["records"], all_deals, SCHEMA)
    append_ids = {str(d["deal_id"]) for d in cycle_deals}
    to_append = [d for d in unmatched if str(d["deal_id"]) in append_ids]

    changes, incomplete = [], []
    for match in matches:
        deal, record = match["deal"], match["row"]
        gaps = completeness_gaps(deal)
        if gaps:
            incomplete.append((deal, gaps, record["row_number"]))
            continue
        new = build_row(deal)
        for idx, old, value in changed_cells(record["cells"], new, AUTO_INDICES, {AUTO["valor"]: money, AUTO["data"]: sheet_date, AUTO["telefone"]: _digits, AUTO["numero"]: text_id, AUTO["tech"]: text_id}):
            changes.append((record["row_number"], idx, old, value, deal))
    append_ok = []
    for deal in to_append:
        gaps = completeness_gaps(deal)
        if gaps:
            incomplete.append((deal, gaps, None))
        else:
            append_ok.append(deal)

    print(f"Controle de Vendas | ciclo={cycle} | fonte={source_ts} | MATCH won={len(all_deals)}")
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
    credentials = getattr(gc, "auth", None) or getattr(getattr(gc, "http_client", None), "auth", None)
    service_email = getattr(credentials, "service_account_email", "")
    if not service_email:
        raise SystemExit("[abort] credencial do service account não identificada")
    preflight_protections(sh, ws, service_email)
    ensure_tech(sh, ws)
    if changes:
        ws.batch_update([{"range": rowcol_to_a1(row, idx + 1), "values": [[value]]}
                         for row, idx, _old, value, _deal in changes], value_input_option="USER_ENTERED")
    if append_ok:
        start = state["last"] + 1
        rows = [build_row(deal, start + offset) for offset, deal in enumerate(append_ok)]
        ws.update(rows, f"A{start}:AF{start + len(rows) - 1}", value_input_option="USER_ENTERED")
    print(f"[write] OK updates={len(changes)} append={len(append_ok)}")


if __name__ == "__main__":
    main()
