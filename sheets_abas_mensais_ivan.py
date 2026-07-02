# -*- coding: utf-8 -*-
"""
Geracao das ABAS MENSAIS da planilha OFICIAL de Comissoes do Ivan (1XVRuIMN...),
no fecho do ciclo (dia 20). Cada ciclo gera/popula a aba do mes a partir do
template do mes anterior, SO nas colunas automaticas; comissoes ficam em branco
(Luciana/Ricardo preenchem).

Frentes (Bruno 30/06):
  B. {Mes}_MATCH      <- consolidado (Match Won do ciclo). Colunas A-H + D
                         (Interno/Externo) + N/O/P contato + Q deal_id (ocultas).
  C. {Mes}_Elaboracao <- HubSpot direto (pipeline Proponente ganho). A-F, com
                         C = produto cru. (implementado na Frente C)
  D. Tabela Ricardo   <- HubSpot direto (Elaboracao won 2026 + link). (Frente D)

Junho esta FECHADO (template, nunca escrito). Tudo aqui e preparatorio pro run
de 20/julho. Dry-run default; --write gated; geracao idempotente (dedup por
deal_id na coluna tecnica oculta).

Uso:
  python sheets_abas_mensais_ivan.py --match-mes                 # dry-run MATCH do ciclo
  python sheets_abas_mensais_ivan.py --match-mes --cycle 2026-06 --write
  python sheets_abas_mensais_ivan.py --match-mes --sheet-id <sandbox> --write
"""

import argparse
import re
import sys

import gspread
from gspread.utils import rowcol_to_a1

from sync import get_sheets_client, PORTAL_ID  # noqa: F401
from sheets_reporting_financeiro_mensal import (
    parse_brl, parse_closedate, fmt_date_br, map_lei,
    load_consolidado, split_vendas, current_cycle, cycle_window, MIN_ROWS_GUARD,
)
from sheets_comissoes_ivan import select_cycle, _norm, _digits, utf8_stdout

OFICIAL_ID_DEFAULT = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"

MES_PT = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
          7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"}


def cycle_to_mes(cycle):
    """'2026-07' -> 'Julho' (nome PT do mes do FIM do ciclo)."""
    return MES_PT[int(cycle[5:7])]


# ===================================================
# GERACAO DE ABA DO MES (generico)
# ===================================================

def hide_columns(sh, ws, start_idx, count):
    """Oculta `count` colunas a partir de start_idx (0-based) via hiddenByUser."""
    sh.batch_update({
        "requests": [{
            "updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": start_idx, "endIndex": start_idx + count},
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        }]
    })


def hide_sheet(sh, ws):
    """Oculta a aba inteira (pra validacao antes de publicar)."""
    sh.batch_update({
        "requests": [{
            "updateSheetProperties": {
                "properties": {"sheetId": ws.id, "hidden": True},
                "fields": "hidden",
            }
        }]
    })


def ensure_month_tab(sh, template_name, new_name, extra_headers, n_template_cols):
    """Garante a aba do mes. Se NAO existe: duplica o template (preserva
    formatacao), limpa os dados (linhas 2+), acrescenta extra_headers a partir
    da coluna n_template_cols (0-based) e oculta as colunas extras. Retorna
    (ws, criada_agora)."""
    titles = {w.title: w for w in sh.worksheets()}
    if new_name in titles:
        return titles[new_name], False
    template = titles.get(template_name)
    if template is None:
        raise SystemExit(f"[abort] template '{template_name}' nao existe na planilha.")
    ws = sh.duplicate_sheet(template.id, new_sheet_name=new_name)
    # limpar dados do template (mantem linha 1 = header)
    ws.batch_clear([f"A2:AZ{max(ws.row_count, 2)}"])
    if extra_headers:
        c0 = rowcol_to_a1(1, n_template_cols + 1)
        c1 = rowcol_to_a1(1, n_template_cols + len(extra_headers))
        ws.update(values=[extra_headers], range_name=f"{c0}:{c1}", value_input_option="USER_ENTERED")
        hide_columns(sh, ws, n_template_cols, len(extra_headers))
    return ws, True


def read_existing(sh, tab, n_cols, tech_idx):
    """Le a aba (se existe). Retorna (existe, last_data_row, set deal_ids, is_automation).
    is_automation = a aba ja tem a coluna tecnica 'deal_id' no header (foi gerada por
    esta automacao). Se a aba existe e NAO e automation, e uma aba MANUAL/fechada
    (ex.: Junho_MATCH) — protegida contra escrita."""
    titles = {w.title for w in sh.worksheets()}
    if tab not in titles:
        return False, 1, set(), False
    end_col = rowcol_to_a1(1, tech_idx + 1).rstrip("1")
    vals = sh.values_get(f"'{tab}'!A1:{end_col}5000",
                         params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])
    header = vals[0] if vals else []
    is_auto = len(header) > tech_idx and str(header[tech_idx]).strip() == "deal_id"
    last_row = 1
    ids = set()
    for n, row in enumerate(vals[1:], start=2):
        ar = (row + [""] * (tech_idx + 1))
        if not any(str(c).strip() for c in ar[:n_cols]):
            continue
        last_row = n
        did = str(ar[tech_idx]).strip()
        if did:
            ids.add(did)
    return True, last_row, ids, is_auto


# ===================================================
# FRENTE B — {Mes}_MATCH
# ===================================================

MATCH_TEMPLATE = "Junho_MATCH"
N_MATCH_TEMPLATE = 13                 # A-M (Cliente..Rafaela)
MATCH_EXTRA = ["Nome do proponente", "Telefone do proponente", "E-mail do proponente", "deal_id"]
MATCH_TECH_IDX = 16                   # Q (13 + 3 contato)
# indices 0-based das colunas AUTO no MATCH
MCOL = {"cliente": 0, "fonte": 1, "proponente": 2, "interno": 3, "projeto": 4,
        "numero": 5, "valor": 6, "data": 7,
        "contato_nome": 13, "contato_tel": 14, "contato_email": 15}


def build_match_row(r):
    """Linha A-Q do {Mes}_MATCH. A-H + D auto; I-M (comissoes) em branco;
    N/O/P contato; Q deal_id."""
    out = [""] * (MATCH_TECH_IDX + 1)  # A..Q
    out[MCOL["cliente"]] = r["cliente"]
    out[MCOL["fonte"]] = map_lei(r["lei_principal"])
    out[MCOL["proponente"]] = r["proponente"]
    out[MCOL["interno"]] = r["interno_externo"]
    out[MCOL["projeto"]] = r["nome_projeto"]
    out[MCOL["numero"]] = r["numero_projeto"]
    v = parse_brl(r["valor_bruto"])
    out[MCOL["valor"]] = v if v is not None else ""
    d = parse_closedate(r["closedate"])
    out[MCOL["data"]] = fmt_date_br(d) if d else ""
    out[MCOL["contato_nome"]] = r.get("nome_contato_proponente", "")
    out[MCOL["contato_tel"]] = r.get("telefone_proponente", "")
    out[MCOL["contato_email"]] = r.get("email_proponente", "")
    out[MATCH_TECH_IDX] = str(r["deal_id"]).strip()
    return out


def run_match_mes(sh, cycle, inc, write, tab=None, hidden=False):
    mes = cycle_to_mes(cycle)
    tab = tab or f"{mes}_MATCH"
    cands = select_cycle(inc, cycle)
    exists, last_row, existing_ids, is_auto = read_existing(sh, tab, N_MATCH_TEMPLATE, MATCH_TECH_IDX)
    protegida = exists and not is_auto and tab != ""  # aba manual/fechada (sem coluna tecnica)
    novos = [r for r in cands if str(r["deal_id"]).strip() not in existing_ids]

    print("=" * 78)
    print(f"FRENTE B — {tab} (Match Won do ciclo {cycle})")
    print(f"  aba existe: {'sim' if exists else 'nao (sera criada do template ' + MATCH_TEMPLATE + ')'}")
    print(f"  Match no ciclo: {len(cands)} | ja na aba: {len(cands) - len(novos)} | NOVOS: {len(novos)}")
    if protegida:
        print(f"  [PROTECAO] '{tab}' existe SEM coluna tecnica deal_id = aba MANUAL/FECHADA. Write bloqueado.")
    print("-" * 78)
    for r in novos:
        v = parse_brl(r["valor_bruto"])
        print(f"  + {str(r['cliente'])[:34]:34} | {r['interno_externo']:8} | R$ {v or 0:>12,.2f}"
              f" | {fmt_date_br(r.get('_date'))} | deal {r['deal_id']}")
    if not novos:
        print("  (nenhum Match novo neste ciclo)")
    print()

    if not write:
        return
    if protegida:
        print(f"[write] {tab}: BLOQUEADO (aba manual/fechada, ex. Junho). Nada gravado.")
        return
    if not novos:
        print(f"[write] {tab}: 0 novos — nada a gravar.")
        return
    ws, criada = ensure_month_tab(sh, MATCH_TEMPLATE, tab, MATCH_EXTRA, N_MATCH_TEMPLATE)
    if criada:
        last_row = 1
    rows_out = [build_match_row(r) for r in novos]
    start = last_row + 1
    end = start + len(rows_out) - 1
    rng = f"A{start}:{rowcol_to_a1(1, MATCH_TECH_IDX + 1).rstrip('1')}{end}"
    ws.update(values=rows_out, range_name=rng, value_input_option="USER_ENTERED")
    print(f"[write] {tab}: {len(rows_out)} linha(s) em {rng} "
          f"({'aba criada' if criada else 'append'}). Comissoes I-M em branco; contato/deal_id ocultos.")
    if hidden and criada:
        hide_sheet(sh, ws)
        print(f"[hidden] aba '{tab}' OCULTA pra validacao (reexibir/rodar sem --hidden no dia 20).")


# ===================================================
# HUBSPOT (Frentes C e D — Elaboracao)
# ===================================================

import os
import requests
from sync import BASE  # "https://api.hubapi.com"

PIPELINE_PROPONENTE = "839644419"
STAGES_GANHO_PROP = ["1246571362", "1246571363", "1253441207"]  # Fechado/Ganho + 2 pos-venda
PRODUTOS_ELABORACAO = ["Elaboração", "Prestação de Contas", "Customização"]  # Bruno 02/07
ELAB_PROPS = ["dealname", "nome_do_proponente", "closedate", "produto", "condicao_de_pagamento",
              "valor_do_aporte", "valor_vendido", "lei_principal", "numero_do_projeto"]


def load_hubspot_token():
    tok = os.environ.get("HUBSPOT_TOKEN", "").strip()
    if tok:
        return tok
    for path in [os.path.expanduser("~/.brada-secrets/hubspot.env"),
                 r"C:\Users\bruno\.brada-secrets\hubspot.env"]:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("HUBSPOT_TOKEN"):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("[abort] HUBSPOT_TOKEN nao encontrado (env nem ~/.brada-secrets/hubspot.env).")


def search_elaboracao_won(token):
    """Todos os deals do pipeline Proponente ganhos com produto de elaboracao.
    Filtro de data e feito no Python (volume pequeno)."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    filters = [
        {"propertyName": "pipeline", "operator": "EQ", "value": PIPELINE_PROPONENTE},
        {"propertyName": "dealstage", "operator": "IN", "values": STAGES_GANHO_PROP},
        {"propertyName": "produto", "operator": "IN", "values": PRODUTOS_ELABORACAO},
    ]
    out, after = [], None
    while True:
        payload = {"filterGroups": [{"filters": filters}], "properties": ELAB_PROPS, "limit": 100,
                   "sorts": [{"propertyName": "closedate", "direction": "DESCENDING"}]}
        if after:
            payload["after"] = after
        resp = requests.post(f"{BASE}/crm/v3/objects/deals/search", headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            raise SystemExit(f"[abort] HubSpot search {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        out.extend(data.get("results", []))
        after = (data.get("paging") or {}).get("next", {}).get("after")
        if not after:
            break
    return out


def _produto_cru(v):
    return str(v or "").strip()


def _proponente(p):
    return (p.get("nome_do_proponente") or p.get("dealname") or "").strip()


# ---- Frente C: {Mes}_Elaboracao de Projetos ----
ELAB_TEMPLATE = "Junho_Elaboração de Projetos"
N_ELAB_TEMPLATE = 12           # A-L
ELAB_EXTRA = ["deal_id"]
ELAB_TECH_IDX = 12             # M


def build_elaboracao_row(d):
    """A-F auto (C=produto cru). G Data de pagamento = B Data do fechamento e
    H Valor pago = E Valor (convencao da modelo, uniformizada com a tabela do
    Ricardo — Bruno 02/07). I-L manuais em branco + M deal_id."""
    p = d["properties"]
    out = [""] * (ELAB_TECH_IDX + 1)  # A-M
    out[0] = _proponente(p)
    cd = parse_closedate(p.get("closedate"))
    out[1] = fmt_date_br(cd) if cd else ""
    out[2] = _produto_cru(p.get("produto"))
    out[3] = (p.get("condicao_de_pagamento") or "").strip()
    v = parse_brl(p.get("valor_do_aporte"))
    out[4] = v if v is not None else ""
    out[5] = (p.get("lei_principal") or "").strip()
    out[6] = out[1]   # G Data de pagamento = B Data do fechamento
    out[7] = out[4]   # H Valor pago = E Valor
    out[ELAB_TECH_IDX] = d["id"]
    return out


def _deals_no_ciclo(deals, cycle):
    start, end = cycle_window(cycle)
    out = []
    for d in deals:
        cd = parse_closedate(d["properties"].get("closedate"))
        if cd and start <= cd <= end:
            out.append(d)
    return out


def run_elaboracao_mes(sh, cycle, deals, write, tab=None, hidden=False):
    mes = cycle_to_mes(cycle)
    tab = tab or f"{mes}_Elaboração de Projetos"
    cands = _deals_no_ciclo(deals, cycle)
    exists, last_row, existing_ids, is_auto = read_existing(sh, tab, 6, ELAB_TECH_IDX)
    protegida = exists and not is_auto
    novos = [d for d in cands if d["id"] not in existing_ids]

    print("=" * 78)
    print(f"FRENTE C — {tab} (Proponente ganho no ciclo {cycle})")
    print(f"  aba existe: {'sim' if exists else 'nao (criada do template ' + ELAB_TEMPLATE + ')'}")
    print(f"  deals no ciclo: {len(cands)} | ja na aba: {len(cands) - len(novos)} | NOVOS: {len(novos)}")
    if protegida:
        print(f"  [PROTECAO] '{tab}' existe SEM coluna tecnica deal_id = aba MANUAL/FECHADA. Write bloqueado.")
    print("-" * 78)
    for d in novos:
        p = d["properties"]
        print(f"  + {_proponente(p)[:34]:34} | {_produto_cru(p.get('produto')):20} "
              f"| {fmt_date_br(parse_closedate(p.get('closedate')))} | deal {d['id']}")
    if not novos:
        print("  (nenhum deal Elaboracao novo neste ciclo)")
    print()
    if not write:
        return
    if protegida:
        print(f"[write] {tab}: BLOQUEADO (aba manual/fechada). Nada gravado.")
        return
    if not novos:
        print(f"[write] {tab}: 0 novos — nada a gravar.")
        return
    ws, criada = ensure_month_tab(sh, ELAB_TEMPLATE, tab, ELAB_EXTRA, N_ELAB_TEMPLATE)
    if criada:
        last_row = 1
    rows_out = [build_elaboracao_row(d) for d in novos]
    start_row = last_row + 1
    end_row = start_row + len(rows_out) - 1
    rng = f"A{start_row}:{rowcol_to_a1(1, ELAB_TECH_IDX + 1).rstrip('1')}{end_row}"
    ws.update(values=rows_out, range_name=rng, value_input_option="USER_ENTERED")
    print(f"[write] {tab}: {len(rows_out)} linha(s) em {rng} "
          f"({'aba criada' if criada else 'append'}). I-L manuais em branco; deal_id oculto.")
    if hidden and criada:
        hide_sheet(sh, ws)
        print(f"[hidden] aba '{tab}' OCULTA pra validacao (reexibir/rodar sem --hidden no dia 20).")


# ---- Frente D: Tabela Elaboracao Won 2026 pro Ricardo ----
# Alvo: planilha "Vendas_26" (NAO a oficial de Comissoes), aba "Copia de Vendas
# 26_elaboracao" (Bruno 30/06). Layout da aba modelo "Vendas 26_elaboracao":
# titulo "VENDAS 26" na linha 1, header na linha 3 a partir da coluna C; +
# Link Hubspot + deal_id (oculto). Data de pagamento/Valor Pago = Ricardo.
RICARDO_SHEET_ID = "13timE4IsrBPR7PIoIdOeBOp_OFup-Wa1LY0RO-tvjrY"
RICARDO_TAB = "Cópia de Vendas 26_elaboração"
RIC_HDR_ROW = 3                 # header na linha 3
RIC_DATA_ROW0 = 4               # dados a partir da linha 4
RIC_COL0 = 2                    # dados comecam na coluna C (idx 2; A/B = margem)
RICARDO_HEADER = ["Nome do Proponente", "Data do fechamento", "Condição de Pagamento", "Valor",
                  "Lei da Submissão", "Data de pagamento", "Valor Pago", "Link Hubspot", "deal_id"]
RIC_DEALID_IDX = RIC_COL0 + len(RICARDO_HEADER) - 1   # coluna do deal_id (oculta)


def build_ricardo_row(d):
    """Linha (a partir da coluna C): Nome, Data fechamento, Condicao, Valor, Lei,
    Data de pagamento, Valor Pago, Link Hubspot, deal_id.
    Convencao da aba modelo 'Vendas 26_elaboracao' (Bruno 30/06): Data de
    pagamento = Data do fechamento e Valor Pago = Valor (auto-preenchidos)."""
    p = d["properties"]
    cd = parse_closedate(p.get("closedate"))
    v = parse_brl(p.get("valor_do_aporte"))
    data_br = fmt_date_br(cd) if cd else ""
    valor = v if v is not None else ""
    return [
        _proponente(p),
        data_br,
        (p.get("condicao_de_pagamento") or "").strip(),
        valor,
        (p.get("lei_principal") or "").strip(),
        data_br,   # Data de pagamento = Data do fechamento (modelo)
        valor,     # Valor Pago = Valor (modelo)
        f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{d['id']}",
        d["id"],
    ]


def run_ricardo(gc, deals, write, rebuild=False):
    sh = gc.open_by_key(RICARDO_SHEET_ID)
    tab = RICARDO_TAB
    if rebuild and write:
        # uniformiza: limpa os dados (mantem titulo + header) e re-popula tudo.
        # Seguro enquanto o Ricardo ainda nao preencheu nada manualmente.
        ws0 = sh.worksheet(tab)
        last_col = rowcol_to_a1(1, RIC_DEALID_IDX + 1).rstrip("1")
        ws0.batch_clear([f"C{RIC_DATA_ROW0}:{last_col}3000"])
        print(f"[rebuild] dados de '{tab}' (C{RIC_DATA_ROW0}:{last_col}) limpos; re-populando do zero.")
    cands = []
    for d in deals:
        cd = parse_closedate(d["properties"].get("closedate"))
        if cd and cd.year >= 2026:
            d["_date"] = cd
            cands.append(d)
    cands.sort(key=lambda d: d["_date"], reverse=True)

    vals = sh.values_get(f"'{tab}'!A1:AZ3000", params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])

    def cell(row, i):
        return (row + [""] * (RIC_DEALID_IDX + 1))[i]

    has_header = (len(vals) >= RIC_HDR_ROW
                  and str(cell(vals[RIC_HDR_ROW - 1], RIC_COL0)).strip() == "Nome do Proponente")
    existing_ids = set()
    last_data_row = RIC_HDR_ROW
    for n, row in enumerate(vals, start=1):
        if n < RIC_DATA_ROW0:
            continue
        nome = str(cell(row, RIC_COL0)).strip()
        did = str(cell(row, RIC_DEALID_IDX)).strip()
        if nome or did:
            last_data_row = n
            if did:
                existing_ids.add(did)
    novos = [d for d in cands if d["id"] not in existing_ids]

    print("=" * 78)
    print(f"FRENTE D — '{tab}' (Planilha Vendas_26) | Elaboracao ganho >= 2026")
    print(f"  header presente: {'sim' if has_header else 'nao (sera escrito titulo + header)'}")
    print(f"  deals 2026: {len(cands)} | ja na aba: {len(cands) - len(novos)} | NOVOS: {len(novos)}")
    print("-" * 78)
    for d in novos[:30]:
        p = d["properties"]
        print(f"  + {_proponente(p)[:36]:36} | {fmt_date_br(d['_date'])} | deal {d['id']}")
    if not novos:
        print("  (nada novo)")
    print()
    if not write:
        return
    ws = sh.worksheet(tab)
    if not has_header:
        ws.update(values=[["VENDAS 26"]], range_name=rowcol_to_a1(1, RIC_COL0 + 1),
                  value_input_option="USER_ENTERED")
        c0 = rowcol_to_a1(RIC_HDR_ROW, RIC_COL0 + 1)
        c1 = rowcol_to_a1(RIC_HDR_ROW, RIC_COL0 + len(RICARDO_HEADER))
        ws.update(values=[RICARDO_HEADER], range_name=f"{c0}:{c1}", value_input_option="USER_ENTERED")
        hide_columns(sh, ws, RIC_DEALID_IDX, 1)
        last_data_row = RIC_HDR_ROW
    if not novos:
        print(f"[write] {tab}: 0 novos — nada a gravar.")
        return
    rows_out = [build_ricardo_row(d) for d in novos]
    start_row = last_data_row + 1
    end_row = start_row + len(rows_out) - 1
    c0 = rowcol_to_a1(start_row, RIC_COL0 + 1)
    c1 = rowcol_to_a1(end_row, RIC_COL0 + len(RICARDO_HEADER))
    ws.update(values=rows_out, range_name=f"{c0}:{c1}", value_input_option="USER_ENTERED")
    print(f"[write] {tab}: {len(rows_out)} linha(s) em {c0}:{c1}. Link Hubspot por deal; deal_id oculto; "
          "Data de pagamento/Valor Pago pro Ricardo.")


# ===================================================
# MAIN
# ===================================================

def main():
    utf8_stdout()
    ap = argparse.ArgumentParser(description="Geracao das abas mensais da planilha oficial do Ivan")
    ap.add_argument("--write", action="store_true", help="grava (default: dry-run)")
    ap.add_argument("--sheet-id", default=OFICIAL_ID_DEFAULT)
    ap.add_argument("--cycle", default=None, help="ciclo YYYY-MM (default: corrente)")
    ap.add_argument("--match-mes", action="store_true", help="gerar/popular {Mes}_MATCH")
    ap.add_argument("--elaboracao-mes", action="store_true", help="gerar/popular {Mes}_Elaboracao (Frente C)")
    ap.add_argument("--ricardo", action="store_true", help="tabela Elaboracao Won 2026 (Frente D)")
    ap.add_argument("--tab", default=None, help="override do nome da aba (sandbox/teste)")
    ap.add_argument("--rebuild", action="store_true",
                    help="Frente D: limpa os dados da tabela e re-popula do zero (uniformiza)")
    ap.add_argument("--hidden", action="store_true",
                    help="oculta as abas do mes recem-criadas (validacao antes de publicar dia 20)")
    args = ap.parse_args()

    cycle = args.cycle or current_cycle()
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", cycle):
        raise SystemExit(f"--cycle invalido: {cycle!r} (esperado YYYY-MM)")

    none_flag = not (args.match_mes or args.elaboracao_mes or args.ricardo)
    do_match = args.match_mes or none_flag
    do_elab = args.elaboracao_mes or none_flag
    do_ric = args.ricardo or none_flag

    gc = get_sheets_client()
    sh = gc.open_by_key(args.sheet_id)

    if do_match:
        rows, fonte_ts = load_consolidado(gc)
        if len(rows) < MIN_ROWS_GUARD:
            raise SystemExit(f"[abort] consolidado com {len(rows)} linhas (< {MIN_ROWS_GUARD}).")
        inc, _ = split_vendas(rows)
        print(f"consolidado fonte_ts={fonte_ts} | Match Won total={len(inc)}\n")
        run_match_mes(sh, cycle, inc, args.write, tab=args.tab, hidden=args.hidden)

    if do_elab or do_ric:
        token = load_hubspot_token()
        deals = search_elaboracao_won(token)
        print(f"\nHubSpot: {len(deals)} deals Proponente ganho (produtos {PRODUTOS_ELABORACAO})\n")
        if do_elab:
            run_elaboracao_mes(sh, cycle, deals, args.write, tab=args.tab, hidden=args.hidden)
        if do_ric:
            run_ricardo(gc, deals, args.write, rebuild=args.rebuild)

    if not args.write:
        print("[dry-run] nada gravado. Use --write quando aprovado.")


if __name__ == "__main__":
    main()
