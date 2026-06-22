"""
Financeiro ELABORAÇÃO — Ivan (S-A2, sessão 22-25/06/2026).

Popula a aba `Cópia de Junho_Elaboração de Projetos` (gid 1225729992) da
planilha OFICIAL do Ivan "Comissões 2026" (1XVRuIMN9...) com as colunas A-G a
partir do consolidado da Brada_Dashboard_Deals. H+ é território da Luciana
(fórmulas: Líquido pago = G*(1-12%); Ivan/Ricardo = IF(F<>""; I*5%; ...)) —
NÃO TOCAR.

Decisões 22/06 (ata_ivan_22jun + sessão S-A2):
  - Source-of-truth do conjunto = manual (`Junho_Elaboração de Projetos`,
    gid 1005722547). Itera linha a linha e popula com o que existe no HubSpot.
  - Filtro estágio = "Fechado / Ganho" estrito (id 1246571362, ord 6, prob 1.0,
    isClosed=true). NÃO inclui pós-venda.
  - Col G = `closedate` (Data de fechamento do card) — regra estabelecida na S-A.
  - Tiebreak na reconciliação: (valor + nome substring) > lei > closedate.
  - Override anti-drift via HubSpot API pros 5 campos críticos: closedate,
    lei_principal, data_do_aporte, valor_do_aporte, condicao_de_pagamento.

Uso:
  python ops/financeiro_elaboracao_ivan.py                     # dry-run
  python ops/financeiro_elaboracao_ivan.py --write              # escreve A:G
  python ops/financeiro_elaboracao_ivan.py --write --allow-partial
"""

import argparse
import csv
import datetime as dt
import io
import os
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

import gspread
import requests

# Padrão de ops/: sobe 1 nível pra achar sync.py na raiz.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sync import PORTAL_ID, get_sheets_client  # noqa: E402

# Reuso do filhote MATCH (S-A, mesma sessão). Quando virar 3+ filhotes,
# extrair pra `lib/financeiro_consolidado.py` (decisão do plano S-A2).
from ops.financeiro_match_ivan import (  # noqa: E402
    CONSOLIDADO_HEADER,
    CONSOLIDADO_WS,
    HUBSPOT_TOKEN,
    MIN_ROWS_GUARD,
    SHEETS_EPOCH,
    SOURCE_SPREADSHEET_ID,
    fetch_deal_props,
    fmt_brl,
    fmt_date_br,
    hubspot_deal_url,
    load_consolidado,
    norm,
    parse_brl,
    parse_closedate,
    print_table,
    section,
    serial_to_date,
    to_csv,
)


# ===========================================================
# CONFIG da S-A2
# ===========================================================

IVAN_SHEET_ID = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"  # Comissões 2026
WRITE_TAB = "Cópia de Junho_Elaboração de Projetos"  # gid 1225729992, alvo --write
REF_TAB   = "Junho_Elaboração de Projetos"            # gid 1005722547, manual

EXPECTED_HEADER_A_G = [
    "Nome do Proponente", "Data do fechamento", "Condição de Pagamento",
    "Valor", "Lei da Submissão", "Data de pagamento", "Valor Pago",
]

DEFAULT_CYCLE_START = date(2026, 5, 21)
DEFAULT_CYCLE_END   = date(2026, 6, 20)

# Pipeline Proponente — stages pós-ganho (decisão Bruno revisada na S-A2):
# considera Fechado/Ganho + Acompanhamento (pós-venda inicial) + Ganho-em-entrega
# como "negócio fechado de fato". Filtro de mês = closedate ∈ ciclo.
PROPONENTE_GANHO_STAGE_ID = "1246571362"
PROPONENTE_GANHO_STAGES_LABELS = {
    "Fechado / Ganho",
    "Acompanhamento / Pós-venda inicial",
    "Ganho (pós-venda em entrega)",
}

# Mapa lei_principal (picklist HubSpot) -> Lei da Submissão (rótulo da Luciana).
# Diferente do LEI_FONTE_MAP do MATCH (que mapeia pro rótulo do Incentivador).
# Calibrado a partir de Maio+Junho_Elaboração (Passo 0.c).
LEI_FONTE_MAP_ELAB = {
    "Rouanet":           "Lei Rouanet",
    "Esporte Federal":   "Lei Federal do Esporte",
    "Esporte Estadual":  "Lei Estadual do Esporte",
    "Cultura Estadual":  "Lei Estadual de Cultura",
    "Cultura Municipal": "Lei ISS RJ",
    "Audiovisual":       "Lei Audiovisual",   # picklist HubSpot ainda não tem; aguarda Bruno adicionar
    "(sem lei preenchida)": "",
}


def map_lei_elab(lei):
    return LEI_FONTE_MAP_ELAB.get(lei, lei)


def is_name_match(m_name, deal):
    """Fuzzy match entre nome da manual e nome do deal (cliente OR proponente).
    Estratégia: (1) substring direto OR (2) ≥66% dos tokens significativos
    (≥4 chars) da manual presentes no deal. Cobre casos onde Ricardo renomeou
    (ex.: manual='Associação Missão Intensidade', deal='Escola de Dança Missao
    Intensidade')."""
    if not m_name:
        return False
    deal_name = (norm(deal.get("cliente", "")) + " "
                 + norm(deal.get("proponente", "")) + " "
                 + norm(deal.get("nome_do_proponente", "") or ""))
    if m_name in deal_name:
        return True
    tokens = [t for t in m_name.split() if len(t) >= 4]
    if not tokens:
        return False
    hits = sum(1 for t in tokens if t in deal_name)
    return hits / len(tokens) >= 0.66


# ===========================================================
# LEITURA da planilha do Ivan (parametrizada pelas tabs da Elaboração)
# ===========================================================

def open_ivan_sheet(gc):
    return gc.open_by_key(IVAN_SHEET_ID)


def read_write_tab_snapshot(sh):
    ws = sh.worksheet(WRITE_TAB)
    vals = ws.get("A:G", value_render_option="UNFORMATTED_VALUE")
    if not vals:
        raise SystemExit(f"Aba '{WRITE_TAB}' vazia.")
    header = vals[0]
    rows = vals[1:] if len(vals) > 1 else []
    return ws, header, rows


def read_formula_cells(sh, cells):
    ws = sh.worksheet(WRITE_TAB)
    out = {}
    for c in cells:
        try:
            v = ws.get(c, value_render_option="FORMULA")
            out[c] = v[0][0] if v and v[0] else ""
        except Exception as e:
            out[c] = f"<erro: {type(e).__name__}: {e}>"
    return out


def read_junho_elaboracao_manual(sh):
    """Le REF_TAB (manual, read-only). Junho_Elaboração tem A-K (Luciana
    estendeu); a gente só pega A-G + skip 'Total'."""
    try:
        ws = sh.worksheet(REF_TAB)
    except gspread.exceptions.WorksheetNotFound:
        raise SystemExit(f"Aba '{REF_TAB}' não encontrada na planilha do Ivan.")
    vals = ws.get("A:G", value_render_option="UNFORMATTED_VALUE")
    if not vals:
        return []
    out = []
    for raw in vals[1:]:
        row = list(raw) + [""] * (7 - len(raw))
        nome = str(row[0] or "").strip()
        data_fech_raw = row[1]
        cond_pag = str(row[2] or "").strip()
        valor_raw = row[3]
        lei = str(row[4] or "").strip()
        data_pag_raw = row[5]
        valor_pago_raw = row[6]

        if not nome and not valor_raw:
            continue
        if norm(nome) in {"total", "totais", "soma"}:
            continue

        def parse_date_cell(v):
            if isinstance(v, (int, float)) and v:
                return serial_to_date(v)
            s = str(v or "").strip()
            if not s:
                return None
            for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s, fmt).date()
                except ValueError:
                    pass
            return None

        def parse_num_cell(v):
            if isinstance(v, (int, float)):
                return float(v)
            return parse_brl(v) or 0.0

        out.append({
            "nome":      nome,
            "data_fech": parse_date_cell(data_fech_raw),
            "cond_pag":  cond_pag,
            "valor":     parse_num_cell(valor_raw),
            "lei":       lei,
            "data_pag":  parse_date_cell(data_pag_raw),
            "valor_pago": parse_num_cell(valor_pago_raw),
        })
    return out


# ===========================================================
# FILTRO do pool
# ===========================================================

def filter_elaboracao_pool(consolidado, cycle_start=None, cycle_end=None):
    """pipeline=Proponente AND produto=Elaboração AND stage ∈ pós-ganho
    AND valor >= 0 AND closedate ∈ ciclo (se cycle_start/end passado).

    Decisão Bruno revisada na S-A2 (22/06): considera Fechado/Ganho +
    Acompanhamento + Ganho-em-entrega como "negócio fechado de fato". Mês
    = closedate."""
    pool = []
    seen = {}
    dups = []
    for r in consolidado:
        if r.get("pipeline") != "Proponente":
            continue
        if r.get("produto") != "Elaboração":
            continue
        if (r.get("stage") or "").strip() not in PROPONENTE_GANHO_STAGES_LABELS:
            continue
        v = parse_brl(r.get("valor_bruto"))
        if v is None or v < 0:
            continue
        if cycle_start and cycle_end:
            d = parse_closedate(r.get("closedate", ""))
            if not d or not (cycle_start <= d <= cycle_end):
                continue
        did = (r.get("deal_id") or "").strip()
        if did in seen:
            dups.append(did)
            continue
        seen[did] = r
        pool.append(r)
    if dups:
        raise SystemExit(f"Hard-fail: deal_id duplicado no pool Elaboração: {dups}")
    return pool


def fetch_overrides(deal_ids):
    """Refresh dos 5 campos críticos via HubSpot API."""
    props = ["closedate", "lei_principal", "data_do_aporte", "valor_do_aporte", "condicao_de_pagamento"]
    return fetch_deal_props(deal_ids, props)


# Stages map pra resolver dealstage_id → label local (sem chamar API toda hora)
PROPONENTE_STAGE_ID_TO_LABEL = {
    "1246571362": "Fechado / Ganho",
    "1246571363": "Acompanhamento / Pós-venda inicial",
    "1253441207": "Ganho (pós-venda em entrega)",
}


def fetch_elaboracao_pool_fresh(cycle_start, cycle_end):
    """Busca pool Proponente+Elaboração+pós-ganho+ciclo direto do HubSpot
    (NÃO do consolidado — evita lag do sync horário, útil após PATCH/CREATE
    recente). Retorna dicts no mesmo formato do consolidado pra reuso do
    resto do pipeline."""
    if not HUBSPOT_TOKEN:
        raise SystemExit("HUBSPOT_TOKEN ausente")
    H = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
    stage_ids = list(PROPONENTE_STAGE_ID_TO_LABEL.keys())
    cs_ms = int(datetime.combine(cycle_start, datetime.min.time()).timestamp() * 1000)
    ce_ms = int(datetime.combine(cycle_end, datetime.max.time()).timestamp() * 1000)
    props = ["dealname", "dealstage", "pipeline", "closedate", "valor_do_aporte",
             "lei_principal", "nome_do_proponente", "produto", "data_do_aporte",
             "condicao_de_pagamento", "tipo_de_proponente"]
    out = []
    after = None
    while True:
        body = {
            "filterGroups": [{"filters": [
                {"propertyName": "pipeline",  "operator": "EQ",  "value": "839644419"},
                {"propertyName": "produto",   "operator": "EQ",  "value": "Elaboração"},
                {"propertyName": "dealstage", "operator": "IN",  "values": stage_ids},
                {"propertyName": "closedate", "operator": "GTE", "value": cs_ms},
                {"propertyName": "closedate", "operator": "LTE", "value": ce_ms},
            ]}],
            "properties": props,
            "limit": 100,
        }
        if after:
            body["after"] = after
        r = requests.post("https://api.hubapi.com/crm/v3/objects/deals/search",
                          headers=H, json=body, timeout=30)
        data = r.json()
        for d in data.get("results", []):
            p = d.get("properties", {})
            out.append({
                "deal_id":      d["id"],
                "pipeline":     "Proponente",
                "produto":      p.get("produto") or "",
                "stage":        PROPONENTE_STAGE_ID_TO_LABEL.get(p.get("dealstage") or "", "?"),
                "cliente":      p.get("dealname") or "",
                "proponente":   p.get("nome_do_proponente") or "",
                "nome_do_proponente": p.get("nome_do_proponente") or "",
                "closedate":    p.get("closedate") or "",
                "lei_principal": p.get("lei_principal") or "",
                "data_aporte":  p.get("data_do_aporte") or "",
                "valor_bruto":  p.get("valor_do_aporte") or "0",
                "tipo_de_proponente": p.get("tipo_de_proponente") or "",
                "condicao_de_pagamento": p.get("condicao_de_pagamento") or "",
                "nome_projeto": "",
                "numero_projeto": "",
                "convertido":   "1",
                "won_ganho":    "1",
                "fluxo_comissao": "Elaboração",
            })
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return out


# ===========================================================
# RECONCILIAÇÃO com tiebreak lei -> closedate
# ===========================================================

def reconcile_with_tiebreak(manual, pool):
    """Pra cada linha da manual, acha o deal correspondente no pool com
    cascata de chaves: (valor, nome substring) > tiebreak lei > tiebreak data.
    Retorna {match_strong, match_value_only, match_ambiguous,
             unmatched_manual, unmatched_ours}."""
    pool_by_value = defaultdict(list)
    for r in pool:
        v = round(parse_brl(r.get("valor_bruto")) or 0, 2)
        pool_by_value[v].append(r)

    out = {
        "match_strong":     [],
        "match_value_only": [],
        "match_ambiguous":  [],
        "unmatched_manual": [],
        "unmatched_ours":   [],
    }
    matched_ours_ids = set()

    for m in manual:
        v_key = round(float(m.get("valor") or 0), 2)
        cands = pool_by_value.get(v_key, [])
        m_name = norm(m.get("nome", ""))
        contains = [r for r in cands if is_name_match(m_name, r)]

        # cascata
        if len(contains) == 1:
            o = contains[0]
            out["match_strong"].append({"manual": m, "ours": o})
            matched_ours_ids.add(o["deal_id"])
            continue
        if len(contains) > 1:
            # tiebreak lei
            m_lei = norm(m.get("lei", ""))
            by_lei = [r for r in contains if m_lei and
                      m_lei == norm(map_lei_elab(r.get("lei_principal", "")))]
            if len(by_lei) == 1:
                out["match_strong"].append({"manual": m, "ours": by_lei[0]})
                matched_ours_ids.add(by_lei[0]["deal_id"])
                continue
            # tiebreak data
            m_date = m.get("data_fech")
            by_date = [r for r in contains
                       if m_date == parse_closedate(r.get("closedate", ""))]
            if len(by_date) == 1:
                out["match_strong"].append({"manual": m, "ours": by_date[0]})
                matched_ours_ids.add(by_date[0]["deal_id"])
                continue
            # ambíguo de fato
            out["match_ambiguous"].append({"manual": m, "ours": contains})
            for r in contains:
                matched_ours_ids.add(r["deal_id"])
            continue

        # 0 substring matches; checa se valor casa com algum (value_only)
        if len(cands) == 1:
            out["match_value_only"].append({"manual": m, "ours": cands[0]})
            matched_ours_ids.add(cands[0]["deal_id"])
        elif len(cands) > 1:
            out["match_ambiguous"].append({"manual": m, "ours": cands})
            for r in cands:
                matched_ours_ids.add(r["deal_id"])
        else:
            out["unmatched_manual"].append(m)

    for o in pool:
        if o["deal_id"] not in matched_ours_ids:
            out["unmatched_ours"].append(o)

    return out


# ===========================================================
# BUILD A-G a partir da MANUAL
# ===========================================================

def build_rows_from_pool(pool):
    """Pra cada deal do pool (closedate ∈ ciclo), monta linha A-G usando os
    campos HubSpot diretamente. Regra Bruno (S-A2 22/06): pool fresh do
    HubSpot é source-of-truth, não a manual."""
    items = []
    for r in pool:
        flags = []
        d_close = parse_closedate(r.get("closedate", ""))
        d_aporte = parse_closedate(r.get("data_aporte", ""))
        valor_aporte = parse_brl(r.get("valor_bruto")) or 0.0
        lei = r.get("lei_principal", "")
        cond = r.get("condicao_de_pagamento", "")

        if not lei or lei == "(sem lei preenchida)":
            flags.append("lei_principal vazia no HubSpot — col E sai vazia")
        if not cond:
            flags.append("condicao_de_pagamento vazia no HubSpot — col C sai vazia")

        if d_aporte and valor_aporte > 0:
            g_value = round(valor_aporte, 2)
        else:
            g_value = ""

        row_a_g = [
            r.get("nome_do_proponente") or r.get("cliente", ""),     # A
            fmt_date_br(d_close),                                     # B
            cond,                                                      # C
            round(valor_aporte, 2) if valor_aporte > 0 else "",        # D
            map_lei_elab(lei) if lei and lei != "(sem lei preenchida)" else "",  # E
            fmt_date_br(d_aporte),                                     # F
            g_value,                                                   # G
        ]
        items.append({
            "deal": r, "row_a_g": row_a_g, "flags": flags,
            "url": hubspot_deal_url(r["deal_id"]),
        })
    # Sort por closedate asc
    items.sort(key=lambda it: parse_closedate(it["deal"].get("closedate", "")) or date.min)
    return items


def build_rows_from_manual(manual, pool):
    """Pra cada linha da manual, monta dict {manual, our, row_a_g, flags, url}.
    Track deal_ids alocados pra impedir 2 linhas manual apontarem pro MESMO deal."""
    pool_by_value = defaultdict(list)
    for r in pool:
        v = round(parse_brl(r.get("valor_bruto")) or 0, 2)
        pool_by_value[v].append(r)

    items = []
    allocated = set()
    for m in manual:
        v_key = round(float(m.get("valor") or 0), 2)
        cands = [r for r in pool_by_value.get(v_key, []) if r["deal_id"] not in allocated]
        m_name = norm(m.get("nome", ""))
        contains = [r for r in cands if is_name_match(m_name, r)]

        # cascata lookup
        our = None
        if len(contains) == 1:
            our = contains[0]
        elif len(contains) > 1:
            m_lei = norm(m.get("lei", ""))
            by_lei = [r for r in contains if m_lei and
                      m_lei == norm(map_lei_elab(r.get("lei_principal", "")))]
            if len(by_lei) == 1:
                our = by_lei[0]
            else:
                m_date = m.get("data_fech")
                by_date = [r for r in contains
                           if m_date == parse_closedate(r.get("closedate", ""))]
                if len(by_date) == 1:
                    our = by_date[0]
                else:
                    our = max(contains, key=lambda r: len(norm(r.get("cliente", ""))))
        elif len(cands) == 1:
            our = cands[0]

        if our is not None:
            allocated.add(our["deal_id"])

        if our is None:
            items.append({
                "manual": m, "our": None, "row_a_g": None,
                "flags": [f"deal não encontrado em Proponente+Elaboração+Fechado/Ganho (nome='{m.get('nome','')[:40]}', valor={m.get('valor','?')})"],
                "url": "",
            })
            continue

        flags = []
        d_close = parse_closedate(our.get("closedate", ""))
        d_aporte = parse_closedate(our.get("data_aporte", ""))
        valor_aporte = parse_brl(our.get("valor_bruto")) or 0.0

        lei = our.get("lei_principal", "")
        if not lei or lei == "(sem lei preenchida)":
            flags.append(
                f"lei_principal vazia no HubSpot (manual indica '{m.get('lei','?')}'); col E sai vazia"
            )

        cond = our.get("condicao_de_pagamento", "")
        if not cond:
            flags.append(
                f"condicao_de_pagamento vazia no HubSpot (manual indica '{m.get('cond_pag','?')}'); col C sai vazia"
            )

        # G: valor_do_aporte SE data_do_aporte preenchido (pago); senão vazio
        if d_aporte and valor_aporte > 0:
            g_value = round(valor_aporte, 2)
        else:
            g_value = ""

        row_a_g = [
            our.get("nome_do_proponente") or our.get("cliente", ""),     # A
            fmt_date_br(d_close),                                         # B
            cond,                                                          # C
            round(valor_aporte, 2) if valor_aporte > 0 else "",            # D
            map_lei_elab(lei) if lei and lei != "(sem lei preenchida)" else "",  # E
            fmt_date_br(d_aporte),                                         # F
            g_value,                                                       # G
        ]
        items.append({
            "manual": m, "our": our, "row_a_g": row_a_g,
            "flags": flags, "url": hubspot_deal_url(our["deal_id"]),
        })
    return items


def print_message_pro_ivan(items):
    section("MENSAGEM PRO IVAN / RICARDO (Bruno copia daqui pra baixo)")
    print("Ivan, segue o status do financeiro Elaboração — Junho/2026.")
    print(f"Vou popular a `Cópia de Junho_Elaboração de Projetos` com os {len(items)} deals da manual.\n")
    n_ok = sum(1 for it in items if not it["flags"])
    n_no_deal = sum(1 for it in items if it["our"] is None)
    print(f"Resumo: {n_ok}/{len(items)} sem gaps · {n_no_deal} deals não encontrados no HubSpot\n")
    for i, it in enumerate(items, 1):
        m = it["manual"]
        nome = m.get("nome", "")
        val_str = fmt_brl(m.get("valor", 0)) if m.get("valor") else "(sem valor)"
        date_m = m["data_fech"].strftime("%d/%m/%Y") if m.get("data_fech") else "?"
        lei_m = m.get("lei", "")
        cond_m = m.get("cond_pag", "")
        print(f"### {i}. {nome} — {val_str} — {date_m} — {lei_m} — {cond_m}")
        if it["our"]:
            print(f"    {it['url']}")
        if it["flags"]:
            for f in it["flags"]:
                print(f"    - {f}")
        else:
            print("    - sem gaps")
        print()


# ===========================================================
# ESCRITA gated
# ===========================================================

def do_write(ws, rows_a_g, M):
    N = len(rows_a_g)
    end = max(M, N) + 1
    if end >= 2:
        ws.batch_clear([f"A2:G{end}"])
    if N > 0:
        ws.update(range_name=f"A2:G{N + 1}", values=rows_a_g, value_input_option="USER_ENTERED")
    return N


def gate_check(items, formula_alarm, header_ok, consolidado_size, n_lines, allow_partial=False):
    fails, warns = [], []
    if consolidado_size < MIN_ROWS_GUARD:
        fails.append(f"consolidado com {consolidado_size} linhas (< {MIN_ROWS_GUARD})")
    if not header_ok:
        fails.append("header da aba destino não é o esperado")
    if formula_alarm:
        fails.append(f"H2:I2 referencia '{REF_TAB}!' (cópia por fórmula)")
    if n_lines <= 0 or n_lines > 200:
        fails.append(f"N a escrever fora do range sane: {n_lines}")
    n_flags = sum(1 for it in items if it.get("flags"))
    if n_flags:
        warns.append(f"{n_flags}/{n_lines} linha(s) com flags (campos vazios no HubSpot)")
    return fails, warns


# ===========================================================
# MAIN
# ===========================================================

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser(description="Financeiro ELABORAÇÃO Ivan — populador A-G")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--cycle-start", type=lambda s: dt.date.fromisoformat(s), default=None)
    ap.add_argument("--cycle-end", type=lambda s: dt.date.fromisoformat(s), default=None)
    ap.add_argument("--allow-partial", action="store_true")
    args = ap.parse_args()

    cs = args.cycle_start or DEFAULT_CYCLE_START
    ce = args.cycle_end or DEFAULT_CYCLE_END

    gc = get_sheets_client()
    section("FINANCEIRO ELABORAÇÃO IVAN — S-A2 (v1, 2026-06-22)")
    print(f"fonte: {SOURCE_SPREADSHEET_ID}/{CONSOLIDADO_WS}")
    print(f"alvo:  {IVAN_SHEET_ID} aba '{WRITE_TAB}' (gid 1225729992)")
    print(f"manual: aba '{REF_TAB}' (gid 1005722547, read-only)")
    print(f"ciclo de referência: {cs:%d/%m/%Y} a {ce:%d/%m/%Y}")

    # 1) Pré-check + snapshot
    sh = open_ivan_sheet(gc)
    try:
        ws_write, header, current_rows = read_write_tab_snapshot(sh)
    except gspread.exceptions.APIError as e:
        raise SystemExit(f"Pré-check permission FAIL: {e}")
    header_stripped = [str(h).strip() for h in header[:7]]
    header_ok = header_stripped == EXPECTED_HEADER_A_G
    section("Pré-check (snapshot do estado atual)")
    print(f"header esperado: {EXPECTED_HEADER_A_G}")
    print(f"header real:     {header_stripped}")
    print(f"VÁLIDO: {header_ok}")
    M = len([r for r in current_rows if any(str(c).strip() for c in r)])
    print(f"M (linhas atualmente em A:G): {M}")
    if M > 0:
        section("Snapshot CSV do estado atual (A:G) — antes da escrita")
        snap = []
        for r in current_rows[:M]:
            row = list(r) + [""] * (7 - len(r))
            if isinstance(row[5], (int, float)):
                d = serial_to_date(row[5]); row[5] = d.isoformat() if d else row[5]
            if isinstance(row[1], (int, float)):
                d = serial_to_date(row[1]); row[1] = d.isoformat() if d else row[1]
            snap.append(row)
        print(to_csv(snap, EXPECTED_HEADER_A_G).strip())

    # 2) Fórmulas H2:I2 (alarme)
    section("Inspeção H2:I2 (alarme se referencia outra aba)")
    forms = read_formula_cells(sh, ["H2", "I2", "J2", "K2"])
    for c, v in forms.items():
        print(f"  {c}: {v!r}")
    formula_alarm = any(f"{REF_TAB}!" in str(v) for v in forms.values())
    if formula_alarm:
        print(f">>> ALARME: H+ referencia '{REF_TAB}!'. Pausar.")

    # 3) Manual
    manual = read_junho_elaboracao_manual(sh)
    section(f"Manual (Junho_Elaboração de Projetos): {len(manual)} linhas")
    if manual:
        rows_m = [
            [m["nome"][:35],
             m["data_fech"].strftime("%d/%m/%Y") if m["data_fech"] else "?",
             m["cond_pag"][:18],
             fmt_brl(m["valor"]) if m["valor"] else "",
             m["lei"][:22],
             m["data_pag"].strftime("%d/%m/%Y") if m["data_pag"] else "",
             fmt_brl(m["valor_pago"]) if m["valor_pago"] else "",
             ]
            for m in manual
        ]
        print_table(["nome", "data fech", "cond pag", "valor", "lei", "data pag", "valor pago"], rows_m)

    # 4) Pool direto do HubSpot (não do consolidado — evita lag do sync horário)
    rows, fonte_ts = load_consolidado(gc)  # só pra ter o MIN_ROWS_GUARD do consolidado
    print(f"\nconsolidado: {len(rows)} linhas | atualizado: {fonte_ts or '?'} (usado só pro guard)")
    pool = fetch_elaboracao_pool_fresh(cs, ce)
    print(f"pool Proponente+Elaboração+pós-ganho+ciclo (HubSpot live): {len(pool)} deals")
    n_cond = sum(1 for r in pool if r.get("condicao_de_pagamento"))
    n_lei  = sum(1 for r in pool if r.get("lei_principal"))
    n_data_aporte = sum(1 for r in pool if r.get("data_aporte"))
    print(f"fill: lei {n_lei}/{len(pool)} · condicao_pag {n_cond}/{len(pool)} · data_aporte {n_data_aporte}/{len(pool)}")

    # 5) Build a partir do pool fresh (regra Bruno: HubSpot = source-of-truth)
    section("Build A-G a partir do pool HubSpot")
    items = build_rows_from_pool(pool)

    section(f"Linhas A-G (total {len(items)})")
    headers_p = ["#", "A Proponente", "B Fech.", "C Cond.", "D Valor", "E Lei", "F Pag.", "G Pago", "flags"]
    rows_pp = []
    for i, it in enumerate(items, 1):
        row = it["row_a_g"]
        rows_pp.append([
            i,
            str(row[0])[:30],
            str(row[1])[:10],
            str(row[2])[:18],
            fmt_brl(row[3]) if isinstance(row[3], (int, float)) else "",
            str(row[4])[:22],
            str(row[5])[:10],
            fmt_brl(row[6]) if isinstance(row[6], (int, float)) else "",
            f"{len(it['flags'])}" if it["flags"] else "OK",
        ])
    print_table([h[:30] for h in headers_p], rows_pp, max_rows=30)

    # 6) Cross-check contra manual (INFORMATIVO; não bloqueia)
    section("Cross-check vs manual Junho_Elaboração de Projetos")
    pool_names = {norm(it["deal"].get("nome_do_proponente") or it["deal"].get("cliente", "")) for it in items}
    manual_names = {norm(m["nome"]): m for m in manual}
    only_in_manual = [m for n, m in manual_names.items()
                      if not any(n in pn or any(t in pn for t in n.split() if len(t) >= 5) for pn in pool_names)]
    if only_in_manual:
        print(f"{len(only_in_manual)} linha(s) da manual SEM correspondente no pool do ciclo (closedate fora de junho):")
        for m in only_in_manual:
            d = m["data_fech"].strftime("%d/%m/%Y") if m["data_fech"] else "?"
            print(f"  - {m['nome'][:50]} | {d} | {m['lei']}")
        print("  (esses deals entram na planilha do mês do closedate, não aqui.)")
    else:
        print("manual e pool batem 100% — nada da manual ficou de fora.")

    # 7) Cross-check (soma D escrita vs soma D manual — INFORMATIVO)
    soma_d = round(sum((it["row_a_g"][3] if isinstance(it["row_a_g"][3], (int, float)) else 0) for it in items), 2)
    soma_manual = round(sum(m.get("valor", 0) for m in manual), 2)
    section("Cross-check semântico (informativo)")
    print(f"soma col D (Valor) escrita pool:  {fmt_brl(soma_d)}")
    print(f"soma col D (Valor) manual junho:  {fmt_brl(soma_manual)}")
    print(f"delta:                            {fmt_brl(soma_d - soma_manual)}")
    print("(delta esperado: manual inclui deals de meses passados pendentes; pool tem só ciclo atual)")

    # 8) Decisão + gates
    section("DECISÃO FINAL")
    rows_a_g_final = [it["row_a_g"] for it in items if it["row_a_g"]]
    print(f"Linhas a escrever: {len(rows_a_g_final)} de {len(items)} da manual")
    print(f"Ciclo: {cs:%d/%m/%Y} a {ce:%d/%m/%Y}")
    print("G = closedate ; F = data_do_aporte ; G(Valor Pago) = valor_do_aporte SE F preenchido")

    section("Gates do --write")
    fails, warns = gate_check(items, formula_alarm, header_ok, len(rows), len(rows_a_g_final), allow_partial=args.allow_partial)
    if warns:
        for w in warns:
            print(f"   [warn] {w}")
    if fails:
        print(">>> BLOQUEADO. Razões:")
        for f in fails:
            print(f"   - {f}")
    else:
        print("OK — gates passam.")

    if not args.write:
        print("\n[dry-run] nenhuma escrita feita.")
        return
    if fails:
        raise SystemExit("\n--write ABORTADO: gates falharam.")

    section("ESCRITA (cirúrgica em A:G)")
    n_written = do_write(ws_write, rows_a_g_final, M)
    print(f"OK — {n_written} linha(s) escrita(s) em A2:G{n_written + 1}.")
    if M > n_written:
        print(f"[aviso] {M - n_written} linha(s) antiga(s) limpa(s) entre A{n_written + 2}:G{M + 1}.")
    print(f"\nLink: https://docs.google.com/spreadsheets/d/{IVAN_SHEET_ID}/edit#gid=1225729992")


if __name__ == "__main__":
    main()
