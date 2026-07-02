# -*- coding: utf-8 -*-
"""
Automacao da aba "Controle de Vendas" da planilha OFICIAL de Comissoes do Ivan
(1XVRuIMN...). Roda no dia 20 (fecho do ciclo) e faz APPEND incremental dos
Match Won do ciclo corrente que ainda nao estao na planilha.

Principios (folha de pagamento = cirurgico):
  - SO APPEND. Nunca clear, nunca sobrescreve linha existente.
  - Preenche apenas A,B,C,E,F,I,J,L (decisao Bruno 28/06). D,G,H,K,M-R ficam
    em branco (manuais do Ivan/Luciana).
  - Dry-run default; --write so grava apos gate manual.
  - Dedup por deal_id (coluna tecnica oculta) + heuristica de valor pras linhas
    legadas (sem deal_id). O lado seguro e NAO duplicar: candidato com valor ja
    presente entra como "provavel duplicata" e NAO e gravado sem --force-dup.
  - Contrato de header (A-R) hard-fail: o Ivan edita a mao; shift de coluna poria
    numero errado em folha.

Fonte dos dados: aba `consolidado` da Brada_Dashboard_Deals (mesma do reporting
paralelo), ja enriquecida pelo build_consolidado_layer do sync.py. Sheets-only,
sem HubSpot.

Uso:
  python sheets_comissoes_ivan.py                       # dry-run, ciclo corrente
  python sheets_comissoes_ivan.py --cycle 2026-06       # dry-run, ciclo especifico
  python sheets_comissoes_ivan.py --cycle 2026-06 --write
  python sheets_comissoes_ivan.py --all-pending         # dry-run de TODOS os faltantes (catch-up)

Escopo deste modulo: a Controle de Vendas (cumulativa). As ABAS MENSAIS
({Mes}_MATCH, {Mes}_Elaboracao) e a tabela do Ricardo sao geradas pelo modulo
irmao sheets_abas_mensais_ivan.py (Bruno 30/06). Junho esta FECHADO (template).
Criterio do append = ciclo do mes (incremental).
"""

import argparse
import datetime
import json
import re
import sys

import gspread

from sync import get_sheets_client, PORTAL_ID  # noqa: F401  (PORTAL_ID p/ link futuro)
from sheets_reporting_financeiro_mensal import (
    parse_brl,
    parse_closedate,
    fmt_date_br,
    map_lei,
    load_consolidado,
    split_vendas,
    current_cycle,
    cycle_window,
    MIN_ROWS_GUARD,
)

# ===================================================
# CONFIG
# ===================================================

OFICIAL_ID_DEFAULT = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"  # Comissoes 2026 (Ivan)
CV_WS = "Controle de Vendas"

# Layout exato A-R da aba do Ivan (comparado com strip; o sheet tem headers com
# espaco no fim, ex.: "Valor "). Hard-fail se divergir.
CV_HEADER_EXPECTED = [
    "Cliente", "Fonte de recurso", "Proponente", "Dados para Cobrança", "Projeto",
    "Numero do projeto", "Nº conta M", "Nº conta C", "Valor", "Data do aporte",
    "DATA na Conta Movimentação", "Interno ou externo?", "Comissão BRADA",
    "Líquido Brada", "Comissão Ivan 8%", "Comissão Jaque 4%", "Comissão externo 3%",
    "Nome do externo",
]
N_CV_COLS = len(CV_HEADER_EXPECTED)          # 18 (A-R)
TECH_COL_HEADER = "deal_id"                  # coluna tecnica oculta, indice 18 (S)
TECH_COL_IDX = N_CV_COLS                      # 18 -> coluna S
TECH_COL_A1 = "S"

# indices 0-based dentro de A-R das colunas AUTO (Bruno: A,B,C,E,F,I,J,L)
COL = {
    "cliente": 0,    # A
    "fonte": 1,      # B
    "proponente": 2, # C
    "projeto": 4,    # E
    "numero": 5,     # F
    "valor": 8,      # I
    "data": 9,       # J
    "interno": 11,   # L
}


# ===================================================
# HELPERS
# ===================================================

def _norm(s):
    return " ".join(str(s or "").strip().lower().split())


def _digits(s):
    return re.sub(r"\D", "", str(s or ""))


def _valf(cell):
    """Celula (str BR ou numero do Sheets) -> float arredondado, ou None."""
    if cell is None or cell == "":
        return None
    if isinstance(cell, (int, float)):
        return round(float(cell), 2)
    return parse_brl(cell)


def utf8_stdout():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ===================================================
# LEITURA DA CV
# ===================================================

def read_cv(sh, ws_name=CV_WS):
    """Le a aba Controle de Vendas (UNFORMATTED). Valida o contrato de header.
    Retorna dict com: header bruto, data_rows (list de list), last_data_row (1-based),
    has_tech (bool), set de deal_ids tecnicos ja presentes, e o estado legado
    (valores + numero_projeto por linha) pra dedup heuristico."""
    resp = sh.values_get(
        f"'{ws_name}'!A1:Z2000",
        params={"valueRenderOption": "UNFORMATTED_VALUE"},
    )
    vals = resp.get("values", [])
    if not vals:
        raise SystemExit(f"[abort] aba '{ws_name}' veio vazia.")
    header = vals[0]
    header_strip = [str(h).strip() for h in header[:N_CV_COLS]]
    # Coluna A ("Cliente") e editada a mao pelo Ivan (virou ";" em 30/06) — valida
    # B-R estritamente e deixa a coluna A FLEXIVEL (os dados de A continuam sendo o
    # cliente). So as colunas que entram em folha precisam casar 1:1.
    diff = [(i, (header_strip[i] if i < len(header_strip) else "<faltando>"), CV_HEADER_EXPECTED[i])
            for i in range(1, N_CV_COLS)
            if i >= len(header_strip) or header_strip[i] != CV_HEADER_EXPECTED[i]]
    if diff:
        raise SystemExit(
            "[abort] header B-R da Controle de Vendas divergiu do contrato (alguem mexeu no layout?).\n"
            f"  esperado (B-R): {CV_HEADER_EXPECTED[1:]}\n"
            f"  atual    (B-R): {header_strip[1:]}\n"
            f"  diffs (idx, atual, esperado): {diff}\n"
            "  Escrita posicional abortada pra nao por numero errado em folha."
        )
    has_tech = len(header) > TECH_COL_IDX and str(header[TECH_COL_IDX]).strip() == TECH_COL_HEADER

    def cell(row, i):
        return (row + [""] * 40)[i]

    data_rows = []
    last_data_row = 1  # linha do header
    seen_deal_ids = set()
    legacy = []  # [{val, numdigits}] das linhas SEM deal_id
    for n, row in enumerate(vals[1:], start=2):  # n = numero da linha no sheet (1-based)
        ar = [cell(row, i) for i in range(N_CV_COLS)]
        if not any(str(c).strip() for c in ar):
            continue  # linha em branco (ignora gaps)
        last_data_row = n
        data_rows.append(ar)
        did = str(cell(row, TECH_COL_IDX)).strip() if has_tech else ""
        if did:
            seen_deal_ids.add(did)
        else:
            legacy.append({
                "val": _valf(cell(row, COL["valor"])),
                "num": _digits(cell(row, COL["numero"])),
                "cli": _norm(cell(row, COL["cliente"])),
            })
    return {
        "header": header,
        "data_rows": data_rows,
        "last_data_row": last_data_row,
        "has_tech": has_tech,
        "seen_deal_ids": seen_deal_ids,
        "legacy": legacy,
    }


# ===================================================
# SELECAO + DEDUP
# ===================================================

def select_cycle(inc, cycle, all_pending=False):
    """Filtra Match incluidas pelo ciclo (closedate na janela 21/mes-1 a 20/mes).
    all_pending=True ignora o ciclo (catch-up de tudo)."""
    out = []
    if all_pending:
        for r in inc:
            r["_date"] = parse_closedate(r["closedate"])
            out.append(r)
        return out
    start, end = cycle_window(cycle)
    for r in inc:
        d = parse_closedate(r["closedate"])
        if d and start <= d <= end:
            r["_date"] = d
            out.append(r)
    return out


def classify(cands, cv):
    """Separa candidatos em: ja_presente (deal_id batendo), provavel_dup
    (valor ja na CV em linha legada), novo. Heuristica conservadora: valor igual
    => provavel duplicata (nao grava sem --force-dup)."""
    legacy_by_val = {}
    for L in cv["legacy"]:
        if L["val"] is not None:
            legacy_by_val.setdefault(round(L["val"], 2), []).append(L)
    ja_presente, provavel_dup, novos = [], [], []
    for r in cands:
        did = str(r["deal_id"]).strip()
        if did and did in cv["seen_deal_ids"]:
            ja_presente.append(r)
            continue
        v = parse_brl(r["valor_bruto"])
        vr = round(v, 2) if v is not None else None
        hit = legacy_by_val.get(vr, []) if vr is not None else []
        if hit:
            # reforco: numero do projeto (so digitos) tambem bate?
            rnum = _digits(r.get("numero_projeto"))
            num_match = any(L["num"] and rnum and L["num"] == rnum for L in hit)
            r["_dup_num_match"] = num_match
            provavel_dup.append(r)
        else:
            novos.append(r)
    return ja_presente, provavel_dup, novos


# ===================================================
# MONTAGEM DA LINHA
# ===================================================

def build_row(r):
    """Monta a linha A-S (19 col): A,B,C,E,F,I,J,L preenchidas + S=deal_id.
    D,G,H,K,M-R em branco (manuais). Valor como numero; data como dd/mm/aaaa
    (USER_ENTERED converte pra data real no Sheets)."""
    out = [""] * (N_CV_COLS + 1)  # A..S
    out[COL["cliente"]] = r["cliente"]
    out[COL["fonte"]] = map_lei(r["lei_principal"])
    out[COL["proponente"]] = r["proponente"]
    out[COL["projeto"]] = r["nome_projeto"]
    out[COL["numero"]] = r["numero_projeto"]
    v = parse_brl(r["valor_bruto"])
    out[COL["valor"]] = v if v is not None else ""
    d = parse_closedate(r["closedate"])
    out[COL["data"]] = fmt_date_br(d) if d else ""
    out[COL["interno"]] = r["interno_externo"]
    out[TECH_COL_IDX] = str(r["deal_id"]).strip()
    return out


# ===================================================
# COMPLETUDE (F-valid)
# ===================================================

def completude_gaps(r):
    """Lista de campos faltando (data/valor/lei/cnpj) pro candidato. cnpj vazio
    sinaliza deal sem company (nome do cliente vem 'sujo')."""
    faltas = []
    if not parse_closedate(r["closedate"]):
        faltas.append("closedate")
    v = parse_brl(r["valor_bruto"])
    if not v or v <= 0:
        faltas.append("valor")
    lei = str(r.get("lei_principal", "")).strip()
    if not lei or lei == "(sem lei preenchida)":
        faltas.append("lei_principal")
    if not str(r.get("cnpj", "")).strip():
        faltas.append("cnpj(sem_company?)")
    return faltas


# ===================================================
# ESCRITA (APPEND)
# ===================================================

def ensure_tech_header(sh, ws, cv):
    """Garante o header 'deal_id' na coluna tecnica S1 e oculta a coluna S."""
    if not cv["has_tech"]:
        ws.update(values=[[TECH_COL_HEADER]], range_name=f"{TECH_COL_A1}1",
                  value_input_option="USER_ENTERED")
    # ocultar coluna S (idx 18) via batchUpdate
    sh.batch_update({
        "requests": [{
            "updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": TECH_COL_IDX, "endIndex": TECH_COL_IDX + 1},
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        }]
    })


def append_rows(ws, start_row, rows):
    """Escreve as linhas a partir de start_row (1-based) no range A:S, USER_ENTERED."""
    end_row = start_row + len(rows) - 1
    rng = f"A{start_row}:{TECH_COL_A1}{end_row}"
    ws.update(values=rows, range_name=rng, value_input_option="USER_ENTERED")
    return rng


def hide_rows(sh, ws, start_row, end_row):
    """Oculta as linhas start_row..end_row (1-based) — validacao antes de publicar."""
    sh.batch_update({
        "requests": [{
            "updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "ROWS",
                          "startIndex": start_row - 1, "endIndex": end_row},
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        }]
    })


# ===================================================
# RELATORIO (GATE)
# ===================================================

def print_gate(cycle, all_pending, inc, cands, ja_presente, provavel_dup, novos, cv, fonte_ts):
    print("=" * 78)
    print(f"CONTROLE DE VENDAS — append incremental | ciclo={cycle}"
          f"{' (ALL-PENDING)' if all_pending else ''}")
    print(f"consolidado fonte_ts={fonte_ts} | Match Won total={len(inc)} | no ciclo={len(cands)}")
    print(f"CV atual: {len(cv['data_rows'])} linhas | ultima linha com dados={cv['last_data_row']} | "
          f"coluna tecnica deal_id={'sim' if cv['has_tech'] else 'nao (1o run)'}")
    print("-" * 78)
    print(f"  ja na CV (deal_id):     {len(ja_presente)}")
    print(f"  provavel duplicata:     {len(provavel_dup)}  (valor ja existe; NAO grava sem --force-dup)")
    print(f"  NOVOS a appendar:       {len(novos)}")
    print("=" * 78)

    if provavel_dup:
        print("\n[PROVAVEL DUPLICATA] (revise: e a mesma venda ou so coincidiu o valor?)")
        for r in provavel_dup:
            v = parse_brl(r["valor_bruto"])
            print(f"  - {str(r['cliente'])[:40]:40} | R$ {v:>12,.2f} | proj={str(r['nome_projeto'])[:24]:24}"
                  f" | num_bate={'sim' if r.get('_dup_num_match') else 'nao'} | deal {r['deal_id']}")

    if novos:
        print("\n[NOVOS A APPENDAR]")
        for r in novos:
            v = parse_brl(r["valor_bruto"])
            faltas = completude_gaps(r)
            flag = f"  ⚠ FALTA: {','.join(faltas)}" if faltas else ""
            print(f"  + {str(r['cliente'])[:40]:40} | R$ {v:>12,.2f} | {r['interno_externo']:8}"
                  f" | {fmt_date_br(r.get('_date'))} | deal {r['deal_id']}{flag}")
    else:
        print("\n[NOVOS A APPENDAR] nenhum — nada a fazer neste ciclo.")

    # F-valid: completude HubSpot dos Match do ciclo (cobrar antes do dia 20)
    com_gaps = [(r, completude_gaps(r)) for r in cands]
    com_gaps = [(r, g) for r, g in com_gaps if g]
    if com_gaps:
        print(f"\n[COMPLETUDE] {len(com_gaps)}/{len(cands)} Match do ciclo com dado faltando no HubSpot "
              "(corrigir antes do dia 20):")
        for r, g in com_gaps:
            print(f"  ! {str(r['cliente'])[:40]:40} | faltam: {', '.join(g):28} | deal {r['deal_id']}")
    print()


# ===================================================
# MAIN
# ===================================================

def main():
    utf8_stdout()
    ap = argparse.ArgumentParser(description="Append incremental na Controle de Vendas oficial (Ivan)")
    ap.add_argument("--write", action="store_true", help="grava (default: dry-run)")
    ap.add_argument("--sheet-id", default=OFICIAL_ID_DEFAULT, help="planilha oficial (default: Comissoes 2026)")
    ap.add_argument("--ws", default=CV_WS, help=f"nome da aba (default: '{CV_WS}'; use outro p/ sandbox de teste)")
    ap.add_argument("--cycle", default=None, help="ciclo YYYY-MM (default: corrente)")
    ap.add_argument("--all-pending", action="store_true",
                    help="ignora ciclo: lista/append de TODOS os Match faltantes (catch-up; use com cuidado)")
    ap.add_argument("--force-dup", action="store_true",
                    help="grava tambem os 'provavel duplicata' (valor ja existe). NAO use sem validar.")
    ap.add_argument("--hidden", action="store_true",
                    help="oculta as linhas recem-appendadas (validacao antes de publicar dia 20)")
    args = ap.parse_args()

    cycle = args.cycle or current_cycle()
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", cycle):
        raise SystemExit(f"--cycle invalido: {cycle!r} (esperado YYYY-MM)")

    gc = get_sheets_client()
    rows, fonte_ts = load_consolidado(gc)
    if len(rows) < MIN_ROWS_GUARD:
        raise SystemExit(f"[abort] consolidado com {len(rows)} linhas (< {MIN_ROWS_GUARD}) — "
                         "possivel leitura no meio do sync horario. Nada escrito.")
    inc, _ = split_vendas(rows)
    cands = select_cycle(inc, cycle, all_pending=args.all_pending)

    sh = gc.open_by_key(args.sheet_id)
    cv = read_cv(sh, args.ws)

    ja_presente, provavel_dup, novos = classify(cands, cv)
    print_gate(cycle, args.all_pending, inc, cands, ja_presente, provavel_dup, novos, cv, fonte_ts)

    a_gravar = list(novos)
    if args.force_dup:
        a_gravar += provavel_dup
        print(f"[--force-dup] incluindo {len(provavel_dup)} provavel(is) duplicata(s) na gravacao.")

    if not args.write:
        print("[dry-run] nada gravado. Revise o gate acima e rode com --write quando aprovado.")
        return
    if not a_gravar:
        print("[write] 0 linhas novas — nada a gravar (idempotente).")
        return

    ws = sh.worksheet(args.ws)
    ensure_tech_header(sh, ws, cv)
    rows_out = [build_row(r) for r in a_gravar]
    start = cv["last_data_row"] + 1
    rng = append_rows(ws, start, rows_out)
    print(f"[write] OK: {len(rows_out)} linha(s) appendada(s) em {rng}. "
          f"Linhas existentes preservadas (append puro).")
    if args.hidden:
        end = start + len(rows_out) - 1
        hide_rows(sh, ws, start, end)
        print(f"[hidden] linhas {start}-{end} OCULTAS pra validacao (reexibir no dia 20).")
    for r in a_gravar:
        print(f"   + {str(r['cliente'])[:40]:40} | R$ {parse_brl(r['valor_bruto']):>12,.2f} | deal {r['deal_id']}")


if __name__ == "__main__":
    main()
