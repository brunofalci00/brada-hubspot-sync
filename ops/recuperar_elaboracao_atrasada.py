# -*- coding: utf-8 -*-
"""Recupera deal de Elaboracao que entrou em Ganho DEPOIS do fecho do ciclo dele.

O run do dia 20 fotografa o ciclo naquele instante. Card que vira Ganho no dia
21 pertence ao ciclo anterior pelo `closedate`, mas a aba daquele mes ja foi
gerada e nunca mais e revisitada — a linha some do relatorio mensal para sempre.
Foi o caso do `Florescer Financeiro` (deal 63027767681, closedate 16/07): entrou
depois do fecho de 20/07 e ficou fora de `Julho_Elaboracao de Projetos`.

Complicacao: as abas ja fechadas ganham protecao de coluna. A `Julho_Elaboracao`
tem a coluna G (Data de pagamento) protegida e a service account nao esta na
lista de editores, entao um append normal em A:M seria recusado inteiro.

Como isto contorna: escreve em DOIS pedacos, A:F e H:M, pulando G. So faz isso
quando G deveria mesmo ficar vazia — condicao de captacao ("10% vr captado"),
onde nao ha pagamento no fechamento. Se o deal tiver pagamento no fechamento, G
precisa de valor e o script se recusa: melhor apontar do que gravar linha
incompleta numa aba que alimenta conferencia de folha.

Uso:
  python ops/recuperar_elaboracao_atrasada.py --cycle 2026-07
  python ops/recuperar_elaboracao_atrasada.py --cycle 2026-07 --write
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gspread.utils import rowcol_to_a1

from sync import get_sheets_client
from sheets_reporting_financeiro_mensal import cycle_window, parse_closedate, fmt_date_br
from sheets_abas_mensais_ivan import (
    OFICIAL_ID_DEFAULT, ELAB_TECH_IDX, MES_PT, _is_captado, _proponente,
    build_elaboracao_row, load_hubspot_token, read_existing, search_elaboracao_won,
)

COL_G = 6  # Data de pagamento — protegida nas abas ja fechadas


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycle", required=True, help="ciclo YYYY-MM da aba a completar")
    ap.add_argument("--sheet-id", default=OFICIAL_ID_DEFAULT)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ini, fim = cycle_window(args.cycle)
    tab = f"{MES_PT[int(args.cycle[5:7])]}_Elaboração de Projetos"

    deals = search_elaboracao_won(load_hubspot_token())
    do_ciclo = []
    for d in deals:
        data = parse_closedate(d["properties"].get("closedate"))
        if data and ini <= data <= fim:
            d["_d"] = data
            do_ciclo.append(d)

    gc = get_sheets_client()
    sh = gc.open_by_key(args.sheet_id)
    existe, ultima, ids, is_auto = read_existing(sh, tab, 6, ELAB_TECH_IDX)
    if not existe:
        raise SystemExit(f"[abort] aba {tab!r} nao existe.")
    if not is_auto:
        raise SystemExit(f"[abort] {tab!r} nao tem a coluna tecnica deal_id: aba manual, nao mexo.")

    faltando = [d for d in do_ciclo if d["id"] not in ids]
    print(f"{tab} | ciclo {args.cycle} ({ini} a {fim})")
    print(f"  no HubSpot: {len(do_ciclo)} | ja na aba: {len(do_ciclo) - len(faltando)} | FALTANDO: {len(faltando)}")

    escrever, recusados = [], []
    for d in faltando:
        cond = (d["properties"].get("condicao_de_pagamento") or "").strip()
        alvo = escrever if _is_captado(cond) else recusados
        alvo.append((d, cond))

    for d, cond in escrever:
        print(f"  + {_proponente(d['properties'])[:40]:40} | {fmt_date_br(d['_d'])} | "
              f"cond={cond!r} | deal {d['id']}")
    for d, cond in recusados:
        print(f"  [RECUSADO] {_proponente(d['properties'])[:34]:34} | cond={cond!r}: tem pagamento no "
              f"fechamento, entao a coluna G precisa de valor — e ela esta protegida. "
              f"Preencher a mao ou liberar a protecao. deal {d['id']}")
    if not escrever:
        print("  (nada a escrever)")
        return
    if not args.write:
        print("[dry-run] nada gravado.")
        return

    ultima_col = rowcol_to_a1(1, ELAB_TECH_IDX + 1).rstrip("1")
    linha = ultima + 1
    for d, _cond in escrever:
        row = build_elaboracao_row(d)
        assert row[COL_G] == "" and row[COL_G + 1] == "", "G/H deveriam estar vazios em captacao"
        ws = sh.worksheet(tab)
        # dois pedacos, pulando G (protegida)
        ws.update(values=[row[:COL_G]], range_name=f"A{linha}:F{linha}",
                  value_input_option="USER_ENTERED")
        ws.update(values=[row[COL_G + 1:]], range_name=f"H{linha}:{ultima_col}{linha}",
                  value_input_option="USER_ENTERED")
        print(f"[write] {tab} linha {linha}: A:F + H:{ultima_col} (G protegida, vazia por captacao) "
              f"deal {d['id']}")
        linha += 1


if __name__ == "__main__":
    main()
