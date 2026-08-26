# -*- coding: utf-8 -*-
"""Preenche celula VAZIA das abas de Elaboracao a partir do HubSpot.

As frentes de Elaboracao sao append-only com dedup por `deal_id`: linha ja escrita nunca e
revisitada. Entao, quando alguem preenche um campo no HubSpot depois do fecho, a planilha fica
parada no que estava. Foi o caso de 19/08: o Ricardo confirmou em contrato a condicao de pagamento
de tres projetos, o campo entrou no CRM, e as abas continuariam com a celula em branco para sempre.

Escreve SO onde a celula esta vazia. Nunca corrige o que alguem digitou: se o HubSpot e a planilha
divergem, isso e conversa, nao script. Mesmo principio ja em producao na aba da Bia.

Uso:
  python ops/preencher_vazias_elaboracao.py
  python ops/preencher_vazias_elaboracao.py --write
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gspread.utils import rowcol_to_a1

from sync import get_sheets_client
from sheets_reporting_financeiro_mensal import fmt_date_br, parse_brl, parse_closedate
from sheets_abas_mensais_ivan import (
    ELAB_TECH_IDX, OFICIAL_ID_DEFAULT, RICARDO_SHEET_ID, RICARDO_TAB, RIC_COL0, RIC_DATA_ROW0,
    RIC_DEALID_IDX, _is_captado, load_hubspot_token, resolver_proponentes, search_elaboracao_won,
)

# (planilha, aba, primeira linha de dados, coluna do deal_id, {coluna: campo})
# Os campos sao os que o time preenche no card DEPOIS do fecho. Nome de proponente
# fica de fora: tem script proprio (corrigir_proponente_gravado), porque la a regra
# nao e "vazia", e "entidade diferente".
ALVOS = [
    (RICARDO_SHEET_ID, RICARDO_TAB, RIC_DATA_ROW0, RIC_DEALID_IDX, {
        RIC_COL0 + 2: "condicao_de_pagamento", RIC_COL0 + 4: "lei_principal"}),
    (OFICIAL_ID_DEFAULT, "Maio_Elaboração de Projetos", 2, None, {2: "condicao_de_pagamento",
                                                                  4: "lei_principal"}),
    (OFICIAL_ID_DEFAULT, "Junho_Elaboração de Projetos", 2, None, {3: "condicao_de_pagamento",
                                                                   5: "lei_principal"}),
    (OFICIAL_ID_DEFAULT, "Julho_Elaboração de Projetos", 2, ELAB_TECH_IDX, {
        3: "condicao_de_pagamento", 5: "lei_principal"}),
    (OFICIAL_ID_DEFAULT, "Agosto_Elaboração de Projetos", 2, ELAB_TECH_IDX, {
        3: "condicao_de_pagamento", 5: "lei_principal"}),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()
    deals = search_elaboracao_won(token)
    resolver_proponentes(deals, token)
    por_id = {d["id"]: d["properties"] for d in deals}

    gc = get_sheets_client()
    total = 0
    for sheet_id, aba, primeira, col_id, mapa in ALVOS:
        sh = gc.open_by_key(sheet_id)
        try:
            vals = sh.values_get(f"'{aba}'!A1:BZ3000",
                                 params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])
        except Exception as erro:
            print(f"[pula] {aba}: {str(erro)[:90]}")
            continue
        if col_id is None:
            print(f"[pula] {aba}: sem coluna de deal_id, nao da para saber qual card e qual linha")
            continue

        mudancas = []
        for n, raw in enumerate(vals, start=1):
            if n < primeira:
                continue
            linha = list(raw) + [""] * 60
            did = str(linha[col_id]).strip()
            p = por_id.get(did)
            if not did or not p:
                continue
            for col, campo in mapa.items():
                if str(linha[col]).strip():
                    continue                       # so preenche vazia
                valor = (p.get(campo) or "").strip()
                if valor:
                    mudancas.append((n, col, campo, valor, did))

        print("=" * 100)
        print(f"{aba} | {len(mudancas)} celula(s) vazia(s) com valor no HubSpot")
        for n, col, campo, valor, did in mudancas:
            print(f"  {rowcol_to_a1(n, col + 1):>5}  {campo:<24} <- {valor!r}  deal {did}")
        if not mudancas:
            print("  (nada a preencher)")
            continue
        total += len(mudancas)
        if not args.write:
            continue
        ws = sh.worksheet(aba)
        ws.batch_update([{"range": rowcol_to_a1(n, col + 1), "values": [[valor]]}
                         for n, col, _c, valor, _d in mudancas], value_input_option="USER_ENTERED")
        print(f"  [write] {len(mudancas)} celula(s).")

    print("=" * 100)
    print(f"TOTAL: {total} celula(s)")
    if not args.write:
        print("[dry-run] nada gravado. Use --write.")


if __name__ == "__main__":
    main()
