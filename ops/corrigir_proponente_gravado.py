# -*- coding: utf-8 -*-
"""Corrige a coluna de proponente nas linhas de Elaboracao que ja foram gravadas.

A automacao escrevia `nome_do_proponente` na coluna "Nome do Proponente". Esse campo foi
backfillado do `dealname` em 22/06 e o que ficou la e o nome do PROJETO. Nos deals antigos o
dealname por acaso era o proponente, entao a maioria das linhas parecia certa; nos criados
depois do backfill, nao. Medido em 19/08 na tabela do Ricardo: 5 de 34 traziam projeto no lugar
de proponente, e eram justamente os 5 mais recentes.

O `sheets_abas_mensais_ivan.py` ja passou a usar a empresa associada. Mas as duas frentes sao
append-only com dedup por `deal_id`, entao linha ja escrita nunca e revisitada: sem este script
o erro fica na planilha para sempre.

Escreve SO a celula do proponente, e so onde o valor difere. Nao toca em nenhuma outra coluna,
o que mantem intacto qualquer ajuste manual do Ivan ou do Ricardo.

Nome abreviado NAO e erro: "EGP BRASIL" e melhor que "ESCRITORIO DE GERENCIAMENTO DE PROJETOS ",
e "Escola de Danca Missao Intensidade" -> "Missao Intensidade" PERDE informacao. Medido em 20/08:
das 20 celulas que diferem do cadastro, 19 sao abreviacao deliberada e ficam como estao.

A vigesima e outra coisa: "<Empresa> - Novo(a) Deal", o nome que o HubSpot da a deal criado sem
nome. Ninguem escolheu escrever assim, e o sistema que preencheu, e num relatorio de apuracao isso
le como erro. Sai com --limpar-sufixo-de-deal, que reconhece so esse sufixo e mais nada.

Uso:
  python ops/corrigir_proponente_gravado.py
  python ops/corrigir_proponente_gravado.py --write
  python ops/corrigir_proponente_gravado.py --limpar-sufixo-de-deal --write
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gspread.utils import rowcol_to_a1

from sync import get_sheets_client
from financeiro_match_common import mesma_entidade, tem_sufixo_de_deal
from sheets_abas_mensais_ivan import (
    ELAB_TECH_IDX, OFICIAL_ID_DEFAULT, RICARDO_SHEET_ID, RICARDO_TAB,
    RIC_COL0, RIC_DATA_ROW0, RIC_DEALID_IDX, _proponente,
    load_hubspot_token, resolver_proponentes, search_elaboracao_won,
)

# (planilha, aba, coluna 0-based do proponente, coluna 0-based do deal_id, primeira linha de dados)
ALVOS = [
    (RICARDO_SHEET_ID, RICARDO_TAB, RIC_COL0, RIC_DEALID_IDX, RIC_DATA_ROW0),
    (OFICIAL_ID_DEFAULT, "Agosto_Elaboração de Projetos", 0, ELAB_TECH_IDX, 2),
    (OFICIAL_ID_DEFAULT, "Julho_Elaboração de Projetos", 0, ELAB_TECH_IDX, 2),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--limpar-sufixo-de-deal", action="store_true",
                    help="tambem grava onde a celula carrega o '- Novo(a) Deal' do HubSpot")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()
    deals = search_elaboracao_won(token)
    com_empresa = resolver_proponentes(deals, token)
    correto = {d["id"]: (_proponente(d["properties"]) or "").strip() for d in deals}
    print(f"HubSpot: {len(deals)} deals | com empresa associada: {com_empresa} | "
          f"fallback (card sem empresa): {len(deals) - com_empresa}\n")

    gc = get_sheets_client()
    total = 0
    for sheet_id, aba, col_prop, col_id, primeira in ALVOS:
        sh = gc.open_by_key(sheet_id)
        try:
            vals = sh.values_get(f"'{aba}'!A1:AZ3000",
                                 params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])
        except Exception as erro:
            print(f"[pula] {aba}: {str(erro)[:110]}")
            continue

        mudancas, abreviados, boilerplate = [], [], []
        for n, raw in enumerate(vals, start=1):
            if n < primeira:
                continue
            linha = list(raw) + [""] * (col_id + 1)
            did = str(linha[col_id]).strip()
            if not did or did not in correto:
                continue
            atual, alvo = str(linha[col_prop]).strip(), correto[did]
            if not alvo or atual == alvo:
                continue
            if not mesma_entidade(atual, alvo):
                mudancas.append((n, atual, alvo, did))     # entidade diferente: e o nome do PROJETO
            elif tem_sufixo_de_deal(atual):
                boilerplate.append((n, atual, alvo, did))  # mesmo proponente, sujo de CRM
            else:
                abreviados.append((n, atual, alvo))        # mesmo proponente, so mais curto

        print("=" * 104)
        com_id = sum(1 for n, r in enumerate(vals, 1)
                     if n >= primeira and str((list(r) + [""] * (col_id + 1))[col_id]).strip())
        print(f"{aba} | linhas com deal_id: {com_id} | ENTIDADE ERRADA: {len(mudancas)} "
              f"| sufixo de deal: {len(boilerplate)} "
              f"| nome abreviado (preservado): {len(abreviados)}")
        for n, atual, alvo, did in mudancas:
            print(f"  {rowcol_to_a1(n, col_prop + 1):>5}  {atual[:38]:<40} -> {alvo[:44]:<46} deal {did}")
        marca = "" if args.limpar_sufixo_de_deal else "  [use --limpar-sufixo-de-deal]"
        for n, atual, alvo, did in boilerplate:
            print(f"  {rowcol_to_a1(n, col_prop + 1):>5}  {atual[:38]:<40} -> {alvo[:34]:<36}"
                  f"sufixo do HubSpot{marca}")
        for n, atual, alvo in abreviados:
            print(f"  {rowcol_to_a1(n, col_prop + 1):>5}  [mantido] {atual[:34]:<36} (cadastro: {alvo[:40]})")

        a_gravar = mudancas + (boilerplate if args.limpar_sufixo_de_deal else [])
        if not a_gravar:
            print("  (nada a corrigir)")
            continue
        total += len(a_gravar)
        if not args.write:
            continue
        ws = sh.worksheet(aba)
        ws.batch_update([{"range": rowcol_to_a1(n, col_prop + 1), "values": [[alvo]]}
                         for n, _atual, alvo, _did in a_gravar],
                        value_input_option="USER_ENTERED")
        print(f"  [write] {len(a_gravar)} celula(s) corrigida(s). Nenhuma outra coluna tocada.")

    print("=" * 104)
    print(f"TOTAL: {total} celula(s)")
    if not args.write:
        print("[dry-run] nada gravado. Use --write.")


if __name__ == "__main__":
    main()
