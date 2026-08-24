# -*- coding: utf-8 -*-
"""Abre duas colunas no INICIO de cada aba da planilha CLIENTES/MATCH/COMISSAO.

  A = deal_id       (oculta) — a chave que liga a linha ao negocio
  B = Link HubSpot  (visivel) — o financeiro abre o negocio e ve recibo e anexo

Por que no INICIO, e nao no fim. Em 20/08 alguem ordenou o Controle de Vendas em ordem
alfabetica selecionando um intervalo que nao incluia a coluna do link. As 40 linhas trocaram de
lugar, a coluna ficou parada, e **31 de 31 links passaram a apontar para o cliente errado** — numa
planilha que alimenta folha, sem ninguem ter tocado em nenhum link.

Coluna no comeco viaja junto com qualquer selecao que comece em A. Coluna no fim, nao.

Sem a chave nao ha dedup: a segunda execucao da automacao duplicaria tudo.

Dry-run por padrao. Idempotente: se as colunas ja existirem, nao insere de novo.

Uso:
  python ops/preparar_abas_planilha_leis.py
  python ops/preparar_abas_planilha_leis.py --write
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sync import get_sheets_client
import planilha_leis as pl

PLANILHA = "1rd14NDGamfvDEolnLt96n95wypCxnbQYI3ARfELB7Vg"
CAB_ID, CAB_LINK = "deal_id", "Link HubSpot"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    gc = get_sheets_client()
    sh = gc.open_by_key(PLANILHA)
    por_titulo = {ws.title: ws for ws in sh.worksheets()}

    plano = []
    for aba in pl.ABAS:
        ws = por_titulo.get(aba)
        if ws is None:
            print(f"[abort] aba {aba!r} nao existe na planilha")
            raise SystemExit(1)
        linha_cab = pl.LAYOUT[aba]["linha_cabecalho"]
        vals = sh.values_get(f"'{aba}'!A1:C{linha_cab}",
                             params={"valueRenderOption": "FORMATTED_VALUE"}).get("values", [])
        atual = vals[linha_cab - 1] if len(vals) >= linha_cab else []
        primeira = str(atual[0]).strip() if atual else ""
        if primeira == CAB_ID:
            print(f"  [ja tem]  {aba:<14} coluna A ja e {CAB_ID!r}")
            continue
        plano.append((aba, ws, linha_cab, primeira))
        print(f"  [inserir] {aba:<14} hoje a coluna A e {primeira!r} -> vira coluna C")

    if not plano:
        print("\n(todas as abas ja preparadas)")
        return
    print()
    print(f"{len(plano)} aba(s) a preparar. Cada uma ganha 2 colunas no inicio e desloca o resto.")
    if not args.write:
        print("[dry-run] nada alterado. Use --write.")
        return

    for aba, ws, linha_cab, _p in plano:
        sh.batch_update({"requests": [
            {"insertDimension": {"range": {
                "sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 2},
                "inheritFromBefore": False}},
            {"updateDimensionProperties": {
                "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 1},
                "properties": {"hiddenByUser": True}, "fields": "hiddenByUser"}},
        ]})
        ws.update(values=[[CAB_ID, CAB_LINK]], range_name=f"A{linha_cab}:B{linha_cab}",
                  value_input_option="RAW")
        conf = sh.values_get(f"'{aba}'!A{linha_cab}:B{linha_cab}").get("values", [[]])[0]
        ok = conf[:2] == [CAB_ID, CAB_LINK]
        print(f"  [write] {aba:<14} read-back {conf} -> {'OK' if ok else 'FALHOU'}")
        if not ok:
            raise SystemExit("[abort] read-back nao confirmou; parando antes da proxima aba.")

    print()
    print("Colunas prontas. O LAYOUT do planilha_leis.py precisa do deslocamento de +2.")


if __name__ == "__main__":
    main()
