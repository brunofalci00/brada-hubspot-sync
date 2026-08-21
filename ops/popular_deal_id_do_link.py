# -*- coding: utf-8 -*-
"""Recupera o `deal_id` de dentro do link do HubSpot e grava na coluna tecnica.

O Controle de Vendas tem 40 linhas e NENHUMA com o `deal_id` gravado: a automacao so escreve a
coluna tecnica em linha nova, e todas vieram da carga historica do Ivan. Consequencia: a cada
rodada a ligacao linha-negocio e reconstruida por adivinhacao (cliente + projeto + numero + valor +
data). Dai os ambiguos e as linhas que nao casam. Se alguem corrigir um valor ou um nome, a linha
perde o negocio em silencio.

Mas a chave ja esta na planilha desde 19/08, dentro da URL da coluna `Link HubSpot`. Este script
so extrai de la e grava onde a celula tecnica esta vazia. Nada e pedido a ninguem.

Tres travas, porque escrever id errado e pior que id nenhum:
  1. So preenche celula VAZIA. Nunca reescreve id existente.
  2. Link de BUSCA nao vira id (linha orfa continua orfa).
  3. Confere no HubSpot que o id existe antes de gravar; id de negocio apagado nao entra.

Uso:
  python ops/popular_deal_id_do_link.py
  python ops/popular_deal_id_do_link.py --write
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gspread.utils import rowcol_to_a1

from sync import get_sheets_client
from financeiro_match_common import deal_id_do_link
from hubspot_financeiro import BASE, load_hubspot_token
import sheets_comissoes_ivan as vendas
import sheets_cobrancas_bia as bia

# (planilha, aba, coluna do link, coluna tecnica, primeira linha de dados)
ALVOS = [
    (vendas.OFICIAL_ID_DEFAULT, "Controle de Vendas", vendas.LINK_IDX, vendas.TECH_IDX, 3),
    (bia.OFICIAL_ID_DEFAULT, "Controle de Cobranças - Bia", bia.LINK_IDX, bia.TECH_IDX, 2),
]


def existem_no_hubspot(ids, token):
    """Quais desses ids sao negocios de verdade hoje."""
    if not ids:
        return set()
    body = {"properties": ["dealname"], "inputs": [{"id": i} for i in sorted(ids)]}
    req = urllib.request.Request(
        f"{BASE}/crm/v3/objects/deals/batch/read", data=json.dumps(body).encode(), method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            res = json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"[abort] batch/read -> {e.code}: {e.read()[:300]}")
    return {str(x["id"]) for x in res.get("results", [])}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()
    gc = get_sheets_client()
    total = 0

    for sheet_id, aba, col_link, col_tech, primeira in ALVOS:
        sh = gc.open_by_key(sheet_id)
        try:
            vals = sh.values_get(f"'{aba}'!A1:BZ3000",
                                 params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])
        except Exception as erro:
            print(f"[pula] {aba}: {str(erro)[:100]}")
            continue

        candidatos, ja_tem, sem_link, so_busca = [], 0, 0, 0
        for n, raw in enumerate(vals, start=1):
            if n < primeira:
                continue
            linha = list(raw) + [""] * (max(col_link, col_tech) + 1)
            if not str(linha[0]).strip():
                continue
            if str(linha[col_tech]).strip():
                ja_tem += 1
                continue
            link = str(linha[col_link]).strip()
            if not link:
                sem_link += 1
                continue
            did = deal_id_do_link(link)
            if not did:
                so_busca += 1          # link de busca: a linha nao tem negocio, e isso e o certo
                continue
            candidatos.append((n, did, str(linha[0])[:34]))

        reais = existem_no_hubspot({d for _n, d, _c in candidatos}, token)
        gravar = [(n, d, c) for n, d, c in candidatos if d in reais]
        fantasmas = [(n, d, c) for n, d, c in candidatos if d not in reais]

        print("=" * 104)
        print(f"{aba}")
        print(f"  ja tinham deal_id: {ja_tem} | sem link: {sem_link} | link de busca (orfas): {so_busca}")
        print(f"  a gravar: {len(gravar)} | id que nao existe mais no HubSpot: {len(fantasmas)}")
        for n, did, cli in gravar:
            print(f"    {rowcol_to_a1(n, col_tech + 1):>6}  {cli:<36} <- {did}")
        for n, did, cli in fantasmas:
            print(f"    [PULADO] linha {n} {cli:<30} id {did} nao existe mais; conferir a mao")

        if not gravar:
            print("  (nada a gravar)")
            continue
        total += len(gravar)
        if not args.write:
            continue
        ws = sh.worksheet(aba)
        ws.batch_update([{"range": rowcol_to_a1(n, col_tech + 1), "values": [[did]]}
                         for n, did, _c in gravar], value_input_option="RAW")
        print(f"  [write] {len(gravar)} celula(s). Nenhuma outra coluna tocada.")

    print("=" * 104)
    print(f"TOTAL: {total} celula(s)")
    if not args.write:
        print("[dry-run] nada gravado. Use --write.")


if __name__ == "__main__":
    main()
