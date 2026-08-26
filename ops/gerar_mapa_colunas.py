# -*- coding: utf-8 -*-
"""Gera o mapa de colunas da planilha do financeiro: o que a automacao preenche e o que fica manual.

O documento sai do CODIGO, nao de memoria: le o LAYOUT de `planilha_leis.py` e os cabecalhos
REAIS das 6 abas, e cruza os dois. Se alguem inserir coluna na planilha ou mudar o mapa, basta
rodar de novo — o documento nao envelhece calado.

Tambem mede, no HubSpot, quanto cada campo de origem vem preenchido de verdade. "Automatica" quer
dizer que o script sabe de onde puxar, e nao que a celula vem cheia; misturar as duas coisas e o
jeito mais rapido de prometer o que a planilha nao entrega.

Uso:
  python ops/gerar_mapa_colunas.py
  python ops/gerar_mapa_colunas.py --out caminho.md
"""
import argparse
import collections
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sync import get_sheets_client
from hubspot_financeiro import BASE, load_hubspot_token
import planilha_leis as pl

PLANILHA = "1rd14NDGamfvDEolnLt96n95wypCxnbQYI3ARfELB7Vg"
PADRAO = (r"C:\Users\bruno\Documents\Brada\HubSpot\Relatorios_Financeiro"
          r"\Mapa_Colunas_Planilha_Leis.md")

# De onde cada coluna sai. Chave = nome canonico no LAYOUT.
ORIGEM = {
    "patrocinador": ("empresa associada ao negocio", None),
    "lei": ("`lei_principal`", "lei_principal"),
    "projeto": ("`nome_do_projeto`", "nome_do_projeto"),
    "obs": ("derivado de `tipo_de_proponente`", "tipo_de_proponente"),
    "numero": ("`numero_do_projeto`", "numero_do_projeto"),
    "match": ("`closedate`", "closedate"),
    "valor_match": ("R$ 700 fixo, constante no codigo", None),
    "t_aporte": ("`valor_do_aporte`", "valor_do_aporte"),
    "fp": ("`numero_parcelas_financeiro`", "numero_parcelas_financeiro"),
    "proponente": ("`nome_do_proponente`", "nome_do_proponente"),
    "nome": ("`nome_contato_proponente`", "nome_contato_proponente"),
    "email": ("`email_proponente`", "email_proponente"),
    "contato": ("`telefone_proponente`", "telefone_proponente"),
    "pct": ("`percentual_brada`", "percentual_brada"),
    "valor_comissao": ("percentual x aporte, calculado", None),
    # Existem no LAYOUT mas a automacao nao escreve: o campo do CRM vive vazio.
    "contrato": (None, "numero_contrato_financeiro"),
    "status": (None, None),
}

GRUPO_MANUAL = {
    "COB.PROPON.": "cobranca", "COBRAR PROP.": "cobranca", "COMPROVANTE": "cobranca",
    "% JAQUE": "comissao interna", "STATUS": "cobranca", "OBS": "livre",
    "FRUIÇÃO": "rito fiscal", "DECLAR.PAT.": "rito fiscal", "APROVAÇÃO": "rito fiscal",
    "COMP.MATCH": "rito fiscal", "COMPROV.": "rito fiscal", "ABATIM.": "rito fiscal",
    "RECIBO": "parcelas", "RECIBO 20%": "parcelas", "VALOR TOTAL": "cobranca",
}


def _post(url, token, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST", headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def medir_preenchimento(token, campos):
    """Quantos negocios ganhos tem cada campo preenchido de verdade."""
    estagios = ["1253324968", "contractsent", "1247329455", "1247329456"]
    todos, after = [], None
    while True:
        body = {"filterGroups": [{"filters": [
            {"propertyName": "dealstage", "operator": "IN", "values": estagios}]}],
            "properties": list(campos), "limit": 100}
        if after:
            body["after"] = after
        r = _post(f"{BASE}/crm/v3/objects/deals/search", token, body)
        todos += r["results"]
        after = (r.get("paging") or {}).get("next", {}).get("after")
        if not after:
            break
    n = len(todos) or 1
    return {c: sum(1 for d in todos if str(d["properties"].get(c) or "").strip())
            for c in campos}, len(todos)


def grupo_de(cabecalho):
    base = cabecalho.strip().upper().rstrip(" 1234")
    for chave, g in GRUPO_MANUAL.items():
        if base.startswith(chave.upper().rstrip(" 1234")):
            return g
    if base.endswith("-DATA") or base[:1].isdigit():
        return "parcelas"
    return "outro"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=PADRAO)
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()
    campos = sorted({c for _t, c in ORIGEM.values() if c} | {"uf_incentivo"})
    fill, total_deals = medir_preenchimento(token, campos)

    gc = get_sheets_client()
    sh = gc.open_by_key(PLANILHA)

    L = []
    L.append("# Mapa de colunas: o que a automacao preenche e o que fica manual")
    L.append("")
    L.append("Planilha `CLIENTES/MATCH/COMISSAO`, 6 abas, uma por enquadramento fiscal.")
    L.append("")
    L.append("> Gerado por `ops/gerar_mapa_colunas.py` a partir do codigo e dos cabecalhos reais "
             "das abas. Rodar de novo depois de qualquer mudanca de layout.")
    L.append("")
    L.append("**Automatica** quer dizer que o script sabe de onde puxar, e nao que a celula vem "
             "cheia. A coluna de preenchimento abaixo mostra a diferenca.")
    L.append("")

    tot_a = tot_m = 0
    por_grupo = collections.Counter()
    resumo = []

    for aba in pl.ABAS:
        lc = pl.LAYOUT[aba]["linha_cabecalho"]
        vals = sh.values_get(f"'{aba}'!A1:CZ{lc}",
                             params={"valueRenderOption": "FORMATTED_VALUE"}).get("values", [])
        cab = list(vals[lc - 1]) if len(vals) >= lc else []
        inv = {pl.pos(aba, k): k for k in pl.LAYOUT[aba]["cols"]}
        inv[pl.COL_DEAL_ID] = "__deal_id"
        inv[pl.COL_LINK] = "__link"

        auto, manual = [], []
        for j, h in enumerate(cab):
            h = str(h).strip()
            if not h:
                continue
            k = inv.get(j)
            if k == "__deal_id":
                auto.append((h, "chave do negocio (coluna oculta)", ""))
            elif k == "__link":
                auto.append((h, "montado do `deal_id`", ""))
            elif k and ORIGEM.get(k, (None, None))[0]:
                desc, prop = ORIGEM[k]
                pct = f"{100 * fill[prop] // total_deals}%" if prop else "sempre"
                auto.append((h, desc, pct))
            else:
                manual.append((h, grupo_de(h)))
                por_grupo[grupo_de(h)] += 1

        tot_a += len(auto)
        tot_m += len(manual)
        resumo.append((aba.strip(), len(auto), len(manual)))

        L.append(f"## {aba.strip()}")
        L.append("")
        L.append(f"{len(auto)} automaticas, {len(manual)} manuais. "
                 f"Cabecalho na linha {lc}.")
        L.append("")
        L.append("### Automaticas")
        L.append("")
        L.append("| Coluna | De onde vem | Vem preenchido |")
        L.append("|---|---|---|")
        for h, desc, pct in auto:
            L.append(f"| {h} | {desc} | {pct} |")
        L.append("")
        L.append("### Manuais")
        L.append("")
        L.append("| Coluna | Por que fica manual |")
        L.append("|---|---|")
        for h, g in manual:
            L.append(f"| {h} | {g} |")
        L.append("")

    L.insert(8, "")
    L.insert(8, "| Aba | Automaticas | Manuais |\n|---|---|---|\n" + "\n".join(
        f"| {a} | {x} | {y} |" for a, x, y in resumo) +
        f"\n| **Total** | **{tot_a}** | **{tot_m}** |")
    L.insert(8, "")
    L.insert(8, "## Resumo")

    L.append("## Onde estao as manuais")
    L.append("")
    L.append("| Grupo | Colunas |")
    L.append("|---|---|")
    for g, n in por_grupo.most_common():
        L.append(f"| {g} | {n} |")
    L.append("")
    L.append("As de **cobranca** e **comissao interna** ficaram fora por decisao: a automacao nao "
             "mexe em cobranca. As de **parcelas** e **rito fiscal** nao existem no HubSpot, e "
             "criar campo para elas significaria manutencao recorrente pelo comercial.")
    L.append("")
    L.append("## O que decide se a linha e escrita")
    L.append("")
    L.append(f"`lei_principal` ({100 * fill['lei_principal'] // total_deals}% preenchida hoje) e "
             f"`uf_incentivo` ({100 * fill['uf_incentivo'] // total_deals}%). As duas passaram a "
             "ser obrigatorias em 24/08, entao negocio novo vem completo; o numero acima olha para "
             f"os {total_deals} ganhos historicos.")
    L.append("")
    L.append("Sem uma das duas, o script **nao escreve**: manda para revisao com o link do card. "
             "Escrever na aba errada manda o negocio para o rito fiscal errado, e isso e pior do "
             "que a linha nao existir.")
    L.append("")

    with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(L))
    print(f"{tot_a} automaticas | {tot_m} manuais | {tot_a + tot_m} colunas nas 6 abas")
    print(f"medido em {total_deals} negocios ganhos")
    print(args.out)


if __name__ == "__main__":
    main()
