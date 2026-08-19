# -*- coding: utf-8 -*-
"""Coluna "Link HubSpot" nas abas da planilha de Comissoes 2026.

O financeiro precisa chegar no negocio a partir da planilha, para ver recibo, anexo e o que nao
cabe numa celula. Hoje a planilha e um beco sem saida.

Cinco abas ja tem `deal_id` escondido e o link sai direto dele. Nas outras e preciso casar cada
linha com um negocio, e ai mora o risco: link errado manda o financeiro para o cliente errado numa
planilha que alimenta folha de pagamento. Por isso nada casa por nome sozinho — sempre com um
segundo campo corroborando — e so o que fica ALTA e gravado.

Niveis:
  ALTA     candidato unico com chave forte. Grava o link do negocio.
  MEDIA    candidato unico, mas a chave e fraca (nome de um token so, ou valor divergente).
  AMBIGUA  mais de um candidato.
  ORFA     nenhum candidato. Grava um link de BUSCA pelo nome, para o financeiro achar sozinho.

MEDIA e AMBIGUA nao sao gravadas: saem no relatorio para alguem olhar.

Uso:
  python ops/vincular_link_hubspot.py
  python ops/vincular_link_hubspot.py --write
  python ops/vincular_link_hubspot.py --aba "Junho_MATCH"
"""
import argparse
import datetime as dt
import os
import sys
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gspread.utils import rowcol_to_a1

from sync import get_sheets_client
from sheets_reporting_financeiro_mensal import load_consolidado, parse_brl, parse_closedate
from financeiro_match_common import (
    PORTAL_ID, forca_do_nome, linha_e_dado, money, norm, select_match_won, text_id,
)
from sheets_abas_mensais_ivan import (
    OFICIAL_ID_DEFAULT, load_hubspot_token, resolver_proponentes, search_elaboracao_won, _proponente,
)

HEADER_LINK = "Link HubSpot"


def com_retry(fn, *a, **kw):
    """Repete em falha transitoria do Google (503, 429).

    O Sheets devolveu 503 duas vezes seguidas ao abrir esta planilha em 19/08, e
    voltou sozinho. Sem retry, um 503 no meio da carga deixaria parte das abas com
    link e parte sem, e a proxima execucao teria que descobrir onde parou.
    """
    for tentativa in range(1, 6):
        try:
            return fn(*a, **kw)
        except Exception as erro:
            transitorio = any(c in str(erro) for c in ("503", "429", "500", "Timeout"))
            if not transitorio or tentativa == 5:
                raise
            espera = 4 * tentativa
            print(f"  [retry {tentativa}/4] {str(erro)[:70]} — nova tentativa em {espera}s")
            time.sleep(espera)


def link_negocio(deal_id):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{deal_id}"


def link_busca(nome):
    """Lista de negocios ja filtrada pelo nome. Para linha sem candidato."""
    q = urllib.parse.quote(str(nome or "").strip())
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/objects/0-3/views/all/list?query={q}"


def data_da_celula(v):
    """A planilha devolve data ora como serial do Sheets, ora como texto."""
    if isinstance(v, (int, float)) and v:
        return dt.date(1899, 12, 30) + dt.timedelta(days=int(v))
    return parse_closedate(str(v or ""))


# (aba, universo, coluna do link esperada, coluna tecnica ou None, mapa de chaves)
# A coluna do link e "ultima visivel + 1", conferida em 19/08. O script recalcula
# em runtime e aborta se divergir: layout muda, e escrever no lugar errado numa
# planilha de folha e pior que nao escrever.
ABAS = [
    ("Controle de Vendas",            "match", 26, 31, {"cliente": 0, "projeto": 6, "numero": 7, "valor": 10, "data": 11}),
    ("Controle de Cobranças - Bia",   "match", 21, 28, {"cliente": 0, "projeto": 9, "numero": 10, "valor": 11, "data": 14}),
    ("Maio_Vendas",                   "match", 18, None, {"cliente": 0, "projeto": 4, "numero": 5, "valor": 8, "data": 9}),
    ("Junho_MATCH",                   "match", 16, None, {"cliente": 0, "projeto": 4, "numero": 5, "valor": 6, "data": 7}),
    ("Julho_MATCH",                   "match", 17, 16, {"cliente": 0, "projeto": 4, "numero": 5, "valor": 6, "data": 7}),
    ("Agosto_MATCH",                  "match", 17, 16, {"cliente": 0, "projeto": 4, "numero": 5, "valor": 6, "data": 7}),
    ("Maio_Elaboração de Projetos",   "elab",  11, None, {"cliente": 0, "data": 1, "valor": 3, "lei": 4}),
    ("Junho_Elaboração de Projetos",  "elab",  12, None, {"cliente": 0, "data": 1, "valor": 4, "lei": 5}),
    ("Julho_Elaboração de Projetos",  "elab",  13, 12, {"cliente": 0, "data": 1, "valor": 4, "lei": 5}),
    ("Agosto_Elaboração de Projetos", "elab",  13, 12, {"cliente": 0, "data": 1, "valor": 4, "lei": 5}),
]


def candidatos_match(linha, ch, universo):
    """MATCH: numero do projeto e valor sao a chave forte; nome e data corroboram."""
    numero = text_id(linha[ch["numero"]])
    valor = money(linha[ch["valor"]])
    data = data_da_celula(linha[ch["data"]])
    nome = str(linha[ch["cliente"]]).strip()

    fortes = [d for d in universo
              if numero and text_id(d.get("numero_projeto")) == numero
              and valor and money(d.get("valor_bruto")) == valor]
    if fortes:
        return fortes, "numero+valor"
    # sem numero, cai para nome + valor, que ainda e corroborado
    medios = [d for d in universo
              if valor and money(d.get("valor_bruto")) == valor
              and forca_do_nome(nome, d.get("cliente"))]
    if medios:
        return medios, "nome+valor"
    # ultimo recurso: nome + data. Nunca nome sozinho.
    if data:
        fracos = [d for d in universo
                  if parse_closedate(d.get("closedate", "")) == data
                  and forca_do_nome(nome, d.get("cliente"))]
        if fracos:
            return fracos, "nome+data"
    return [], ""


def candidatos_elab(linha, ch, universo):
    """Elaboracao: nome E data juntos, sempre. Lei so desempata."""
    nome = str(linha[ch["cliente"]]).strip()
    data = data_da_celula(linha[ch["data"]])
    if not data:
        return [], ""
    por_nome = [d for d in universo
                if forca_do_nome(nome, d["prop"]) or forca_do_nome(nome, d["proj"])]
    hits = [d for d in por_nome if d["data"] == data]
    if len(hits) > 1:
        lei = norm(str(linha[ch["lei"]]).replace("Lei ", ""))
        por_lei = [d for d in hits if lei and d["lei"] == lei]
        if len(por_lei) == 1:
            return por_lei, "nome+data+lei"
    if hits:
        return hits, "nome+data"
    # Nome bate e data nao. Isso NAO e orfa: "achei um, mas a data diverge" e uma
    # informacao melhor que "nao achei", e as vezes e a data do HubSpot que esta
    # errada. Medido em 19/08: "Centro de Fortalecimento Maria Madalena" casa por
    # nome com um negocio que esta SEM closedate, que ja era pendencia conhecida.
    # Volta como candidato, mas o criterio garante que nunca vira ALTA.
    return por_nome, "nome, data diverge"


CRITERIOS_SEM_CORROBORACAO = {"nome, data diverge"}


def forca_do_casamento(linha, ch, deal, universo_tipo, criterio):
    """ALTA so quando a chave e forte E o nome nao e de um token so."""
    if criterio in CRITERIOS_SEM_CORROBORACAO:
        return "MEDIA"
    if criterio == "numero+valor":
        return "ALTA"
    nome = str(linha[ch["cliente"]]).strip()
    alvo = deal.get("cliente") if universo_tipo == "match" else deal["prop"]
    f = forca_do_nome(nome, alvo) or forca_do_nome(nome, deal.get("proj", ""))
    return "ALTA" if f in ("exato", "forte") else "MEDIA"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--aba", action="append", help="limita a estas abas (repetir)")
    ap.add_argument("--com-busca", action="store_true",
                    help="tambem grava o link de BUSCA nas linhas orfas. Fora por padrao: o "
                         "formato da URL de lista do HubSpot nao da para validar por API, e "
                         "link morto numa planilha de folha e pior que celula vazia. Ligar "
                         "depois de abrir um no navegador.")
    ap.add_argument("--sheet-id", default=OFICIAL_ID_DEFAULT)
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    gc = get_sheets_client()
    rows, _ts = com_retry(load_consolidado, gc)
    match_won = select_match_won(rows)

    tok = load_hubspot_token()
    elab_deals = search_elaboracao_won(tok)
    resolver_proponentes(elab_deals, tok)
    elab = [{"deal_id": d["id"], "prop": _proponente(d["properties"]),
             "proj": (d["properties"].get("nome_do_proponente") or "").strip(),
             "data": parse_closedate(d["properties"].get("closedate")),
             "lei": norm(d["properties"].get("lei_principal"))} for d in elab_deals]
    print(f"universo: {len(match_won)} Match ganho | {len(elab)} Elaboracao ganho\n")

    sh = com_retry(gc.open_by_key, args.sheet_id)
    alvos = [a for a in ABAS if not args.aba or a[0] in args.aba]
    total = {"ALTA": 0, "MEDIA": 0, "AMBIGUA": 0, "ORFA": 0, "JA_TEM": 0}
    escritas = 0

    for aba, tipo, col_link, col_tech, ch in alvos:
        try:
            vals = com_retry(sh.values_get, f"'{aba}'!A1:BZ3000",
                             params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])
        except Exception as erro:
            print(f"[pula] {aba}: {str(erro)[:100]}")
            continue
        hdr = [str(c).strip() for c in (vals[0] if vals else [])]

        # A posicao de cada aba esta na tabela ABAS porque o layout varia demais para
        # uma formula ("ultima + 1" quebra na aba da Bia, cuja coluna tecnica fica em
        # AC, sete colunas depois do bloco visivel). O que se verifica aqui e a
        # propriedade que importa: a celula esta LIVRE. Nunca sobrescrever coluna de
        # ninguem numa planilha de folha.
        atual = str(hdr[col_link]).strip() if col_link < len(hdr) else ""
        if atual and atual != HEADER_LINK:
            print(f"[ABORT] {aba}: {rowcol_to_a1(1, col_link+1).rstrip('1')} ja tem "
                  f"{atual!r}. Conferir o layout antes de escrever.")
            continue
        if col_tech is not None and col_link == col_tech:
            print(f"[ABORT] {aba}: coluna do link colide com a tecnica.")
            continue

        universo = match_won if tipo == "match" else elab
        pendentes, linhas_relat = [], []
        for n, raw in enumerate(vals[1:], start=2):
            linha = list(raw) + [""] * 60
            nome = str(linha[ch["cliente"]]).strip()
            if not linha_e_dado(nome):
                continue
            if str(linha[col_link]).strip():
                total["JA_TEM"] += 1
                continue
            did = str(linha[col_tech]).strip() if col_tech is not None else ""
            if did:
                pendentes.append((n, link_negocio(did)))
                total["ALTA"] += 1
                linhas_relat.append(("ALTA", n, nome, f"deal {did} (id na aba)"))
                continue
            cands, criterio = (candidatos_match if tipo == "match" else candidatos_elab)(linha, ch, universo)
            if not cands:
                total["ORFA"] += 1
                if args.com_busca:
                    pendentes.append((n, link_busca(nome)))
                    linhas_relat.append(("ORFA", n, nome, "sem candidato -> link de busca"))
                else:
                    linhas_relat.append(("ORFA", n, nome,
                                         "sem candidato (link de busca segurado, use --com-busca)"))
            elif len(cands) > 1:
                total["AMBIGUA"] += 1
                ids = [c.get("deal_id") for c in cands][:4]
                linhas_relat.append(("AMBIGUA", n, nome, f"{len(cands)} candidatos: {ids}"))
            else:
                d = cands[0]
                nivel = forca_do_casamento(linha, ch, d, tipo, criterio)
                total[nivel] += 1
                rot = f"deal {d['deal_id']} por {criterio}"
                if nivel == "MEDIA":
                    rot += "  <- conferir antes de usar"
                if nivel == "ALTA":
                    pendentes.append((n, link_negocio(d["deal_id"])))
                linhas_relat.append((nivel, n, nome, rot))

        print("=" * 112)
        cont = {}
        for niv, *_ in linhas_relat:
            cont[niv] = cont.get(niv, 0) + 1
        print(f"{aba} | coluna {rowcol_to_a1(1, col_link+1).rstrip('1')} | " +
              " ".join(f"{k}={v}" for k, v in sorted(cont.items())) or "(nada)")
        for niv, n, nome, det in linhas_relat:
            if niv in ("MEDIA", "AMBIGUA") or niv == "ORFA":
                print(f"  [{niv:<7}] L{n:<3} {nome[:38]:<40} {det}")
        if not args.write or not pendentes:
            continue
        ws = com_retry(sh.worksheet, aba)
        if hdr[col_link:col_link + 1] != [HEADER_LINK]:
            com_retry(ws.update, values=[[HEADER_LINK]], range_name=rowcol_to_a1(1, col_link + 1),
                      value_input_option="USER_ENTERED")
        com_retry(ws.batch_update, [{"range": rowcol_to_a1(n, col_link + 1), "values": [[url]]}
                                    for n, url in pendentes], value_input_option="USER_ENTERED")
        escritas += len(pendentes)
        print(f"  [write] {len(pendentes)} link(s) gravado(s).")

    print("=" * 112)
    print("TOTAL " + " | ".join(f"{k}={v}" for k, v in total.items()))
    grav = total["ALTA"] + (total["ORFA"] if args.com_busca else 0)
    print(f"gravaveis: {grav} | para revisao (MEDIA + AMBIGUA): {total['MEDIA'] + total['AMBIGUA']}")
    if not args.com_busca and total["ORFA"]:
        exemplo = link_busca("Nu Bank")
        print(f"orfas seguradas: {total['ORFA']}. Abra este link antes de liberar com --com-busca:")
        print(f"  {exemplo}")
    if not args.write:
        print("[dry-run] nada gravado. Use --write.")
    else:
        print(f"[write] {escritas} celula(s) escrita(s).")


if __name__ == "__main__":
    main()
