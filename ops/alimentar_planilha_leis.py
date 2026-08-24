# -*- coding: utf-8 -*-
"""Roteia os negocios ganhos para as 6 abas da planilha CLIENTES/MATCH/COMISSAO.

Dry-run por padrao. Hoje ele SO relata: faltam tres coisas do lado do processo para poder gravar,
e o proprio relatorio diz quais. Quando existirem, o mesmo script grava com --write, sem mudanca.

O relatorio e o entregavel: mostra, negocio a negocio, em qual aba ele cairia e por que. E o
numero que diz se vale criar o campo de enquadramento, em vez de argumento.

Uso:
  python ops/alimentar_planilha_leis.py
  python ops/alimentar_planilha_leis.py --desde 2026-08-20
"""
import argparse
import collections
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sync import get_sheets_client
from hubspot_financeiro import BASE, load_hubspot_token
from financeiro_match_common import deal_link
import planilha_leis as pl

PLANILHA = "1rd14NDGamfvDEolnLt96n95wypCxnbQYI3ARfELB7Vg"

# Ganho - Incentivador mais os 3 estagios de pos-venda, que tambem guardam negocio ganho
# (isClosed=false neles engana quem filtra por closed-won: sao 55 contra 78).
ESTAGIOS_GANHOS = ["1253324968", "contractsent", "1247329455", "1247329456"]
GANHO = "1253324968"

PROPS = ["dealname", "closedate", "amount", "valor_do_aporte", "percentual_brada",
         "tipo_de_proponente", "nome_do_projeto", "numero_do_projeto", "nome_do_proponente",
         "lei_principal", "linha_de_imposto_categoria", "nome_contato_proponente",
         "email_proponente", "telefone_proponente", "numero_parcelas_financeiro",
         f"hs_date_entered_{GANHO}"]

# O picklist que ainda nao existe. Quando for criado, basta preencher aqui.
CAMPO_ENQUADRAMENTO = "enquadramento_fiscal"


def _post(url, token, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST", headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"[abort] {url.split('/crm')[-1]} -> {e.code}: {e.read()[:300]}")


def _get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def buscar_ganhos(token, campos):
    todos, after = [], None
    while True:
        body = {"filterGroups": [{"filters": [
            {"propertyName": "dealstage", "operator": "IN", "values": ESTAGIOS_GANHOS}]}],
            "properties": campos, "limit": 100}
        if after:
            body["after"] = after
        r = _post(f"{BASE}/crm/v3/objects/deals/search", token, body)
        todos += r["results"]
        after = (r.get("paging") or {}).get("next", {}).get("after")
        if not after:
            return todos


def resolver_patrocinador(deals, token):
    """O PATROCINADOR da planilha e a empresa associada ao negocio, nao um campo de texto.

    Le pelo endpoint v4 de associacao, que e imediato, em vez da property calculada.
    """
    for d in deals:
        try:
            a = _get(f"{BASE}/crm/v4/objects/deals/{d['id']}/associations/companies", token)
            ids = [str(x["toObjectId"]) for x in a.get("results", [])]
        except Exception:
            ids = []
        d["properties"]["_empresa_associada"] = ""
        if ids:
            try:
                c = _get(f"{BASE}/crm/v3/objects/companies/{ids[0]}?properties=name", token)
                d["properties"]["_empresa_associada"] = (c["properties"].get("name") or "").strip()
            except Exception:
                pass


def entrou_no_ganho(props):
    """Quando o negocio ENTROU no estagio de ganho.

    Nao se usa `closedate` como marco: ele retroage. Ja apareceram dois casos esta semana — um
    card criado em 14/07 com closedate de 07/07 e outro criado em 17/08 com closedate do mesmo
    dia mas antes de existir. Data de entrada no estagio nao retroage.
    """
    return str(props.get(f"hs_date_entered_{GANHO}") or "")[:10]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default=None,
                    help="marco zero AAAA-MM-DD: so negocios que entraram no Ganho a partir daqui")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()
    campos = list(PROPS)
    # O picklist pode nao existir ainda; pedir property inexistente derruba a busca.
    try:
        _get(f"{BASE}/crm/v3/properties/deals/{CAMPO_ENQUADRAMENTO}", token)
        campos.append(CAMPO_ENQUADRAMENTO)
        tem_campo = True
    except Exception:
        tem_campo = False

    deals = buscar_ganhos(token, campos)
    resolver_patrocinador(deals, token)

    if args.desde:
        antes = len(deals)
        deals = [d for d in deals if entrou_no_ganho(d["properties"]) >= args.desde]
        print(f"marco zero {args.desde}: {len(deals)} de {antes} negocios entraram no Ganho depois")

    print("=" * 108)
    print(f"ROTEAMENTO — {len(deals)} negocio(s) ganho(s) | campo "
          f"'{CAMPO_ENQUADRAMENTO}' {'EXISTE' if tem_campo else 'NAO EXISTE ainda'}")
    print("=" * 108)

    por_conf = collections.Counter()
    por_aba = collections.Counter()
    pendentes = collections.defaultdict(list)
    for d in deals:
        p = d["properties"]
        aba, conf, motivo = pl.rotear_aba(p, p.get(CAMPO_ENQUADRAMENTO, "") if tem_campo else "")
        por_conf[conf] += 1
        if conf == "ALTA":
            por_aba[aba] += 1
            pendentes[aba].append((d["id"], p, motivo))
        else:
            pendentes[conf].append((d["id"], p, motivo))

    print()
    print("Por confianca:")
    for c in ("ALTA", "MEDIA", "ORFA"):
        print(f"   {c:<6} {por_conf[c]:>3}")
    print()
    print("Destino dos que decidem sozinhos:")
    for aba in pl.ABAS:
        print(f"   {aba:<14} {por_aba[aba]:>3}")

    print()
    print("-" * 108)
    print("QUEM NAO DECIDE (precisa do campo de enquadramento, ou de gente)")
    print("-" * 108)
    for conf in ("MEDIA", "ORFA"):
        for did, p, motivo in pendentes.get(conf, []):
            print(f"  [{conf:<5}] {(p.get('_empresa_associada') or p.get('dealname') or '')[:34]:<36} "
                  f"{motivo[:44]:<46} {deal_link({'deal_id': did})}")

    print()
    print("-" * 108)
    print("AMOSTRA DA LINHA QUE SERIA ESCRITA (3 primeiros de cada aba com destino)")
    print("-" * 108)
    for aba in pl.ABAS:
        itens = pendentes.get(aba, [])[:3]
        if not itens:
            continue
        cols = pl.LAYOUT[aba]["cols"]
        print(f"\n### {aba}  (cabecalho na linha {pl.LAYOUT[aba]['linha_cabecalho']})")
        for did, p, _m in itens:
            linha = pl.build_row({"properties": p}, aba, valor_match="700")
            cheias = {k: linha[i] for k, i in sorted(cols.items(), key=lambda kv: kv[1]) if linha[i]}
            print(f"  deal {did}")
            for k, v in cheias.items():
                print(f"      {k:<16} {str(v)[:44]!r}")

    print()
    print("=" * 108)
    print("O QUE FALTA PARA PODER GRAVAR")
    print("=" * 108)
    faltas = []
    if not tem_campo:
        faltas.append(f"1. property '{CAMPO_ENQUADRAMENTO}' (picklist com as 6 abas), obrigatoria "
                      f"para entrar no Ganho. Hoje {por_conf['MEDIA'] + por_conf['ORFA']} negocio(s) "
                      f"nao tem destino.")
    faltas.append("2. coluna tecnica de deal_id nas 6 abas, como PRIMEIRA coluna. Sem chave nao ha "
                  "dedup, e a segunda execucao duplica tudo.")
    faltas.append("3. marco zero combinado, para a automacao nao brigar com o backfill manual.")
    for f in faltas:
        print("  " + f)
    print()
    print("[dry-run] nada foi escrito na planilha. E nao da para escrever ainda: ver acima.")
    if args.write:
        raise SystemExit("[abort] --write recusado: sem coluna de deal_id nao ha dedup seguro.")


if __name__ == "__main__":
    main()
