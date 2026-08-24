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
from gspread.utils import rowcol_to_a1

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
         "uf_incentivo", "hs_v2_date_entered_current_stage"]

# Marco zero. A contabilizacao comeca aqui: o que fechou antes a Jaqueline sobe a mao, e a
# automacao nao toca. Sem o corte no CODIGO, e nao num acordo verbal, as duas brigam pela
# mesma linha.
MARCO_ZERO = "2026-08-24"

# R$ 700 fixo pelo servico de match, cobrado do patrocinador. Nao existe no HubSpot: e
# constante em todas as 6 abas hoje. Se um dia variar, vira campo.
VALOR_DO_MATCH = "700"


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
    """Quando o negocio entrou no estagio em que esta hoje. "" quando o HubSpot nao diz.

    Nao se usa `closedate` como marco: ele RETROAGE. Tres casos so nesta semana — um card criado
    em 14/07 com closedate de 07/07, outro criado em 17/08 com closedate do mesmo dia mas anterior
    a existir, e o proprio HubSpot reescrevendo o closedate ao mover para closed-won.

    Tambem NAO se usa `hs_date_entered_<id>`: essa property **nao existe** para estagio
    customizado, e o HubSpot ignora property inexistente em silencio — o filtro parecia funcionar
    e descartava tudo. So os 5 estagios nativos tem `hs_v2_date_entered_*` propria.

    `hs_v2_date_entered_current_stage` existe sempre e diz quando entrou no estagio ATUAL. Como
    a busca ja restringe aos estagios ganhos, e a data de entrada no ganho — com uma ressalva:
    num card que foi para Ganho e depois para pos-venda, e a data da pos-venda. Para um marco
    zero isso e aceitavel, porque erra para o lado de escrever MENOS.
    """
    return str(props.get("hs_v2_date_entered_current_stage") or "")[:10]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default=MARCO_ZERO,
                    help=f"marco zero AAAA-MM-DD (default {MARCO_ZERO})")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()
    deals = buscar_ganhos(token, PROPS)
    resolver_patrocinador(deals, token)

    if args.desde:
        antes = len(deals)
        # Sem data, NAO passa: falhar para o lado de nao escrever historico.
        sem_data = [d for d in deals if not entrou_no_ganho(d["properties"])]
        deals = [d for d in deals if entrou_no_ganho(d["properties"]) >= args.desde]
        print(f"marco zero {args.desde}: {len(deals)} de {antes} negocio(s) entraram no estagio "
              f"atual a partir dai")
        if sem_data:
            print(f"   ({len(sem_data)} sem data de entrada; ficam de fora por seguranca)")

    print("=" * 108)
    print(f"ROTEAMENTO — {len(deals)} negocio(s) ganho(s) | por lei_principal + uf_incentivo")
    print("=" * 108)

    por_conf = collections.Counter()
    por_aba = collections.Counter()
    pendentes = collections.defaultdict(list)
    for d in deals:
        p = d["properties"]
        aba, conf, motivo = pl.rotear_aba(p)
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
    print("QUEM NAO DECIDE (falta lei_principal ou uf_incentivo)")
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

    # ------------------------------------------------------------------ escrita
    gc = get_sheets_client()
    sh = gc.open_by_key(PLANILHA)
    ws_por_titulo = {w.title: w for w in sh.worksheets()}

    print()
    print("=" * 108)
    print("ESCRITA")
    print("=" * 108)
    total = 0
    for aba in pl.ABAS:
        alvo = pendentes.get(aba, [])
        if not alvo:
            continue
        ws = ws_por_titulo.get(aba)
        if ws is None:
            raise SystemExit(f"[abort] aba {aba!r} nao existe")

        linha_cab = pl.LAYOUT[aba]["linha_cabecalho"]
        vals = sh.values_get(f"'{aba}'!A1:CZ2000",
                             params={"valueRenderOption": "FORMATTED_VALUE"}).get("values", [])
        cab = vals[linha_cab - 1] if len(vals) >= linha_cab else []

        # Trava: a chave tem que estar na coluna A e o bloco do financeiro logo depois. Se
        # alguem inserir coluna no meio, abortar e melhor que escrever na coluna do vizinho.
        cab = list(cab) + [""] * 8
        if str(cab[pl.COL_DEAL_ID]).strip() != "deal_id":
            raise SystemExit(f"[abort] {aba!r}: coluna A deveria ser 'deal_id', esta "
                             f"{cab[pl.COL_DEAL_ID]!r}. Rodar ops/preparar_abas_planilha_leis.py")
        esperado_anc = "PATROCINADOR"
        achado_anc = str(cab[pl.pos(aba, pl.ANCORA)]).strip().upper()
        if achado_anc != esperado_anc:
            raise SystemExit(f"[abort] {aba!r}: coluna {pl.pos(aba, pl.ANCORA)} deveria ser "
                             f"{esperado_anc!r}, esta {achado_anc!r}. Layout mudou.")

        ja = {str((list(r) + [""])[pl.COL_DEAL_ID]).strip()
              for r in vals[linha_cab:] if r}
        novos = [(did, p) for did, p, _m in alvo if did not in ja]

        # Linha subida a mao nao tem deal_id, entao o dedup por chave nao a enxerga. O marco
        # zero protege do historico, mas nao de alguem ter subido ANTES de o card virar ganho —
        # foi o que aconteceu com o Galiotto em 24/08, e a automacao escreveu a segunda linha.
        # Nao apago linha de humano: aviso e deixo a conciliacao com quem sabe.
        col_proj = pl.pos(aba, "projeto") if "projeto" in pl.LAYOUT[aba]["cols"] else None
        manuais = [(n, list(r) + [""] * 80) for n, r in enumerate(vals[linha_cab:], start=linha_cab + 1)
                   if r and not str((list(r) + [""])[pl.COL_DEAL_ID]).strip()
                   and str((list(r) + [""] * 80)[pl.pos(aba, "patrocinador")]).strip()]
        for did, p in novos:
            proj = pl._norm(p.get("nome_do_projeto"))
            for n, m in manuais:
                if col_proj is None or not proj:
                    continue
                if pl._norm(m[col_proj]) == proj:
                    print(f"    [ATENCAO] a linha {n} ja tem o projeto {p.get('nome_do_projeto')!r} "
                          f"escrito a mao, sem deal_id. Vai ficar duplicado com o deal {did}.")
        print("")
        print(f"{aba:<14} destino: {len(alvo):>2} | ja na aba: "
              f"{len(alvo) - len(novos):>2} | NOVOS: {len(novos)}")
        for did, p in novos:
            print(f"    + {did}  {(p.get('_empresa_associada') or p.get('dealname') or '')[:44]}")
        if not novos or not args.write:
            continue

        primeira_livre = len(vals) + 1
        linhas = [pl.build_row({"properties": p}, aba, VALOR_DO_MATCH, deal_id=did)
                  for did, p in novos]
        largura = max(len(l) for l in linhas)
        fim = primeira_livre + len(linhas) - 1
        ws.update(values=[l + [""] * (largura - len(l)) for l in linhas],
                  range_name=f"A{primeira_livre}:{rowcol_to_a1(1, largura).rstrip('1')}{fim}",
                  value_input_option="USER_ENTERED")
        total += len(novos)
        print(f"    [write] {len(novos)} linha(s) em A{primeira_livre}:{fim}")

    print()
    print("=" * 108)
    print(f"TOTAL: {total} linha(s)")
    if not args.write:
        print("[dry-run] nada escrito. Use --write.")
    if por_conf["MEDIA"] or por_conf["ORFA"]:
        print(f"[atencao] {por_conf['MEDIA'] + por_conf['ORFA']} negocio(s) sem destino: "
              f"falta lei_principal ou uf_incentivo no card.")


if __name__ == "__main__":
    main()
