# -*- coding: utf-8 -*-
"""Move negocio do funil Incentivador para 'Ganho - Incentivador', preservando o closedate.

Existe por causa de um comportamento do HubSpot que ja mordeu antes: ao entrar num estagio
closed-won, ele REESCREVE o closedate com a data de hoje. Num fecho por ciclo 21->20 isso troca
silenciosamente o mes em que a venda conta. Por isso o PATCH e em dois passos, com read-back
entre eles, e a data original volta.

Nao decide nada. O criterio de "isso e ganho?" e do comercial; este script so executa a decisao
depois de tomada, e prova que executou.

Uso:
  python ops/mover_para_ganho.py --deal 64058925378
  python ops/mover_para_ganho.py --deal 64058925378 --write
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_financeiro import BASE, load_hubspot_token
from financeiro_match_common import deal_link

GANHO_INCENTIVADOR = "1253324968"
PROPS = ["dealname", "dealstage", "closedate", "amount", "valor_do_aporte", "data_do_aporte",
         "hs_is_closed_won", "tipo_de_proponente", "percentual_brada", "numero_do_projeto"]


def _req(url, token, metodo="GET", payload=None):
    dados = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=dados, method=metodo, headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            corpo = r.read()
            return json.loads(corpo) if corpo else {}
    except urllib.error.HTTPError as e:
        raise SystemExit(f"[abort] {metodo} {url.split('/crm')[-1]} -> {e.code}: {e.read()[:300]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deal", action="append", required=True, help="id do negocio (repetivel)")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()
    plano = []
    for did in args.deal:
        p = _req(f"{BASE}/crm/v3/objects/deals/{did}?properties=" + ",".join(PROPS), token)["properties"]
        print("=" * 100)
        print(f"{did}  {p.get('dealname')}")
        print(f"  estagio atual   : {p.get('dealstage')}  (won={p.get('hs_is_closed_won')})")
        print(f"  closedate       : {(p.get('closedate') or '')[:10]}")
        print(f"  amount          : {p.get('amount')}")
        print(f"  valor_do_aporte : {p.get('valor_do_aporte')}")
        print(f"  data_do_aporte  : {p.get('data_do_aporte') or '(VAZIO)'}")
        print(f"  tipo_proponente : {p.get('tipo_de_proponente')}  |  percentual_brada: "
              f"{p.get('percentual_brada')}")
        if p.get("dealstage") == GANHO_INCENTIVADOR:
            print("  [ja esta em Ganho, nada a fazer]")
            continue
        if not (p.get("data_do_aporte") or "").strip():
            # Nao bloqueia: a decisao de ganho e do comercial. Mas a coluna "Data do aporte"
            # do {Mes}_MATCH sai vazia no relatorio que vai ao financeiro.
            print("  [ATENCAO] sem data_do_aporte: a coluna 'Data do aporte' do relatorio sai vazia")
        plano.append((did, p.get("closedate")))
        print(f"  -> mover para Ganho - Incentivador, preservando closedate "
              f"{(p.get('closedate') or '')[:10]}")

    print("=" * 100)
    if not plano:
        print("(nada a mover)")
        return
    if not args.write:
        print(f"[dry-run] {len(plano)} negocio(s). Use --write.")
        return

    for did, closedate_original in plano:
        # passo 1: o estagio. O HubSpot pode reescrever o closedate aqui.
        _req(f"{BASE}/crm/v3/objects/deals/{did}", token, "PATCH",
             {"properties": {"dealstage": GANHO_INCENTIVADOR}})
        meio = _req(f"{BASE}/crm/v3/objects/deals/{did}?properties=dealstage,closedate,"
                    f"hs_is_closed_won", token)["properties"]
        reescreveu = (meio.get("closedate") or "")[:10] != (closedate_original or "")[:10]
        print(f"[write] {did} estagio -> Ganho | closedate "
              f"{'REESCRITO para ' + (meio.get('closedate') or '')[:10] if reescreveu else 'preservado'}")

        # passo 2: devolver a data original, se o HubSpot a trocou.
        if reescreveu and closedate_original:
            _req(f"{BASE}/crm/v3/objects/deals/{did}", token, "PATCH",
                 {"properties": {"closedate": closedate_original}})

        fim = _req(f"{BASE}/crm/v3/objects/deals/{did}?properties=dealstage,closedate,"
                   f"hs_is_closed_won", token)["properties"]
        ok = (fim.get("dealstage") == GANHO_INCENTIVADOR
              and (fim.get("closedate") or "")[:10] == (closedate_original or "")[:10])
        print(f"        read-back: estagio={fim.get('dealstage')} "
              f"closedate={(fim.get('closedate') or '')[:10]} won={fim.get('hs_is_closed_won')} "
              f"-> {'CONFIRMADO' if ok else 'FALHOU'}")
        print(f"        {deal_link({'deal_id': did})}")
        if not ok:
            raise SystemExit("[abort] read-back nao confirmou; parando antes do proximo.")


if __name__ == "__main__":
    main()
