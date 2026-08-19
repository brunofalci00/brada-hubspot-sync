# -*- coding: utf-8 -*-
"""Vincula uma empresa ja existente a negocios que estao sem empresa no HubSpot.

Por que existe: a coluna CNPJ da aba `Controle de Cobrancas - Bia` sai vazia quando o card nao tem
empresa vinculada. Nao e campo faltando, e vinculo faltando — sem associacao nao ha de onde tirar o
CNPJ. Conferido em 19/08: os 4 negocios da aba tinham ZERO associacao com company.

O que este script NAO faz, de proposito:

  - nao cria empresa. Cadastrar razao social e CNPJ e de quem conhece o cliente; um palpite aqui
    vira nota fiscal emitida para a pessoa juridica errada.
  - nao troca vinculo existente. Card que ja tem empresa e recusado, mesmo que a empresa pareca
    errada: trocar sem alguem olhar e como sobrescrever dado humano.
  - nao aceita alvo implicito. Deal e empresa entram por argumento, sempre. Assim o comando que
    rodou fica no historico dizendo exatamente o que foi tocado.

Uso:
  python ops/vincular_empresa_deal.py --empresa 54484127567 --deal 61244915521 --deal 61244912569
  python ops/vincular_empresa_deal.py --empresa 54484127567 --deal 61244915521 --write
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_financeiro import BASE, load_hubspot_token

PORTAL_ID = "50771078"


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


def empresas_do_deal(deal_id, token):
    r = _req(f"{BASE}/crm/v4/objects/deals/{deal_id}/associations/companies", token)
    return [str(x["toObjectId"]) for x in r.get("results", [])]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--empresa", required=True, help="id da company que JA existe no HubSpot")
    ap.add_argument("--deal", action="append", required=True, help="id do negocio (repetir por deal)")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()

    emp = _req(f"{BASE}/crm/v3/objects/companies/{args.empresa}?properties=name,cnpj", token)
    nome = (emp.get("properties") or {}).get("name") or ""
    cnpj = (emp.get("properties") or {}).get("cnpj") or ""
    if not nome:
        raise SystemExit(f"[abort] empresa {args.empresa} nao encontrada.")
    print(f"empresa alvo: {nome!r} | cnpj={cnpj!r} | id={args.empresa}")
    if not cnpj:
        print("  [aviso] a empresa nao tem CNPJ cadastrado: a coluna do CNPJ vai continuar vazia.")
    print()

    a_vincular, recusados = [], []
    for did in args.deal:
        d = _req(f"{BASE}/crm/v3/objects/deals/{did}?properties=dealname", token)
        dealname = (d.get("properties") or {}).get("dealname") or ""
        atuais = empresas_do_deal(did, token)
        if atuais:
            recusados.append((did, dealname, atuais))
        else:
            a_vincular.append((did, dealname))

    for did, dealname, atuais in recusados:
        print(f"[RECUSADO] {dealname[:44]:<46} deal {did}: ja tem empresa {atuais}. "
              "Trocar vinculo existente e decisao humana, este script nao faz.")
    for did, dealname in a_vincular:
        print(f"  + {dealname[:44]:<46} deal {did}")
    if not a_vincular:
        print("  (nada a vincular)")
        return
    if not args.write:
        print("\n[dry-run] nada gravado. Use --write.")
        return

    print()
    for did, dealname in a_vincular:
        _req(f"{BASE}/crm/v4/objects/deals/{did}/associations/default/companies/{args.empresa}",
             token, metodo="PUT")
        # read-back: so conta como feito o que a API confirma de volta
        depois = empresas_do_deal(did, token)
        ok = str(args.empresa) in depois
        print(f"[write] deal {did} -> empresa {args.empresa}: "
              f"{'CONFIRMADO' if ok else 'FALHOU (read-back nao viu o vinculo)'}"
              f" | https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{did}")
        if not ok:
            raise SystemExit("[abort] vinculo nao confirmado; parando antes de tocar nos demais.")


if __name__ == "__main__":
    main()
