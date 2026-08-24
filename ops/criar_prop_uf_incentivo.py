# -*- coding: utf-8 -*-
"""Cria a property `uf_incentivo`: o UNICO dado que faltava para rotear a planilha do financeiro.

A planilha tem 6 abas, uma por enquadramento fiscal. A primeira ideia foi um campo de
"enquadramento" com as 6 abas como opcoes — e estava errada, porque duplicaria o que
`lei_principal` ja diz.

Medido em 24/08 sobre os 78 ganhos: **a lei deriva a linha de imposto em 41 de 46 casos**, e as
5 divergencias sao erro de digitacao (as cinco marcadas 'IR' contra o que a lei diz), nao regra.
`lei_principal` ja tem 'Lei do Bem' no enum, entao ate a aba propria dela sai de la.

O que a lei NAO diz e o estado. E so isso falta:

    lei de IR            -> aba IR                  (nao se divide por estado)
    Lei do Bem           -> aba LEI DO BEM          (e IR, mas o rito e outro)
    lei de ICMS + UF     -> ICMS RIO / ICMS SP
    lei de ISS  + UF     -> ISS RIO / ISS SP

So RJ e SP porque so essas duas tem aba. Se aparecer outro estado, o financeiro decide se cria
aba; ate la o roteador manda para revisao em vez de inventar destino.

A UF so importa para lei estadual e municipal. Em negocio de IR ela pode ficar vazia sem
prejuizo — vale lembrar disso antes de marcar como obrigatoria para todo mundo.

Dry-run por padrao. Idempotente.

Uso:
  python ops/criar_prop_uf_incentivo.py
  python ops/criar_prop_uf_incentivo.py --apply
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_financeiro import BASE, load_hubspot_token
import planilha_leis as pl

URL = f"{BASE}/crm/v3/properties/deals"
NOME = "uf_incentivo"

PROP = {
    "name": NOME,
    "label": "UF do incentivo",
    "type": "enumeration",
    "fieldType": "select",
    "groupName": "dealinformation",
    "formField": False,
    "description": ("Estado da lei de incentivo. So importa em lei estadual (ICMS) e municipal "
                    "(ISS), onde define o processo fiscal e a aba do financeiro. Em lei federal "
                    "(IR, Lei do Bem) pode ficar vazio."),
    "options": [{"label": uf, "value": uf, "displayOrder": i, "hidden": False}
                for i, uf in enumerate(pl.UFS)],
}


def _req(url, token, metodo="GET", payload=None):
    dados = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=dados, method=metodo, headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        corpo = r.read()
        return json.loads(corpo) if corpo else {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()

    try:
        atual = _req(f"{URL}/{NOME}", token)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise SystemExit(f"[abort] GET {NOME} -> {e.code}: {e.read()[:300]}")
        atual = None

    print(f"property   : {NOME}")
    print(f"label      : {PROP['label']}")
    print(f"tipo       : {PROP['type']} / {PROP['fieldType']}")
    print("opcoes     :")
    for o in PROP["options"]:
        print(f"               {o['value']!r}")
    print()

    if atual:
        tem = [o["value"] for o in atual.get("options", [])]
        quer = [o["value"] for o in PROP["options"]]
        print(f"[ja existe] opcoes no portal: {tem}")
        if tem == quer:
            print("[ok] identica ao esperado. Nada a fazer.")
        else:
            print(f"[DIVERGE] esperado: {quer}")
            print("          conferir a mao antes de mexer; nao altero opcao de campo em uso.")
        return

    if not args.apply:
        print("[dry-run] a property NAO existe. Use --apply para criar.")
        return

    criada = _req(URL, token, "POST", PROP)
    conferida = _req(f"{URL}/{NOME}", token)
    valores = [o["value"] for o in conferida.get("options", [])]
    ok = valores == [o["value"] for o in PROP["options"]]
    print(f"[apply] criada: {criada.get('name')}")
    print(f"        read-back: {valores}")
    print(f"        {'CONFIRMADO' if ok else 'FALHOU: opcoes divergem do enviado'}")
    if not ok:
        raise SystemExit("[abort] read-back nao confirmou.")
    print()
    print("FALTA, e nao da para fazer por API no plano Starter:")
    print("  1. marcar `uf_incentivo` como obrigatoria em 'Ganho - Incentivador'.")
    print("  2. marcar `lei_principal` tambem: hoje esta em 45 de 78, e e ela que carrega")
    print("     o imposto e a Lei do Bem. Campo derivado de campo pela metade da meia resposta.")
    print()
    print("  Sem trava, campo nasce e morre vazio — as 4 properties financeiras criadas em")
    print("  06/08 sem trava estao em 0 de 78.")


if __name__ == "__main__":
    main()
