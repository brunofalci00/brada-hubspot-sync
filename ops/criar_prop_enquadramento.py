# -*- coding: utf-8 -*-
"""Cria a property `enquadramento_fiscal`: o picklist que diz em qual aba a linha entra.

A planilha CLIENTES/MATCH/COMISSAO tem 6 abas, uma por enquadramento, porque cada lei tem rito
proprio de repasse e cobranca. Escolher a aba errada manda o negocio para o processo fiscal errado.

O HubSpot nao responde isso hoje: `linha_de_imposto_categoria` da IR/ISS/ICMS mas fica vazia em
41% dos ganhos, e **o estado nao existe em campo nenhum** — sendo que 4 das 6 abas separam por
estado. Medido em 20/08: das 78 vendas ganhas, so 29 tem destino; as duas abas de ICMS ficariam
vazias apesar de existirem 6 negocios de ICMS.

Por isso as opcoes sao exatamente os nomes das abas, e nao "imposto" + "UF" separados: o valor
escolhido JA E o destino, sem regra de derivacao no meio para errar.

E preenchimento UNICO, no fechamento. Nao e manutencao recorrente, entao nao esbarra na regra de
nao criar campo eterno para o comercial.

Dry-run por padrao. Idempotente: se ja existir, confere as opcoes e nao recria.

Uso:
  python ops/criar_prop_enquadramento.py
  python ops/criar_prop_enquadramento.py --apply
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
NOME = "enquadramento_fiscal"

# O valor tecnico e o rotulo sao o nome da aba. Manter identicos e proposital: qualquer
# tradução no meio e mais um lugar para divergir.
PROP = {
    "name": NOME,
    "label": "Enquadramento fiscal (aba do financeiro)",
    "type": "enumeration",
    "fieldType": "select",
    "groupName": "dealinformation",
    "formField": False,
    "description": ("Em qual aba da planilha do financeiro este negocio entra. Cada lei tem rito "
                    "proprio de repasse e cobranca, entao o enquadramento define o processo. "
                    "Preencher no fechamento."),
    "options": [{"label": aba.strip(), "value": aba.strip(), "displayOrder": i, "hidden": False}
                for i, aba in enumerate(pl.ABAS)],
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
    print("FALTA UMA COISA, e nao da para fazer por API no plano Starter:")
    print("  marcar como OBRIGATORIA para entrar em 'Ganho - Incentivador'.")
    print("  Sem a trava, o campo nasce e morre vazio — foi o que aconteceu com as 4 properties")
    print("  financeiras criadas em 06/08, que estao em 0 de 78.")


if __name__ == "__main__":
    main()
