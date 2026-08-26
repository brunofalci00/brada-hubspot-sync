# -*- coding: utf-8 -*-
"""Aplica no HubSpot a condicao de pagamento que o Ricardo confirmou em contrato.

Em 19/08 o Ricardo respondeu a validacao dos projetos de Elaboracao citando a clausula de cada
contrato. Os tres eram os que estavam sem `condicao_de_pagamento`, e os tres sao negocios dele:

  "I - o percentual de 10% (dez por cento) sobre o montante total efetivamente captado pelo
   projeto, a titulo de remuneracao pelos servicos de Elaboracao de Projetos, Captacao de
   Recursos e Prestacao de Contas."

Isso e exatamente a opcao `10% vr captado` que ja existe no picklist. Nao ha interpretacao aqui:
e transcrever a resposta escrita do dono do negocio.

O que este script NAO faz: nao mexe em nome de projeto. O Ricardo tambem apontou dois nomes
diferentes do contrato ("Carioca Matsuri I", "Basquete Santos 2027"), mas esses dois negocios
pertencem a uma leva de 22/06 sem dono e sem empresa vinculada, que parece duplicata de negocios
que ja existem. Renomear duplicata suspeita e consolidar o erro.

Uso:
  python ops/aplicar_resposta_ricardo.py
  python ops/aplicar_resposta_ricardo.py --write
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

# deal -> {property: valor}, com o rotulo que o Ricardo usou para se referir a cada um
RESPOSTAS = {
    "61024593686": ("À LA BANGU (projeto Pianópolis)",
                    {"condicao_de_pagamento": "10% vr captado", "lei_principal": "Rouanet"}),
    "60984794789": ("CEMAFER PRODUÇÕES LTDA",
                    {"condicao_de_pagamento": "10% vr captado"}),
    "60177864736": ("ASSOCIAÇÃO NEURONUTRE",
                    {"condicao_de_pagamento": "10% vr captado"}),
}


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
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    token = load_hubspot_token()
    campos = sorted({c for _r, v in RESPOSTAS.values() for c in v})
    body = {"properties": ["dealname", "nome_do_proponente"] + campos,
            "inputs": [{"id": i} for i in RESPOSTAS]}
    atual = {str(x["id"]): x["properties"]
             for x in _req(f"{BASE}/crm/v3/objects/deals/batch/read", token, "POST", body)["results"]}

    plano = []
    for did, (rotulo, novos) in RESPOSTAS.items():
        p = atual.get(did, {})
        for campo, valor in novos.items():
            tem = (p.get(campo) or "").strip()
            if tem == valor:
                print(f"  [ja ok]  {rotulo[:34]:<36} {campo} = {valor!r}")
            elif tem:
                # Nunca sobrescrever resposta que ja existe: se divergir, e conversa,
                # nao script.
                print(f"  [PULADO] {rotulo[:34]:<36} {campo} ja tem {tem!r}, "
                      f"o Ricardo disse {valor!r}. Conferir a mao.")
            else:
                plano.append((did, rotulo, campo, valor))
                print(f"  + {rotulo[:34]:<36} {campo} = {valor!r}")

    if not plano:
        print("\n(nada a aplicar)")
        return
    if not args.write:
        print(f"\n[dry-run] {len(plano)} campo(s). Use --write.")
        return

    print()
    por_deal = {}
    for did, _rot, campo, valor in plano:
        por_deal.setdefault(did, {})[campo] = valor
    for did, props in por_deal.items():
        _req(f"{BASE}/crm/v3/objects/deals/{did}", token, "PATCH", {"properties": props})
        depois = _req(f"{BASE}/crm/v3/objects/deals/{did}?properties=" + ",".join(props), token)
        ok = all((depois["properties"].get(c) or "") == v for c, v in props.items())
        print(f"[write] deal {did}: {'CONFIRMADO' if ok else 'FALHOU no read-back'} "
              f"{props} | {deal_link({'deal_id': did})}")
        if not ok:
            raise SystemExit("[abort] read-back nao confirmou; parando.")


if __name__ == "__main__":
    main()
