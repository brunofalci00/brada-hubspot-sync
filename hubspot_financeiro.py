# -*- coding: utf-8 -*-
"""Leitura direta das 4 properties financeiras no HubSpot.

Por que este modulo existe: as properties `numero_contrato_financeiro`,
`documento_cobranca`, `condicoes_pagamento_financeiro` e
`numero_parcelas_financeiro` foram criadas em 06/08/2026, mas a alteracao do
`sync.py` que as levaria para a aba `consolidado` nunca foi deployada. Em
producao o consolidado tem 37 colunas e nao traz nenhuma das quatro.

A alternativa seria deployar o `sync.py` novo, que mexe no sync horario que
alimenta o Looker e tres dashboards. Ler as quatro direto do HubSpot, so para os
deals candidatos, entrega o mesmo dado sem tocar naquele caminho. Se um dia o
deploy acontecer, `load_consolidado` passa a trazer as colunas e este modulo
continua funcionando: ele sobrescreve com o valor do HubSpot, que e a fonte.

Fora daqui, nenhum modulo deste conjunto faz chamada de escrita ou leitura no
HubSpot: `financeiro_match_common` e proposital e integralmente offline.
"""
from __future__ import annotations

import os

import requests

BASE = "https://api.hubapi.com"
LOTE = 100          # limite do batch/read de deals
TIMEOUT = 30

FINANCE_FIELDS = (
    "numero_contrato_financeiro",
    "documento_cobranca",
    "condicoes_pagamento_financeiro",
    "numero_parcelas_financeiro",
)


def load_hubspot_token():
    """Token do env ou de ~/.brada-secrets/hubspot.env. Nunca literal no codigo."""
    tok = os.environ.get("HUBSPOT_TOKEN", "").strip()
    if tok:
        return tok
    for path in (os.path.expanduser("~/.brada-secrets/hubspot.env"),
                 r"C:\Users\bruno\.brada-secrets\hubspot.env"):
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line.startswith("HUBSPOT_TOKEN"):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("[abort] HUBSPOT_TOKEN nao encontrado (env nem ~/.brada-secrets/hubspot.env).")


def carregar_props_financeiras(deal_ids, token=None):
    """{deal_id: {property: valor}} para os ids pedidos.

    Deal que o HubSpot nao devolve (arquivado, id invalido) simplesmente nao
    aparece no retorno; quem chama trata como vazio.
    """
    ids = [str(i).strip() for i in deal_ids if str(i or "").strip()]
    if not ids:
        return {}
    token = token or load_hubspot_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    out = {}
    for i in range(0, len(ids), LOTE):
        lote = ids[i:i + LOTE]
        payload = {"properties": list(FINANCE_FIELDS),
                   "inputs": [{"id": did} for did in lote]}
        resp = requests.post(f"{BASE}/crm/v3/objects/deals/batch/read",
                             headers=headers, json=payload, timeout=TIMEOUT)
        # 207 = parcial (alguns ids nao existem); os que vieram sao validos.
        if resp.status_code not in (200, 207):
            raise SystemExit(f"[abort] HubSpot batch/read {resp.status_code}: {resp.text[:300]}")
        for item in resp.json().get("results", []):
            props = item.get("properties") or {}
            out[str(item["id"])] = {c: (props.get(c) or "") for c in FINANCE_FIELDS}
    return out


def carregar_cnpj_por_deal(deal_ids, token=None):
    """{deal_id: cnpj} da empresa vinculada a cada negocio.

    Usa o endpoint de ASSOCIACAO, nao a property `hs_primary_associated_company`.
    A property e calculada pelo HubSpot e pode demorar a refletir um vinculo
    recem-criado — que e exatamente o caso quando alguem acabou de ligar a empresa
    ao card para destravar a cobranca. A associacao responde na hora.

    Um GET por deal: o batch/read de associacoes nao aceita esse formato. Como a
    lista de candidatos e pequena (dezenas), o custo e irrelevante perto de errar
    o CNPJ de uma nota fiscal.
    """
    ids = [str(i).strip() for i in deal_ids if str(i or "").strip()]
    if not ids:
        return {}
    token = token or load_hubspot_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    empresa_do_deal = {}
    for did in ids:
        try:
            resp = requests.get(f"{BASE}/crm/v4/objects/deals/{did}/associations/companies",
                                headers=headers, timeout=TIMEOUT)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        for item in resp.json().get("results", []):
            empresa_do_deal[did] = str(item["toObjectId"])
            break          # a primeira e a primaria; deal com 2 empresas e caso raro e manual

    alvo = sorted(set(empresa_do_deal.values()))
    cnpjs = {}
    for i in range(0, len(alvo), LOTE):
        payload = {"properties": ["cnpj"], "inputs": [{"id": c} for c in alvo[i:i + LOTE]]}
        resp = requests.post(f"{BASE}/crm/v3/objects/companies/batch/read",
                             headers=headers, json=payload, timeout=TIMEOUT)
        if resp.status_code not in (200, 207):
            raise SystemExit(f"[abort] HubSpot companies batch/read {resp.status_code}: {resp.text[:300]}")
        for item in resp.json().get("results", []):
            cnpjs[str(item["id"])] = (item["properties"].get("cnpj") or "").strip()

    return {did: cnpjs.get(cid, "") for did, cid in empresa_do_deal.items()}


def enriquecer(deals, token=None):
    """Preenche as 4 chaves financeiras em cada dict de deal, a partir do HubSpot.

    Sobrescreve o que veio do consolidado de proposito: o consolidado pode estar
    ate uma hora atrasado, o HubSpot e a fonte. Devolve quantos deals ficaram com
    pelo menos uma das quatro preenchidas, para o relatorio de completude.
    """
    if not deals:
        return 0
    ids = [d.get("deal_id") for d in deals]
    mapa = carregar_props_financeiras(ids, token=token)
    # CNPJ so vai atras do HubSpot para quem esta sem ele no consolidado. O
    # consolidado e reescrito de hora em hora: quem acabou de ganhar empresa
    # vinculada ficaria com a coluna vazia ate o proximo sync.
    faltando = [d.get("deal_id") for d in deals if not str(d.get("cnpj") or "").strip()]
    cnpjs = carregar_cnpj_por_deal(faltando, token=token) if faltando else {}
    com_dado = 0
    for d in deals:
        did = str(d.get("deal_id", "")).strip()
        vals = mapa.get(did, {})
        for campo in FINANCE_FIELDS:
            d[campo] = vals.get(campo, "")
        if not str(d.get("cnpj") or "").strip() and cnpjs.get(did):
            d["cnpj"] = cnpjs[did]
        if any(str(d[c]).strip() for c in FINANCE_FIELDS):
            com_dado += 1
    return com_dado
