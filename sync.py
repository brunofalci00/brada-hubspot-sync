"""
Sync HubSpot CRM -> Google Sheets
Puxa deals + companies da API HubSpot, enriquece com campos calculados
e escreve no Google Sheets. Roda via GitHub Actions (cron a cada hora)
ou manualmente.

Padrao: espelha a arquitetura do dashboard corridas (brada-tickets-sync).
"""

import datetime
import json
import os
import re
import time
from collections import Counter, defaultdict

import gspread
import requests
from google.oauth2.service_account import Credentials

# ===================================================
# CONFIG
# ===================================================

BASE = "https://api.hubapi.com"
PORTAL_ID = "50771078"

# Credenciais via env (GitHub Secrets) ou arquivo local
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
if not SERVICE_ACCOUNT_FILE:
    # Fallback hierárquico: env (CI/secret) > path prod (CI sem env) > path local (Bruno dev)
    _prod_path = os.path.join(os.path.dirname(__file__), "..", "service-account-key.json")
    _local_path = r"C:\Users\bruno\.brada-secrets\sheets-sa.json"
    SERVICE_ACCOUNT_FILE = _prod_path if os.path.exists(_prod_path) else _local_path

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

DEAL_PROPERTIES = [
    "dealname",
    "dealstage",
    "pipeline",
    "hubspot_owner_id",
    "valor_do_aporte",
    "valor_total_do_diagnostico",  # legado pre-migracao (escondido 05/05, mantido pra leitura historica)
    "valor_diagnostico_empresa",  # 05/05: espelho de Company.vtd, lider POR STAGE (cards COM filtro etapa)
    "valor_diagnostico_empresa_global",  # 05/05: espelho de Company.vtd, lider GLOBAL (cards SEM filtro etapa)
    "amount",  # 05/05: espelhado = valor_do_aporte pelo cron (header e kanban totalizers)
    "data_da_realizacao_do_diagnostico",
    "data_do_aporte",
    "executivo_responsavel",
    "executivo_match",  # OWNER do executivo Brada especifico no deal (E1)
    "trabalhado_por",  # "Executivo Brada" vs "Automatize" (E1) - chave remuneracao
    "motivo_de_perda",
    "nome_do_proponente",
    "tipo_de_proponente",  # 03/06: keystone interno/externo (grupo=interno / Externo=externo). Ver Modelo_Interno_Externo_Tipo_Proponente
    "nome_do_projeto",
    "numero_do_projeto",  # Sprint 1 consolidado: chave de projeto (linkagem cross-pipeline)
    "createdate",
    "closedate",
    "hs_lastmodifieddate",
    "hs_v2_date_entered_current_stage",  # v2 preenche pra deals criados no stage (v1 só quando move)
    "e_o_primeiro_match",
    "produto",
    "valor_oportunidade",
    "origem_lead",
    "status_contato",
    "email",      # custom Deal — e-mail do contato do incentivador (lista Rafaela 08/06)
    "telefone",   # custom Deal — telefone do contato (irmao de email)
    # Sprint C/D (12/06): contato do PROPONENTE no card (distinto do incentivador acima).
    # Properties criadas por ops/criar_props_contato_proponente.py (commit 72895cc).
    "nome_contato_proponente",
    "email_proponente",
    "telefone_proponente",
    "lei_principal",  # criado em E1 - puxa do HubSpot, argmax vira fallback
    "linha_de_imposto_categoria",  # criado em E1 (IR/ICMS/ISS)
    "cnpj_do_incentivador",  # criado em E1-bis - CNPJ da filial/PDV; vazio = fallback Company.cnpj
    # 11 campos de valor por lei (fonte de financiamento)
    "valor_lei_rouanet",
    "valor_lei_do_esporte",  # esporte federal
    "valor_lei_do_esporte_estadual",
    "valor_lei_do_bem",
    "valor_lei_da_cultura",  # cultura estadual
    "valor_lei_da_cultura_municipal",
    "valor_lei_da_crianca_e_do_adolescente",  # FIA
    "valor_lei_do_idoso",
    "valor_lei_da_reciclagem",
    "valor_pronas",
    "valor_pronon",
    # CRIAP (Sprint 0 / S0.4 — Caminho 1 reuso Proponente)
    "projeto_beneficiario_criap",
    "origem_deal_criap",
    "parceiro_indicador_criap",
    "parceiro_indicador_nome_criap",  # AUTO sync.py via sync_parceiro_nome_criap (Sprint 0.5 19/05)
    "parceiro_indicador_cnpj_criap",  # AUTO sync.py via sync_parceiro_cnpj_criap
    "pronac_criap",
]

# Map interno: property -> label legivel pra lei_principal
LEIS_MAP = {
    "valor_lei_rouanet": "Rouanet",
    "valor_lei_do_esporte": "Esporte Federal",
    "valor_lei_do_esporte_estadual": "Esporte Estadual",
    "valor_lei_do_bem": "Lei do Bem",
    "valor_lei_da_cultura": "Cultura Estadual",
    "valor_lei_da_cultura_municipal": "Cultura Municipal",
    "valor_lei_da_crianca_e_do_adolescente": "FIA (Crianca e Adolescente)",
    "valor_lei_do_idoso": "Fundo do Idoso",
    "valor_lei_da_reciclagem": "Reciclagem",
    "valor_pronas": "PRONAS",
    "valor_pronon": "PRONON",
}

# Map lei_principal (label) -> categoria de imposto (IR/ICMS/ISS).
# Usado como fallback quando Deal.linha_de_imposto_categoria nao esta preenchido.
LEI_TO_CATEGORIA = {
    "Rouanet": "IR",
    "Esporte Federal": "IR",
    "Lei do Bem": "IR",
    "FIA (Crianca e Adolescente)": "IR",
    "Fundo do Idoso": "IR",
    "PRONAS": "IR",
    "PRONON": "IR",
    "Esporte Estadual": "ICMS",
    "Cultura Estadual": "ICMS",
    "Reciclagem": "ICMS",
    "Cultura Municipal": "ISS",
}

# E1 criou picklists com value==label (setup_hubspot_fields.py usa _opts()
# que retorna {"value": l, "label": l}). Nao ha normalizacao lowercase a fazer
# nem no read nem no write.
# Descoberto em E2 (19/04): PATCHs do patch_derived_back estavam falhando
# silenciosamente desde 14/04 porque o mapa anterior forcava lowercase.
LEI_PICKLIST_VALUE_TO_LABEL = {lbl: lbl for lbl in LEIS_MAP.values()}
LEI_LABEL_TO_PICKLIST_VALUE = {lbl: lbl for lbl in LEIS_MAP.values()}

CATEGORIA_PICKLIST_VALUE_TO_LABEL = {"IR": "IR", "ICMS": "ICMS", "ISS": "ISS"}
CATEGORIA_LABEL_TO_PICKLIST_VALUE = {"IR": "IR", "ICMS": "ICMS", "ISS": "ISS"}

# NORMALIZACAO UF (E4, 22/04)
# Motivo: Company.state no HubSpot tem formatos mistos (BrasilAPI retorna sigla;
# preenchimento manual usa nome completo com/sem acento + typos reais vistos
# no Sheet 22/04: "Rio de Grande so Sul"). Normalizar antes de escrever no Sheet
# previne bar chart com 2+ barras para mesmo estado.
UF_SIGLAS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}
UF_NORMALIZE = {
    "acre": "AC",
    "alagoas": "AL",
    "amapa": "AP", "amapá": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceara": "CE", "ceará": "CE",
    "distrito federal": "DF",
    "espirito santo": "ES", "espírito santo": "ES",
    "goias": "GO", "goiás": "GO",
    "maranhao": "MA", "maranhão": "MA",
    "mato grosso": "MT",
    "mato grosso do sul": "MS",
    "minas gerais": "MG",
    "para": "PA", "pará": "PA",
    "paraiba": "PB", "paraíba": "PB",
    "parana": "PR", "paraná": "PR",
    "pernambuco": "PE",
    "piaui": "PI", "piauí": "PI",
    "rio de janeiro": "RJ",
    "rio grande do norte": "RN",
    "rio grande do sul": "RS",
    "rio de grande so sul": "RS",  # typo real visto no Sheet 22/04
    "rondonia": "RO", "rondônia": "RO",
    "roraima": "RR",
    "santa catarina": "SC",
    "sao paulo": "SP", "são paulo": "SP", "s. paulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}

# Normalizacao cosmetica do campo produto (value HubSpot -> label).
# Mantem output do Sheet consistente independente de preenchimento manual vs. inferencia.
# Pos E1 (value==label), HubSpot usa valores com acento/cedilha. Manter chaves lowercase
# como fallback pra valores legados eventuais, mas as chaves com acento sao as canonicas.
PRODUTO_PICKLIST_VALUE_TO_LABEL = {
    # Canonico pos-E1 (value==label)
    "Match": "Match",
    "Elaboração": "Elaboração",
    "AprovAI": "AprovAI",
    "Customização": "Customização",
    "Prestação": "Prestação",
    "Lei do bem": "Lei do bem",  # Ivan 04/05: produto novo
    "CRIAPE": "CRIAPE",  # F0.1 20/05: rename de "CRIAP" -> "CRIAPE" (value==label)
    # Legado lowercase (fallback)
    "match": "Match",
    "elaboracao": "Elaboração",
    "aprovai": "AprovAI",
    "customizacao": "Customização",
    "prestacao": "Prestação",
    "lei do bem": "Lei do bem",
    # Legacy CRIAP: deals migrados em F0.1 20/05, mas mantido aqui por 1 ciclo
    # de cron pra cobrir qualquer dado em voo. Remover pos-Sprint 1.
    "CRIAP": "CRIAPE",
}

COMPANY_PROPERTIES = [
    "name",
    "cnpj",
    "origem",
    "domain",
    "industry",
    "state",  # UF - auto-preenchido via BrasilAPI (Fase 4 27/04)
    "city",  # Municipio - auto-preenchido via BrasilAPI (Fase 4 27/04). NB: campo
             # `municipio` nao existe em Company (validado 27/04); usar `city`.
    "zip",  # CEP - auto-preenchido via BrasilAPI (Fase 4 27/04)
    "razao_social",
    # Diagnostico migrado Deal->Company (Fase 3 27/04). Source of truth pos-migracao
    # pro dashboard agregar via Company sem duplicar (1 row por empresa).
    "valor_total_do_diagnostico",
    "valor_lei_rouanet",
    "valor_lei_do_esporte",
    "valor_lei_do_esporte_estadual",
    "valor_lei_do_bem",
    "valor_lei_da_cultura",
    "valor_lei_da_cultura_municipal",
    "valor_lei_da_crianca_e_do_adolescente",
    "valor_lei_do_idoso",
    "valor_lei_da_reciclagem",
    "valor_pronas",
    "valor_pronon",
    # Diagnostico 2025 (16/06): separacao por ano. Os campos acima (unsuffixed) = ano
    # CORRENTE (2026); estes *_2025 = ano anterior, preenchidos via planilha do Ivan.
    # Grupo HubSpot `diagnostico_2025` (ver ops/criar_props_diagnostico_2025.py).
    "valor_total_do_diagnostico_2025",
    "valor_lei_rouanet_2025",
    "valor_lei_do_esporte_2025",
    "valor_lei_do_esporte_estadual_2025",
    "valor_lei_do_bem_2025",
    "valor_lei_da_cultura_2025",
    "valor_lei_da_cultura_municipal_2025",
    "valor_lei_da_crianca_e_do_adolescente_2025",
    "valor_lei_do_idoso_2025",
    "valor_lei_da_reciclagem_2025",
    "valor_pronas_2025",
    "valor_pronon_2025",
    # CRIAP (Sprint 0 / S0.4 — Caminho 1)
    "papel_criap",  # multi-select: patrocinador, parceiro_indicador
    "criap_total_aporte_2026",  # AUTO sync.py via compute_criap_rollups
    "criap_total_aporte_2025",  # AUTO (Sprint 1.5 27/05 — mitigação preventiva ajuste E closedate retroativo)
    "criap_count_negocios_ativos",  # AUTO
    "criap_count_negocios_ganhos",  # AUTO
    "criap_count_negocios_perdidos",  # AUTO (Sprint 0.5 19/05 — comparativo performance parceiro)
    "criap_projetos_apoiados_2026",  # AUTO (CSV)
    "criap_nomes_clientes_indicados",  # AUTO (Sprint 1.5 27/05 — pedido Ivan 26/05: ver clientes que a parceira indicou)
    "criap_aporte_por_cliente_2026",  # AUTO (Sprint 1.5 add-on 28/05 — agregado valor por cliente)
]

WORKSHEET_NAME = "raw_deals"


# ===================================================
# API HUBSPOT
# ===================================================

def req(method, path, **kwargs):
    """HTTP request com retry exponencial em 429."""
    url = f"{BASE}{path}"
    for attempt in range(3):
        r = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
        if r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        return r
    return r


def load_owner_map():
    """Retorna {owner_id: "Nome Sobrenome"} via /crm/v3/owners.

    Owner IDs sao usados como values em campos tipo owner-reference
    (hubspot_owner_id, executivo_match) e em selects legacy com IDs
    (executivo_responsavel). Sem este map, Looker mostra numero bruto.
    """
    owners = {}
    after = None
    while True:
        params = {"limit": 100}
        if after:
            params["after"] = after
        r = req("GET", "/crm/v3/owners", params=params)
        if r.status_code != 200:
            print(f"ERRO owners: {r.status_code} {r.text[:200]}")
            break
        data = r.json()
        for o in data.get("results", []):
            nome = f"{o.get('firstName', '')} {o.get('lastName', '')}".strip()
            if not nome:
                nome = o.get("email", "") or o.get("id", "")
            owners[o["id"]] = nome
        paging = data.get("paging", {}).get("next")
        if not paging:
            break
        after = paging.get("after")
    print(f"Owners carregados: {len(owners)}")
    return owners


def load_stages():
    """Retorna {stage_id: {nome, ordem, pipeline_id, pipeline_nome, probability, is_closed}}."""
    stages = {}
    pipeline_nomes = {"default": "Incentivador", "839644419": "Proponente"}
    for pipeline_id in ["default", "839644419"]:
        r = req("GET", f"/crm/v3/pipelines/deal/{pipeline_id}/stages")
        if r.status_code != 200:
            print(f"ERRO stages {pipeline_id}: {r.status_code}")
            continue
        for s in r.json().get("results", []):
            stages[s["id"]] = {
                "nome": s["label"],
                "ordem": s.get("displayOrder", 999),
                "pipeline_id": pipeline_id,
                "pipeline_nome": pipeline_nomes.get(pipeline_id, pipeline_id),
                "probability": s.get("metadata", {}).get("probability", ""),
                "is_closed": s.get("metadata", {}).get("isClosed", "false") == "true",
            }
    print(f"Stages carregados: {len(stages)}")
    return stages


def fetch_all_deals():
    """Puxa todos os deals via Search API paginada."""
    deals = []
    after = None
    while True:
        body = {
            "limit": 100,
            "properties": DEAL_PROPERTIES,
            "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        }
        if after:
            body["after"] = after
        r = req("POST", "/crm/v3/objects/deals/search", json=body)
        if r.status_code != 200:
            print(f"ERRO search deals: {r.status_code} {r.text[:300]}")
            break
        data = r.json()
        deals.extend(data.get("results", []))
        paging = data.get("paging", {}).get("next")
        if not paging:
            break
        after = paging.get("after")
    print(f"Deals puxados: {len(deals)}")
    return deals


def fetch_associated_companies(deal_ids):
    """Retorna {deal_id: primary_company_id}.

    Escolhe Company com typeId=5 (Primary) explicitamente. Sem isso,
    deals com multiplas companies (primary + secondary typeId=341)
    retornariam a errada quando batch/read v4 nao garante ordem por primary.

    Bug corrigido 24/05 (Bruno): antes usava tos[0] cego, causando
    deal_to_company errado em deals com parceiro indicador CRIAPE.
    Sintoma: cron sync_parceiro_associations_criap registrava
    `[BUG] tentou remover primary` em 14 deals.

    Fallback: tos[0] se nenhuma association marcada como primary
    (improvavel — todo deal HubSpot tem pelo menos 1 primary).
    """
    deal_to_company = {}
    for i in range(0, len(deal_ids), 100):
        batch = deal_ids[i:i + 100]
        r = req(
            "POST",
            "/crm/v4/associations/deals/companies/batch/read",
            json={"inputs": [{"id": did} for did in batch]},
        )
        if r.status_code not in (200, 207):
            print(f"ERRO assoc batch: {r.status_code}")
            continue
        for result in r.json().get("results", []):
            deal_id = result.get("from", {}).get("id")
            tos = result.get("to", [])
            if not (deal_id and tos):
                continue
            # Procurar primary explicitamente (typeId=5 HUBSPOT_DEFINED)
            primary_id = None
            for to in tos:
                types = to.get("associationTypes", []) or []
                if any(t.get("typeId") == 5 for t in types):
                    primary_id = to.get("toObjectId")
                    break
            # Fallback: tos[0] se nenhuma primary detectada (defensivo)
            deal_to_company[deal_id] = primary_id or tos[0].get("toObjectId")
    print(f"Associacoes deal->company: {len(deal_to_company)}")
    return deal_to_company


def fetch_companies(company_ids):
    """Retorna {company_id: {props}}."""
    companies = {}
    unique_ids = list({str(cid) for cid in company_ids if cid})
    for i in range(0, len(unique_ids), 100):
        batch = unique_ids[i:i + 100]
        r = req(
            "POST",
            "/crm/v3/objects/companies/batch/read",
            json={
                "properties": COMPANY_PROPERTIES,
                "inputs": [{"id": cid} for cid in batch],
            },
        )
        if r.status_code != 200:
            print(f"ERRO batch companies: {r.status_code}")
            continue
        for c in r.json().get("results", []):
            companies[c["id"]] = c.get("properties", {})
    print(f"Companies carregadas: {len(companies)}")
    return companies


def fetch_assoc(from_type, to_type, from_ids):
    """Retorna {str(from_id): [str(to_id), ...]} via associations v4 batch.

    Diferente de fetch_associated_companies: NAO filtra primary — pega TODOS os
    associados, pq no fallback de email/telefone interessa o primeiro contato com
    o campo preenchido (lista Rafaela 08/06). Usado pra deals->contacts e
    companies->contacts.
    """
    out = {}
    ids = list({str(i) for i in from_ids if i})
    for i in range(0, len(ids), 100):
        r = req(
            "POST",
            f"/crm/v4/associations/{from_type}/{to_type}/batch/read",
            json={"inputs": [{"id": x} for x in ids[i:i + 100]]},
        )
        if r.status_code not in (200, 207):
            print(f"ERRO assoc {from_type}->{to_type}: {r.status_code}")
            continue
        for res in r.json().get("results", []):
            out[str(res["from"]["id"])] = [
                str(t["toObjectId"]) for t in res.get("to", [])
            ]
    return out


def fetch_contacts(contact_ids):
    """Retorna {contact_id: {email, phone}}.

    Fonte de fallback do email/telefone do deal quando Deal.email/Deal.telefone
    estao vazios. Cobertura medida 08/06: Deal.email 34% -> +contato do deal 42%
    -> +contato da company 62% (lista Rafaela).
    """
    contacts = {}
    unique_ids = list({str(cid) for cid in contact_ids if cid})
    for i in range(0, len(unique_ids), 100):
        r = req(
            "POST",
            "/crm/v3/objects/contacts/batch/read",
            json={
                "properties": ["email", "phone"],
                "inputs": [{"id": cid} for cid in unique_ids[i:i + 100]],
            },
        )
        if r.status_code != 200:
            print(f"ERRO batch contacts: {r.status_code}")
            continue
        for c in r.json().get("results", []):
            contacts[c["id"]] = c.get("properties", {})
    print(f"Contacts carregados: {len(contacts)}")
    return contacts


def fetch_all_companies():
    """Puxa TODAS as Companies via Search API paginada (incluindo órfãs
    sem Deal associado). Usado pra popular aba raw_companies do Sheet.

    Diferença vs `fetch_companies(company_ids)`: esta retorna todas;
    aquela só as associadas a deals que já foram baixados.
    """
    companies = []
    after = None
    # createdate existe em toda Company — ordem estável entre páginas.
    properties = COMPANY_PROPERTIES + ["createdate"]
    while True:
        body = {
            "limit": 100,
            "properties": properties,
            "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        }
        if after:
            body["after"] = after
        r = req("POST", "/crm/v3/objects/companies/search", json=body)
        if r.status_code != 200:
            print(f"ERRO search companies: {r.status_code} {r.text[:300]}")
            break
        data = r.json()
        companies.extend(data.get("results", []))
        paging = data.get("paging", {}).get("next")
        if not paging:
            break
        after = paging.get("after")
    print(f"Companies totais carregadas: {len(companies)}")
    return companies


# ===================================================
# HELPERS
# ===================================================

def _parse_hs_datetime(s):
    """Converte datetime string do HubSpot (ISO com 'Z') em datetime aware."""
    if not s:
        return None
    try:
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_cnpj(s):
    """Remove pontuacao (pontos/tracos/barras/espacos) — retorna so digitos.

    Motivo: CNPJs no HubSpot estao em formatos mistos (ex: '35050782000158',
    '61.549812000185', '57.688.3920001-40'). Sem normalizar, comparacoes entre
    Deal.cnpj_do_incentivador e Company.cnpj dao falso-positivo de divergencia.
    """
    if not s:
        return ""
    return "".join(ch for ch in str(s) if ch.isdigit())


def resolve_cnpj(deal_props, company_props):
    """Retorna CNPJ efetivo preferindo Deal.cnpj_do_incentivador; fallback Company.cnpj.

    Helper do E3-bis (ata backlog Ivan 20/04, Gap A): clientes com multiplas
    filiais/PDVs (Casa do Alemao, Aquario, Cielo) precisam rastrear CNPJ especifico
    do deal, nao so o da matriz Company.
    """
    deal_cnpj = (deal_props.get("cnpj_do_incentivador") or "").strip()
    company_cnpj = (company_props.get("cnpj") or "").strip()
    return deal_cnpj or company_cnpj


def _normalize_uf(s):
    """Normaliza UF para sigla de 2 letras.

    Regras em ordem:
    1. Input vazio -> retorna ""
    2. Input ja e sigla UF valida (ex: "SP", "sp") -> uppercase
    3. Input e nome completo mapeavel (lowercase compared) -> sigla
    4. Nao mapeou -> retorna valor original preservando info

    Descoberto 22/04 montando Widget 2B (Receita por Estado): BrasilAPI popula
    Company.state com sigla; preenchimento manual dos executivos usa nome completo
    com/sem acento + typos. Sem normalizar, bar chart por UF no Looker mostra
    mesmo estado em multiplas barras.
    """
    if not s:
        return ""
    raw = str(s).strip()
    if not raw:
        return ""
    if raw.upper() in UF_SIGLAS:
        return raw.upper()
    return UF_NORMALIZE.get(raw.lower(), raw)


# ===================================================
# ENRIQUECIMENTO
# ===================================================

def enrich(deal, stages, deal_to_company, companies, owners=None,
           deal_to_contacts=None, company_to_contacts=None, contacts=None):
    p = deal.get("properties", {}) or {}
    deal_id = deal["id"]
    stage_id = p.get("dealstage") or ""
    stage_info = stages.get(stage_id, {})
    pipeline_nome = stage_info.get("pipeline_nome", "")
    stage_nome = stage_info.get("nome", stage_id)

    is_closed = stage_info.get("is_closed", False)
    prob = stage_info.get("probability", "")
    # Pós Venda stages são abertos no HubSpot (limitação: só 1 closed-won por pipeline)
    # mas representam deals já fechados — tratados como ganho para fins de revenue.
    # VENDIDO_POS_VENDA cobre Incentivador + Proponente (definido perto da linha 740).
    e_ganho = 1 if (is_closed and prob == "1.0") or stage_id in VENDIDO_POS_VENDA else 0
    e_perdido = 1 if (is_closed and prob == "0.0") else 0
    e_ativo = 1 if not is_closed and stage_id not in VENDIDO_POS_VENDA else 0

    def num(x):
        try:
            return float(x) if x not in (None, "") else 0.0
        except (ValueError, TypeError):
            return 0.0

    valor_aporte = num(p.get("valor_do_aporte"))
    valor_opp = num(p.get("valor_oportunidade"))
    valor_diagnostico = num(p.get("valor_total_do_diagnostico"))

    # Valor projetado: preferir valor_total_do_diagnostico (ja existe, 21% fill em Ganhos Inc)
    # Fallback: valor_oportunidade (campo novo, pos Ivan 14h)
    # Fallback final: valor_do_aporte (se deal sem diagnostico ainda)
    valor_projetado = valor_diagnostico or valor_opp or valor_aporte
    valor_projetado_ativo = valor_projetado if e_ativo else 0.0
    valor_vendido = valor_aporte if e_ganho else 0.0

    # Fonte de financiamento: preferir valor preenchido no HubSpot, fallback pra argmax dos 11 valor_lei_*
    leis_valores = {
        label: num(p.get(prop))
        for prop, label in LEIS_MAP.items()
        if num(p.get(prop)) > 0
    }
    lei_principal_derivada = (
        max(leis_valores.items(), key=lambda x: x[1])[0]
        if leis_valores else "(sem lei preenchida)"
    )
    lei_principal_hubspot_value = p.get("lei_principal") or ""
    lei_principal_hubspot_label = LEI_PICKLIST_VALUE_TO_LABEL.get(lei_principal_hubspot_value, "")
    lei_principal = lei_principal_hubspot_label or lei_principal_derivada
    leis_preenchidas = len(leis_valores)
    valor_total_por_lei = sum(leis_valores.values())

    # Categoria de imposto (IR/ICMS/ISS): preferir valor HubSpot, fallback pro mapa deterministico
    categoria_hubspot_value = p.get("linha_de_imposto_categoria") or ""
    categoria_hubspot_label = CATEGORIA_PICKLIST_VALUE_TO_LABEL.get(categoria_hubspot_value, "")
    categoria_derivada = (
        LEI_TO_CATEGORIA.get(lei_principal, "")
        if lei_principal != "(sem lei preenchida)" else ""
    )
    linha_de_imposto_categoria = categoria_hubspot_label or categoria_derivada or "(sem categoria)"

    def parse_dt(s):
        if not s:
            return None
        try:
            return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None

    createdate = parse_dt(p.get("createdate"))
    closedate = parse_dt(p.get("closedate"))
    now = datetime.datetime.now(datetime.timezone.utc)
    dias_desde_criacao = (now - createdate).days if createdate else None
    mes_criacao = createdate.strftime("%Y-%m") if createdate else ""
    ano_criacao = createdate.strftime("%Y") if createdate else ""
    mes_fechamento = closedate.strftime("%Y-%m") if closedate else ""
    ano_fechamento = closedate.strftime("%Y") if closedate else ""

    entered_stage = parse_dt(p.get("hs_v2_date_entered_current_stage"))
    dias_no_stage = (now - entered_stage).days if entered_stage else None

    company_id = deal_to_company.get(deal_id)
    comp = companies.get(str(company_id), {}) if company_id else {}

    # CNPJ efetivo (E3-bis): preferir Deal.cnpj_do_incentivador, fallback Company.cnpj.
    # Exporta bruto pra rastreabilidade e normalizado (so digitos) pro Looker agregar.
    cnpj_incentivador_bruto = (p.get("cnpj_do_incentivador") or "").strip()
    cnpj_efetivo_bruto = resolve_cnpj(p, comp)
    cnpj_efetivo_normalizado = _normalize_cnpj(cnpj_efetivo_bruto)

    produto_hubspot_value = p.get("produto") or ""
    produto_hubspot_label = PRODUTO_PICKLIST_VALUE_TO_LABEL.get(produto_hubspot_value, "")
    produto = produto_hubspot_label or ("Match" if pipeline_nome == "Incentivador" else "Elaboração")
    produto_foi_inferido = 0 if produto_hubspot_value else 1

    # Fase 9 (30/04): signatures pra deteccao de duplicatas.
    # Counters/severidade/keep_suggestion sao preenchidos no main() em 2a passada
    # (dependem de visao global de todos os deals).
    ano_close = closedate.strftime("%Y") if closedate else ""
    data_close = closedate.strftime("%Y-%m-%d") if closedate else ""
    lei_eff = lei_principal if lei_principal != "(sem lei preenchida)" else ""

    dup_sig_h1 = (f"{company_id}|{lei_eff}|{ano_close}"
                  if (e_ganho and company_id and lei_eff and ano_close) else "")
    dup_sig_h2 = (f"{company_id}|{int(valor_aporte)}|{data_close}"
                  if (valor_aporte > 0 and data_close and company_id) else "")
    # H2-flex (Fase 9.1): mesma company + mesmo valor + mesmo ANO (relaxa data exata).
    # Pega caso Nubank (Pedal Experience Mountain x OPeda, R$2.4M, datas proximas).
    dup_sig_h2flex = (f"{company_id}|{int(valor_aporte)}|{ano_close}"
                      if (valor_aporte > 0 and ano_close and company_id) else "")
    dup_sig_h3 = (f"{company_id}|{lei_eff}"
                  if (e_ativo and company_id and lei_eff) else "")
    # Etapa 1 (02/06): assinatura CRIAPE por projeto via empresa_canonica (CNPJ/nome normalizado).
    # Pega dups CRIAPE que h2/h2flex perdem quando closedate e nulo (ex.: SOTREQ). Review-only.
    emp_canon = _normalize_cnpj(comp.get("cnpj", "")) or _norm_key(comp.get("name", "") or "")
    proj_criap = (p.get("projeto_beneficiario_criap") or "").strip()
    dup_sig_criape = (f"{emp_canon}|{_norm_key(proj_criap)}|{int(valor_aporte)}"
                      if (produto == CRIAP_PRODUTO_VALUE and emp_canon and proj_criap and valor_aporte > 0) else "")

    dn_low = (p.get("dealname") or "").lower()
    dealname_clone_flag = 1 if any(
        t in dn_low for t in ["(clone)", "(copia)", "(copy)", "_copy", "(cópia)"]
    ) else 0

    # Email/telefone do incentivador (lista Rafaela 08/06): cadeia de 3 niveis —
    # campo do proprio Deal -> contato associado ao Deal -> contato associado a
    # Company. Cobertura medida: 34% -> 42% -> 62%.
    def _contact_field(field, deal_val):
        if deal_val:
            return deal_val
        for cid in (deal_to_contacts or {}).get(str(deal_id), []):
            v = (contacts or {}).get(cid, {}).get(field)
            if v:
                return v
        for cid in (company_to_contacts or {}).get(str(company_id), []):
            v = (contacts or {}).get(cid, {}).get(field)
            if v:
                return v
        return ""

    email_eff = _contact_field("email", (p.get("email") or "").strip())
    telefone_eff = _contact_field("phone", (p.get("telefone") or "").strip())

    return {
        "deal_id": deal_id,
        "deal_name": p.get("dealname", ""),
        "pipeline_id": stage_info.get("pipeline_id", ""),
        "pipeline_nome": pipeline_nome,
        "stage_id": stage_id,
        "stage_nome": stage_nome,
        "stage_ordem": stage_info.get("ordem", 999),
        "probability": prob,
        "is_closed": "sim" if is_closed else "nao",
        "e_ganho": e_ganho,
        "e_perdido": e_perdido,
        "e_ativo": e_ativo,
        "produto": produto,
        "produto_foi_inferido": produto_foi_inferido,
        # Valores
        "valor_do_aporte": valor_aporte,
        "valor_total_do_diagnostico": valor_diagnostico,
        "valor_oportunidade": valor_opp,
        "valor_projetado": valor_projetado,
        "valor_projetado_ativo": valor_projetado_ativo,
        "valor_vendido": valor_vendido,
        # Fonte de financiamento
        "lei_principal": lei_principal,
        "linha_de_imposto_categoria": linha_de_imposto_categoria,
        "leis_preenchidas": leis_preenchidas,
        "valor_total_por_lei": valor_total_por_lei,
        "valor_lei_rouanet": num(p.get("valor_lei_rouanet")),
        "valor_lei_do_esporte": num(p.get("valor_lei_do_esporte")),
        "valor_lei_do_esporte_estadual": num(p.get("valor_lei_do_esporte_estadual")),
        "valor_lei_do_bem": num(p.get("valor_lei_do_bem")),
        "valor_lei_da_cultura": num(p.get("valor_lei_da_cultura")),
        "valor_lei_da_cultura_municipal": num(p.get("valor_lei_da_cultura_municipal")),
        "valor_lei_da_crianca_e_do_adolescente": num(p.get("valor_lei_da_crianca_e_do_adolescente")),
        "valor_lei_do_idoso": num(p.get("valor_lei_do_idoso")),
        "valor_lei_da_reciclagem": num(p.get("valor_lei_da_reciclagem")),
        "valor_pronas": num(p.get("valor_pronas")),
        "valor_pronon": num(p.get("valor_pronon")),
        # Atribuicao
        "executivo_responsavel": p.get("executivo_responsavel", ""),
        "executivo_match": p.get("executivo_match", ""),
        "trabalhado_por": p.get("trabalhado_por", "") or "(em preenchimento)",
        "hubspot_owner_id": p.get("hubspot_owner_id", ""),
        # Nome resolvido via /crm/v3/owners - coluna canonica pra Looker filtrar
        # por executivo. Substitui os 3 campos acima no dashboard.
        "executivo_nome": (owners or {}).get(p.get("hubspot_owner_id", ""), "") or "(sem owner)",
        # Diagnostico/qualidade
        "motivo_de_perda": p.get("motivo_de_perda", "") or ("(sem motivo)" if e_perdido else ""),
        "origem_lead": p.get("origem_lead", "") or "(em preenchimento)",
        "status_contato": p.get("status_contato", "") or "(em preenchimento)",
        "e_o_primeiro_match": p.get("e_o_primeiro_match", ""),
        # Contato do incentivador (lista Rafaela 08/06): Deal.email/telefone com
        # fallback pro contato do deal e depois pro contato da company. ~62% fill.
        "email": email_eff,
        "telefone": telefone_eff,
        # Contexto
        "nome_do_proponente": p.get("nome_do_proponente", ""),
        # Sprint C/D: contato do proponente (distinto do email/telefone do incentivador acima)
        "nome_contato_proponente": p.get("nome_contato_proponente", ""),
        "email_proponente": p.get("email_proponente", ""),
        "telefone_proponente": p.get("telefone_proponente", ""),
        "tipo_de_proponente": p.get("tipo_de_proponente", ""),  # 03/06: deriva interno/externo
        "nome_do_projeto": p.get("nome_do_projeto", ""),
        "numero_do_projeto": p.get("numero_do_projeto", ""),  # Sprint 1: chave de projeto
        "projeto_beneficiario_criap": p.get("projeto_beneficiario_criap", ""),  # Sprint 1: projeto_key CRIAPE
        # Datas
        "createdate": p.get("createdate", ""),
        "closedate": p.get("closedate", ""),
        "data_da_realizacao_do_diagnostico": p.get("data_da_realizacao_do_diagnostico", ""),
        "data_do_aporte": p.get("data_do_aporte", ""),
        "ano_criacao": ano_criacao,
        "mes_criacao": mes_criacao,
        "ano_fechamento": ano_fechamento,
        "mes_fechamento": mes_fechamento,
        # CONTRATO LOOKER (datas): emitir AAAA-MM-DD date-only como insumo das
        # dimensoes de periodo. createdate/closedate em ISO viram Texto no Looker; e
        # coluna ESPARSA (ex. data_fechamento ~58% preenchida) tambem vira Texto e nao
        # aceita conversao manual. Por isso, no Looker, NUNCA amarrar o periodo na
        # coluna crua: criar campo calculado data_*_dt = PARSE_DATE("%Y-%m-%d", data_*)
        # e amarrar nele. Bind: vendido/fechado -> data_fechamento; pipeline/safra ->
        # data_criacao. Detalhe+checklist no vault: PLAYBOOK_datas_sheets_looker.
        # Guardrail: check_looker_contract.py (roda no CI apos a escrita).
        "data_criacao": createdate.strftime("%Y-%m-%d") if createdate else "",
        "data_fechamento": closedate.strftime("%Y-%m-%d") if closedate else "",
        "dias_desde_criacao": dias_desde_criacao if dias_desde_criacao is not None else "",
        "dias_no_stage_atual": dias_no_stage if dias_no_stage is not None else "",
        # Company
        "company_id": company_id or "",
        "company_name": comp.get("name", ""),
        "company_cnpj": comp.get("cnpj", ""),
        "cnpj_incentivador": cnpj_incentivador_bruto,  # Deal.cnpj_do_incentivador (bruto)
        "cnpj_efetivo": cnpj_efetivo_normalizado,  # resolvido + normalizado (so digitos) - usar no Looker
        "company_origem": comp.get("origem", ""),
        "company_industry": comp.get("industry", ""),
        "company_state": _normalize_uf(comp.get("state", "")) or "(em preenchimento)",
        "company_municipio": comp.get("city", ""),  # Fase 4 27/04: campo nativo HubSpot e `city` (nao `municipio`); preserva chave da Sheet pra compat Looker
        "company_razao_social": comp.get("razao_social", ""),
        # Fase 6 (28/04): espelho do diagnostico Company-level em cada deal pra
        # gap_diag/pct_ativos_com_diag/valor_prioridade do Quality testarem
        # Company-level sem precisar blend. Source of truth pos-migracao.
        "company_valor_total_do_diagnostico": num(comp.get("valor_total_do_diagnostico")),
        # Fase 9 (30/04): deteccao de duplicatas. Signatures + clone_flag aqui;
        # counts/severidade/keep_suggestion preenchidos em 2a passada no main().
        "dup_signature_h1": dup_sig_h1,
        "dup_signature_h2": dup_sig_h2,
        "dup_signature_h2flex": dup_sig_h2flex,
        "dup_signature_h3": dup_sig_h3,
        "dealname_clone_flag": dealname_clone_flag,
        "dup_count_h1": 0,
        "dup_count_h2": 0,
        "dup_count_h2flex": 0,
        "dup_count_h3": 0,
        "e_potencial_dup": 0,
        "dup_severity": "",
        "dup_keep_suggestion": "",
        # Link
        "link_hubspot": f"https://app.hubspot.com/contacts/{PORTAL_ID}/deal/{deal_id}",
        # Etapa 1 (02/06): assinatura/contador CRIAPE no FIM do dict (colunas no fim do raw_deals).
        "dup_signature_criape": dup_sig_criape,
        "dup_count_criape": 0,
    }


def enrich_company(company, num_deals_by_cid, flags_by_cid=None):
    """Monta dict canônico para aba raw_companies do Sheet.

    Inclui:
    - company_id, company_name
    - cnpj (cru) + cnpj_efetivo (so digitos — chave de agregação)
    - domain, industry, origem, razao_social, createdate
    - state (normalizado pra sigla UF), municipio
    - num_deals_vinculados — contagem de Deals com essa Company
    - Fase 6 (28/04): diagnostico Company-level (source of truth pos-migracao)
      + flags tem_deal_ativo/ganho/perdido pra widgets que filtram por estado do pipeline
    """
    p = company.get("properties", {}) or {}
    cid = company["id"]
    cnpj_raw = p.get("cnpj", "") or ""
    flags = (flags_by_cid or {}).get(str(cid), {})

    def num(x):
        try:
            return float(x) if x not in (None, "") else 0.0
        except (ValueError, TypeError):
            return 0.0

    return {
        "company_id": cid,
        "company_name": p.get("name", "") or "",
        "cnpj": cnpj_raw,
        "cnpj_efetivo": _normalize_cnpj(cnpj_raw),
        "domain": p.get("domain", "") or "",
        "industry": p.get("industry", "") or "",
        "state": _normalize_uf(p.get("state", "")) or "(em preenchimento)",
        "municipio": p.get("city", "") or "",  # Fase 4 27/04: ler `city`, manter chave `municipio` na Sheet
        "razao_social": p.get("razao_social", "") or "",
        "origem": p.get("origem", "") or "",
        "createdate": p.get("createdate", "") or "",
        "num_deals_vinculados": num_deals_by_cid.get(str(cid), 0),
        # Diagnostico Company-level (Fase 3 migrou de Deal -> Company; Fase 6 expoe na Sheet)
        "valor_total_do_diagnostico": num(p.get("valor_total_do_diagnostico")),
        "valor_lei_rouanet": num(p.get("valor_lei_rouanet")),
        "valor_lei_do_esporte": num(p.get("valor_lei_do_esporte")),
        "valor_lei_do_esporte_estadual": num(p.get("valor_lei_do_esporte_estadual")),
        "valor_lei_do_bem": num(p.get("valor_lei_do_bem")),
        "valor_lei_da_cultura": num(p.get("valor_lei_da_cultura")),
        "valor_lei_da_cultura_municipal": num(p.get("valor_lei_da_cultura_municipal")),
        "valor_lei_da_crianca_e_do_adolescente": num(p.get("valor_lei_da_crianca_e_do_adolescente")),
        "valor_lei_do_idoso": num(p.get("valor_lei_do_idoso")),
        "valor_lei_da_reciclagem": num(p.get("valor_lei_da_reciclagem")),
        "valor_pronas": num(p.get("valor_pronas")),
        "valor_pronon": num(p.get("valor_pronon")),
        # Diagnostico 2025 (16/06): ano anterior, no raw_companies pra dashboards de
        # referencia/comparativo. Unsuffixed acima = ano corrente (2026).
        "valor_total_do_diagnostico_2025": num(p.get("valor_total_do_diagnostico_2025")),
        "valor_lei_rouanet_2025": num(p.get("valor_lei_rouanet_2025")),
        "valor_lei_do_esporte_2025": num(p.get("valor_lei_do_esporte_2025")),
        "valor_lei_do_esporte_estadual_2025": num(p.get("valor_lei_do_esporte_estadual_2025")),
        "valor_lei_do_bem_2025": num(p.get("valor_lei_do_bem_2025")),
        "valor_lei_da_cultura_2025": num(p.get("valor_lei_da_cultura_2025")),
        "valor_lei_da_cultura_municipal_2025": num(p.get("valor_lei_da_cultura_municipal_2025")),
        "valor_lei_da_crianca_e_do_adolescente_2025": num(p.get("valor_lei_da_crianca_e_do_adolescente_2025")),
        "valor_lei_do_idoso_2025": num(p.get("valor_lei_do_idoso_2025")),
        "valor_lei_da_reciclagem_2025": num(p.get("valor_lei_da_reciclagem_2025")),
        "valor_pronas_2025": num(p.get("valor_pronas_2025")),
        "valor_pronon_2025": num(p.get("valor_pronon_2025")),
        # Flags de estado do pipeline (1 se Company tem >=1 deal naquele estado).
        # Usadas no Hero "PROJETADO" pra filtrar Companies com pipeline ativo.
        "tem_deal_ativo": 1 if flags.get("ativo", 0) > 0 else 0,
        "tem_deal_ganho": 1 if flags.get("ganho", 0) > 0 else 0,
        "tem_deal_perdido": 1 if flags.get("perdido", 0) > 0 else 0,
        # Etapa 1 (02/06): mesma formula do consolidado -> join consistente entre as duas abas.
        "empresa_canonica": _normalize_cnpj(cnpj_raw) or _norm_key(p.get("name", "") or ""),
    }


# ===================================================
# PATCH BACK (lei_principal / linha_de_imposto_categoria)
# ===================================================

POS_VENDA_STAGES = {"contractsent", "1247329455", "1247329456"}  # Pré Projeto, Projeto em Andamento, Pós Projeto (Incentivador)
# Proponente (Ivan 13/05): venda efetuada em "Fechado" (closed-won), Acompanhamento + Ganho
# são pós-venda (open mas já vendido — mesma lógica do POS_VENDA_STAGES do Incentivador).
PROPONENTE_POS_VENDA_STAGES = {"1246571363", "1253441207"}  # Acompanhamento, Ganho
# União de pós-venda dos dois pipelines (usado em enrich() pra e_ganho/e_ativo).
VENDIDO_POS_VENDA = POS_VENDA_STAGES | PROPONENTE_POS_VENDA_STAGES
# 1253324968 = Ganho Incentivador, 1246571362 = Fechado Proponente (closed-won dos dois pipelines)
STAGES_GANHO = {"1253324968", "1246571362"} | VENDIDO_POS_VENDA
PIPELINE_TO_PRODUTO = {"default": "Match", "839644419": "Elaboração"}  # value==label validado 22/04

# CRIAPE (Sprint 0 / S0.4 14/05 — Caminho 1 reuso pipeline Proponente)
# F0.1 20/05: value padronizado de "CRIAP" para "CRIAPE" (label sempre foi "CRIAPE").
# Constante mantem nome CRIAP_PRODUTO_VALUE pra evitar churn; o VALOR agora e "CRIAPE".
# Filtro CRIAPE em qualquer rollup: deal.pipeline == PROPONENTE_PIPELINE_ID AND deal.produto == CRIAP_PRODUTO_VALUE
PROPONENTE_PIPELINE_ID = "839644419"
CRIAP_PRODUTO_VALUE = "CRIAPE"     # ASCII puro
CRIAP_GANHO_STAGE_ID = "1246571362"   # = "Fechado" no Proponente (closed-won inicial, isClosed=true)
CRIAP_PERDIDO_STAGE_ID = "1246571364"  # = "Perdido" no Proponente
# Bruno 22/05: stages 1246571363 (Acompanhamento) e 1253441207 (Ganho/pos-venda) tambem sao
# "ganho comercial" — projeto ja vendido em fase de entrega. Rollup CRIAPE conta como ganho.
# Antes contava como ativo (bug — deal Eletromidia perderia status quando movesse pra pos-venda).
CRIAP_GANHO_STAGES = {CRIAP_GANHO_STAGE_ID} | PROPONENTE_POS_VENDA_STAGES

# ===== Comissionamento (Sprint 1 — consolidado + reporting Luciana) =====
# AVISO: constantes INFERIDAS da planilha do Ivan (poucas linhas). PENDENTE validação
# Ivan/Luciana antes de tratar como verdade — folha de pagamento depende disso.
# Fator 0.88 = (1 - 12%); o "12%" não tem rótulo na planilha (imposto? Simples?).
PCT_INTERNO = 0.15          # Comissão BRADA em venda interna
PCT_EXTERNO = 0.10          # Comissão BRADA em venda externa
FATOR_LIQUIDO = 0.88        # Líquido Brada = Comissão BRADA × (1 - 12%)
PCT_IVAN = 0.08             # Ivan = Líquido × 8%
PCT_JAQUE = 0.04            # Jaque = Líquido × 4%
PCT_EXTERNO_PESSOA = 0.03   # Externo = Líquido × 3%
MATCH_FIXO = {"ivan": 1000, "jaque_ou_danielle": 700, "rafaela": 200}  # MATCH fixo (participação manual)
# Estágios de conversão (Bruno 01/06): Incentivador conta conversão a partir do
# estágio "match"; Proponente a partir de "ganho" (= CRIAP_GANHO_STAGE_ID).
MATCH_STAGE_INCENTIVADOR_ID = "1246602643"   # "[Match] - Projetos" (não-closed)
GANHO_INCENTIVADOR_STAGE_ID = "1253324968"   # "Ganho - Incentivador" (closed-won)

# Auto-herança origem_lead <- Company.origem (decisao Bruno 23/04 tarde).
# Picklists unificados: os valores em PASSTHROUGH_VALUES existem nos dois campos
# (Deal.origem_lead e Company.origem) e podem ser propagados 1:1.
# Valor ambiguo "Linkedin / Whatsapp / Site" NAO entra aqui — Ivan/Bruno
# classificam caso a caso em `origem_lead` (pode ser LinkedIn, WhatsApp ou Site).
ORIGEM_LEAD_PASSTHROUGH = {
    "LinkedIn", "WhatsApp", "Site", "Feira/Evento",
    "Indicação Interna", "Indicação Externa",
    "Automatize direto", "DigiSAC (Proponente)", "Outros",
}


def _build_primeiro_match_map(raw_deals, deal_to_company):
    """Retorna {company_id: [(closedate, deal_id), ...]} ordenado por closedate asc.

    Pré-computa histórico de Ganhos por Company pra derivar e_o_primeiro_match
    sem queries extras. Deals sem closedate ou sem Company associada são ignorados.
    """
    by_company = defaultdict(list)
    for d in raw_deals:
        props = d.get("properties", {}) or {}
        if props.get("dealstage") not in STAGES_GANHO:
            continue
        cid = deal_to_company.get(d["id"])
        if not cid:
            continue
        closedate = _parse_hs_datetime(props.get("closedate", ""))
        if not closedate:
            continue
        by_company[cid].append((closedate, d["id"]))
    for cid in by_company:
        by_company[cid].sort()
    return by_company


def patch_derived_back(deals_enriched, raw_deals_by_id, deal_to_company=None,
                       primeiro_match_map=None, lookback_hours=2):
    """
    Captura movimento continuo do comercial: quando executivo muda valor_lei_X,
    a derivacao argmax/categoria no enrich() muda. Esta funcao propaga de volta
    pro HubSpot, so em deals modificados nas ultimas N horas (reduz blast radius
    e carga na API).

    Também aplica defaults (E6 Onda A):
    - produto: "Match"/"Elaboração" por pipeline (só se vazio)
    - e_o_primeiro_match: true/false por histórico Ganho da Company (só se null)

    Regras:
    - So faz PATCH se derivacao ≠ valor atual no HubSpot
    - Nunca sobrescreve valor existente com '(sem ...)' / vazio
    - Converte label interno ('Rouanet') pro picklist value do HubSpot ('rouanet')
    """
    deal_to_company = deal_to_company or {}
    primeiro_match_map = primeiro_match_map or {}
    agora = datetime.datetime.now(datetime.timezone.utc)
    cutoff = agora - datetime.timedelta(hours=lookback_hours)
    atualizados = 0
    erros = 0
    produto_defaults = 0
    primeiro_match_defaults = 0
    origem_lead_defaults = 0

    for enriched in deals_enriched:
        deal_id = enriched["deal_id"]
        raw = raw_deals_by_id.get(deal_id)
        if not raw:
            continue
        props = raw.get("properties", {}) or {}

        last_mod = _parse_hs_datetime(props.get("hs_lastmodifieddate", ""))
        if not last_mod or last_mod < cutoff:
            continue

        lei_derivada = enriched.get("lei_principal", "")
        categoria_derivada = enriched.get("linha_de_imposto_categoria", "")

        # So PATCH se temos valor determinado (nao "(sem ...)")
        lei_value_novo = LEI_LABEL_TO_PICKLIST_VALUE.get(lei_derivada, "")
        categoria_value_novo = CATEGORIA_LABEL_TO_PICKLIST_VALUE.get(categoria_derivada, "")

        lei_atual = (props.get("lei_principal") or "").strip()
        categoria_atual = (props.get("linha_de_imposto_categoria") or "").strip()

        patch_payload = {}
        if lei_value_novo and lei_value_novo != lei_atual:
            patch_payload["lei_principal"] = lei_value_novo
        if categoria_value_novo and categoria_value_novo != categoria_atual:
            patch_payload["linha_de_imposto_categoria"] = categoria_value_novo

        # produto default por pipeline (E6 Onda A)
        if not (props.get("produto") or "").strip():
            produto_default = PIPELINE_TO_PRODUTO.get(props.get("pipeline", ""))
            if produto_default:
                patch_payload["produto"] = produto_default
                produto_defaults += 1

        # origem_lead default ← Company.origem (auto-herança 23/04 tarde).
        # Só propaga valores canônicos unificados (ORIGEM_LEAD_PASSTHROUGH).
        # Valor ambíguo "Linkedin / Whatsapp / Site" fica pro executivo classificar.
        origem_lead_atual = (props.get("origem_lead") or "").strip()
        if origem_lead_atual in ("", "(em preenchimento)"):
            company_origem_raw = (enriched.get("company_origem") or "").strip()
            if company_origem_raw in ORIGEM_LEAD_PASSTHROUGH:
                patch_payload["origem_lead"] = company_origem_raw
                origem_lead_defaults += 1

        # e_o_primeiro_match derivado do histórico Ganho da Company (E6 Onda A).
        # Sem closedate no deal atual: assume "mais recente" (trata qualquer Ganho
        # da Company como "anterior") — evita false positives de primeiro match.
        if props.get("e_o_primeiro_match") in (None, ""):
            cid = deal_to_company.get(deal_id)
            if cid:
                ganhos_da_company = primeiro_match_map.get(cid, [])
                closedate_atual = _parse_hs_datetime(props.get("closedate", ""))
                ganhos_anteriores = [
                    (cd, did) for (cd, did) in ganhos_da_company
                    if did != deal_id and (closedate_atual is None or cd < closedate_atual)
                ]
                patch_payload["e_o_primeiro_match"] = "false" if ganhos_anteriores else "true"
                primeiro_match_defaults += 1

        if not patch_payload:
            continue

        r = req(
            "PATCH",
            f"/crm/v3/objects/deals/{deal_id}",
            json={"properties": patch_payload},
        )
        if r.status_code in (200, 201):
            atualizados += 1
        else:
            erros += 1
            print(f"PATCH ERRO deal {deal_id}: {r.status_code} {r.text[:200]}")

    print(
        f"PATCH back: {atualizados} deals atualizados, {erros} erros (lookback {lookback_hours}h) "
        f"| produto defaults: {produto_defaults} | primeiro_match defaults: {primeiro_match_defaults} "
        f"| origem_lead defaults: {origem_lead_defaults}"
    )
    return atualizados


def patch_default_trabalhado_por(raw_deals):
    """Default 'trabalhado_por = Executivo Brada' em deals com o campo vazio.

    Ata backlog Ivan 20/04 (Gap D): Ivan confirmou na reuniao que trabalhado_por
    deve vir preenchido no deal novo. PATCH via defaultValue da property nao
    funciona no Starter (HubSpot aceita body em silencio e ignora). Fallback:
    setar aqui no cron horario ate a Automatize entrar e patchar "Automatize"
    via API propria pros leads dela.

    Regra clássica "nao sobrescrever": se trabalhado_por ja tem valor, skip.
    Usa batch update (100 por call) pra eficiencia.
    """
    vazios = [
        d for d in raw_deals
        if not (d.get("properties", {}).get("trabalhado_por") or "").strip()
    ]
    if not vazios:
        print("patch_default_trabalhado_por: 0 deals com campo vazio")
        return 0

    atualizados = 0
    erros = 0
    for i in range(0, len(vazios), 100):
        chunk = vazios[i:i + 100]
        body = {
            "inputs": [
                {"id": d["id"], "properties": {"trabalhado_por": "Executivo Brada"}}
                for d in chunk
            ]
        }
        r = req("POST", "/crm/v3/objects/deals/batch/update", json=body)
        if r.status_code in (200, 207):
            atualizados += len(chunk)
        else:
            erros += len(chunk)
            print(f"BATCH trabalhado_por ERRO chunk {i}: {r.status_code} {r.text[:200]}")

    print(f"patch_default_trabalhado_por: {atualizados} deals default aplicado, {erros} erros")
    return atualizados


def sync_diagnostico_para_deal_lider(companies_list, deals_list, deal_to_company, ganho_stages_incentivador):
    """Espelha Company.valor_total_do_diagnostico em 2 properties do Deal.

    Pos-migracao 27/04, diagnostico mora em Company. Cards nativos de view de
    Deal so somam property do Deal — quem preencheu na Company nao via valor
    refletido. Esta funcao mantem 2 properties em paralelo, cada uma servindo
    um caso de uso distinto:

    1) `valor_diagnostico_empresa` (lider POR STAGE)
       Para cada (Company, stage), 1 deal lider (mais antigo do stage) recebe
       VTD. Demais do mesmo stage zerados. Use em cards/relatorios COM filtro
       de etapa — cada Company contribui 1x por etapa onde tem deal.

    2) `valor_diagnostico_empresa_global` (lider GLOBAL por Company)
       Para cada Company, 1 unico deal lider (Ganho mais antigo > Ativo mais
       antigo > Mais antigo qualquer) recebe VTD. Demais zerados. Use em
       cards/relatorios SEM filtro de etapa — soma agregada sem duplicar
       Companies multi-stage.

    Resultado:
      - Card POR ETAPA (Diagnostico/Projetos/Ganho da view Jessica): use
        property #1 com SUM. Cada Company aparece 1x na etapa.
      - Card de TOTAL GERAL (sem filtro de etapa): use property #2 com SUM.
        Cada Company contribui 1x na soma total.

    Idempotente — so PATCH se valor desejado != valor atual. Combina ambas
    properties em 1 unico PATCH por deal (eficiencia).

    Sem bug MATIFIC: 4 Ganhos da mesma Company viram 1 lider + 3 zerados em
    AMBAS properties.
    """
    # Index Company -> deals (com props necessarias)
    company_to_deals = defaultdict(list)
    deal_props_idx = {d["id"]: d.get("properties", {}) or {} for d in deals_list}
    for did, cid in deal_to_company.items():
        if cid and did in deal_props_idx:
            company_to_deals[str(cid)].append(did)

    patches = 0
    pulou_correto = 0
    sem_deals = 0
    erros = 0

    for c in companies_list:
        cid = str(c.get("id") or "")
        p = c.get("properties", {}) or {}
        try:
            vtd_company = float(p.get("valor_total_do_diagnostico") or 0)
        except (ValueError, TypeError):
            vtd_company = 0
        if vtd_company <= 0:
            continue

        deal_ids_da_cia = company_to_deals.get(cid, [])
        if not deal_ids_da_cia:
            sem_deals += 1
            continue

        # ===== Lider POR STAGE (property #1) =====
        deals_por_stage = defaultdict(list)
        for did in deal_ids_da_cia:
            stage = deal_props_idx[did].get("dealstage", "")
            deals_por_stage[stage].append(did)
        lideres_stage = set()
        for stage, dids in deals_por_stage.items():
            dids.sort(key=lambda d: deal_props_idx[d].get("createdate") or "9999")
            lideres_stage.add(dids[0])

        # ===== Lider GLOBAL (property #2) =====
        # Tier 1: Ganho Incentivador mais antigo (closedate ASC)
        # Tier 2: Ativo (nao-fechado) mais antigo (createdate ASC)
        # Tier 3: Mais antigo qualquer (createdate ASC)
        ganhos = [(did, deal_props_idx[did]) for did in deal_ids_da_cia
                  if deal_props_idx[did].get("dealstage", "") in ganho_stages_incentivador]
        ativos = [(did, deal_props_idx[did]) for did in deal_ids_da_cia
                  if deal_props_idx[did].get("dealstage", "") not in ganho_stages_incentivador
                  and deal_props_idx[did].get("dealstage", "") != "closedlost"]

        if ganhos:
            ganhos.sort(key=lambda x: x[1].get("closedate") or "9999")
            lider_global_id = ganhos[0][0]
        elif ativos:
            ativos.sort(key=lambda x: x[1].get("createdate") or "9999")
            lider_global_id = ativos[0][0]
        else:
            todos = [(did, deal_props_idx[did]) for did in deal_ids_da_cia]
            todos.sort(key=lambda x: x[1].get("createdate") or "9999")
            lider_global_id = todos[0][0]

        # PATCH: 1 unico PATCH por deal com ambas properties
        for did in deal_ids_da_cia:
            props_atuais = deal_props_idx[did]

            atual_stage = props_atuais.get("valor_diagnostico_empresa")
            atual_global = props_atuais.get("valor_diagnostico_empresa_global")
            try:
                atual_stage_num = float(atual_stage) if atual_stage not in (None, "") else 0
            except (ValueError, TypeError):
                atual_stage_num = 0
            try:
                atual_global_num = float(atual_global) if atual_global not in (None, "") else 0
            except (ValueError, TypeError):
                atual_global_num = 0

            desejado_stage = vtd_company if did in lideres_stage else 0
            desejado_global = vtd_company if did == lider_global_id else 0

            payload = {}
            if abs(atual_stage_num - desejado_stage) >= 0.01:
                payload["valor_diagnostico_empresa"] = str(desejado_stage)
            if abs(atual_global_num - desejado_global) >= 0.01:
                payload["valor_diagnostico_empresa_global"] = str(desejado_global)

            if not payload:
                pulou_correto += 1
                continue

            r = req("PATCH", f"/crm/v3/objects/deals/{did}",
                    json={"properties": payload})
            if r.status_code == 200:
                patches += 1
            else:
                erros += 1
                if erros <= 3:
                    print(f"  [erro] PATCH deal {did}: {r.status_code} {r.text[:150]}")
            time.sleep(0.05)

    print(f"sync_diagnostico_para_deal_lider: patches={patches} | ja_correto={pulou_correto} | "
          f"company_sem_deals={sem_deals} | erros={erros}")
    return patches


def patch_company_diag_from_aporte_ganho(companies_list, deals_list, deal_to_company,
                                          ganho_stages_incentivador):
    """Auto-preenche Company.valor_total_do_diagnostico via SOMA dos aportes
    de Ganhos do pipeline Incentivador associados.

    Regra (decisao Bruno 11/05/2026):
      - Company.valor_total_do_diagnostico esta vazio/0
      - E tem >=1 Deal em stage Ganho do pipeline Incentivador
      - E esse(s) Deal(s) tem valor_do_aporte > 0
      -> PATCH Company.valor_total_do_diagnostico = sum(valor_do_aporte dos Ganhos Inc)

    Premissa: cada Deal Ganho eh um aporte (parcial ou total) do diagnostico
    da empresa. A soma dos aportes vendidos eh piso conservador do diagnostico
    real (que inclui fatias nao-vendidas). Subestima o potencial mas eh melhor
    que zero.

    Multiplos Ganhos: SOMA todos. Empresa pode usar varias leis em paralelo
    no mesmo ano fiscal (cada Ganho eh uma fatia do potencial total
    monetizado).

    Re-execucao: idempotente. Uma vez preenchida, regra "nao sobrescrever"
    protege valor existente (manual ou auto). Pra incluir Ganhos posteriores,
    apagar manualmente o VTD da Company -> proximo cron recalcula sum.

    Auditoria sem flag nova (decisao Bruno — nao complicar schema): rastrear
    via comparacao derivada (Company.VTD == sum(aportes Ganhos Inc) implica
    auto-preenchido).

    Idempotente. Batch update 100/100. Sem lookback (processa base inteira;
    rodadas subsequentes skipam Companies ja preenchidas).
    """
    # Index Company -> deals associados
    company_to_deals = defaultdict(list)
    deal_props_idx = {d["id"]: d.get("properties", {}) or {} for d in deals_list}
    for did, cid in deal_to_company.items():
        if cid and did in deal_props_idx:
            company_to_deals[str(cid)].append(did)

    inputs = []
    sem_ganho_inc = 0
    ja_preenchido = 0
    sem_deals = 0

    for c in companies_list:
        cid = str(c.get("id") or "")
        p = c.get("properties", {}) or {}

        # So preenche se VTD vazio/0
        try:
            vtd_atual = float(p.get("valor_total_do_diagnostico") or 0)
        except (ValueError, TypeError):
            vtd_atual = 0
        if vtd_atual > 0:
            ja_preenchido += 1
            continue

        deal_ids_da_cia = company_to_deals.get(cid, [])
        if not deal_ids_da_cia:
            sem_deals += 1
            continue

        # Filtra Ganhos Incentivador com aporte > 0
        soma_aportes = 0.0
        n_ganhos_qualificados = 0
        for did in deal_ids_da_cia:
            props = deal_props_idx.get(did, {}) or {}
            if props.get("dealstage", "") not in ganho_stages_incentivador:
                continue
            try:
                aporte = float(props.get("valor_do_aporte") or 0)
            except (ValueError, TypeError):
                aporte = 0
            if aporte <= 0:
                continue
            soma_aportes += aporte
            n_ganhos_qualificados += 1

        if n_ganhos_qualificados == 0:
            sem_ganho_inc += 1
            continue

        inputs.append({
            "id": cid,
            "properties": {"valor_total_do_diagnostico": str(int(soma_aportes))},
        })

    # Batch update 100/100
    patches = 0
    erros = 0
    for i in range(0, len(inputs), 100):
        chunk = inputs[i:i + 100]
        r = req("POST", "/crm/v3/objects/companies/batch/update", json={"inputs": chunk})
        if r.status_code in (200, 207):
            patches += len(chunk)
        else:
            erros += len(chunk)
            if erros <= 3:
                print(f"  [erro] batch chunk {i}: {r.status_code} {r.text[:200]}")
        time.sleep(0.1)

    print(f"patch_company_diag_from_aporte_ganho: patches={patches} | "
          f"ja_preenchido={ja_preenchido} | sem_ganho_inc={sem_ganho_inc} | "
          f"sem_deals={sem_deals} | erros={erros}")
    return patches


def compute_criap_rollups(companies_list, deals_list, deal_to_company):
    """Calcula 8 props rollup CRIAP no Company-level (Sprint 0 14/05 + Sprint 1.5 27-28/05).

    Caminho 1: deals CRIAP vivem no pipeline Proponente com produto='CRIAP'.
    Filtro duplo: deal.pipeline == PROPONENTE_PIPELINE_ID AND deal.produto == 'CRIAP'.

    Agregacao dupla: Company aparece como
      (a) patrocinador via deal_to_company (associacao primaria typeId=5)
      (b) parceiro indicador via deal.parceiro_indicador_criap (company_id em string)

    8 props calculadas:
      - criap_total_aporte_2026: soma valor_do_aporte de Ganhos 2026 (Fechado + pos-venda)
      - criap_total_aporte_2025: soma valor_do_aporte de Ganhos 2025 (Sprint 1.5 — mitiga ajuste E)
      - criap_count_negocios_ativos: count deals em negociacao (stage != Ganho/Perdido/pos-venda)
      - criap_count_negocios_ganhos: count deals em Ganho comercial (Fechado, Acompanhamento, Ganho/pos-venda)
      - criap_count_negocios_perdidos: count deals em Perdido (qualquer data)
      - criap_projetos_apoiados_2026: CSV projeto_beneficiario_criap distintos de Ganhos 2026
      - criap_nomes_clientes_indicados: lista (multi-line) de Company.name dos clientes
        patrocinadores trazidos por esta Company, quando ela aparece como parceira indicadora
        (Sprint 1.5 27/05 — pedido Ivan 26/05). Vazio quando Company nao e parceira.
      - criap_aporte_por_cliente_2026: lista (multi-line) "Cliente: R$ N" agregada por
        cliente patrocinador, somando valor_do_aporte de Ganhos 2026 (valor > 0),
        ordenada decrescente por valor, formato BR. Vazio quando Company nao e parceira.
        (Sprint 1.5 add-on 28/05 — completa granularidade pedida por Ivan)

    Idempotente: PATCH so se valor mudou. Batch 100/100.
    Padrao: espelha patch_company_diag_from_aporte_ganho.
    """
    deal_props_idx = {d["id"]: d.get("properties", {}) or {} for d in deals_list}

    # Index nome de Company por id (Sprint 1.5 — pra rollup criap_nomes_clientes_indicados)
    company_name_by_id = {
        str(c.get("id") or ""): (c.get("properties", {}) or {}).get("name", "") or ""
        for c in companies_list
    }

    # Filtra deals CRIAP (pipeline+produto)
    criap_deal_ids = set()
    for did, p in deal_props_idx.items():
        if p.get("pipeline") == PROPONENTE_PIPELINE_ID and p.get("produto") == CRIAP_PRODUTO_VALUE:
            criap_deal_ids.add(did)

    # Index: company_id -> set(deal_ids) onde Company aparece como patrocinador OU parceiro
    by_company = defaultdict(set)
    # Sprint 1.5: index separado pra rollup criap_nomes_clientes_indicados
    # (so quando Company atual eh parceira do deal, listar clientes patrocinadores)
    by_company_parceiro = defaultdict(set)
    for did in criap_deal_ids:
        p = deal_props_idx[did]
        cid_patroc = deal_to_company.get(did)
        if cid_patroc:
            by_company[str(cid_patroc)].add(did)
        cid_parceiro = (p.get("parceiro_indicador_criap") or "").strip()
        if cid_parceiro:
            by_company[str(cid_parceiro)].add(did)
            by_company_parceiro[str(cid_parceiro)].add(did)

    inputs = []
    sem_deals_criap = 0

    for c in companies_list:
        cid = str(c.get("id") or "")
        deal_ids = by_company.get(cid, set())
        if not deal_ids:
            sem_deals_criap += 1
            continue

        total_aporte_2026 = 0.0
        total_aporte_2025 = 0.0
        count_ativos = 0
        count_ganhos = 0
        count_perdidos = 0
        projetos_2026 = set()

        for did in deal_ids:
            p = deal_props_idx[did]
            stage = p.get("dealstage") or ""
            try:
                valor = float(p.get("valor_do_aporte") or 0)
            except (ValueError, TypeError):
                valor = 0
            close = p.get("closedate") or ""
            projeto = (p.get("projeto_beneficiario_criap") or "").strip()

            if stage in CRIAP_GANHO_STAGES:
                count_ganhos += 1
                if close.startswith("2026"):
                    total_aporte_2026 += valor
                    if projeto:
                        projetos_2026.add(projeto)
                elif close.startswith("2025"):
                    total_aporte_2025 += valor
            elif stage == CRIAP_PERDIDO_STAGE_ID:
                count_perdidos += 1
            else:
                count_ativos += 1

        # Sprint 1.5 — criap_nomes_clientes_indicados: so popula quando esta Company atua
        # como parceira indicadora em algum deal. Listar Company.name dos clientes
        # (patrocinadores primary) de cada deal indicado, sem duplicar.
        nomes_clientes_indicados = set()
        deal_ids_como_parceira = by_company_parceiro.get(cid, set())
        for did_p in deal_ids_como_parceira:
            cid_patroc_do_deal = deal_to_company.get(did_p)
            if cid_patroc_do_deal:
                name_cliente = company_name_by_id.get(str(cid_patroc_do_deal), "")
                if name_cliente:
                    nomes_clientes_indicados.add(name_cliente)

        # Sprint 1.5 add-on 28/05 — criap_aporte_por_cliente_2026: agrega valor_do_aporte
        # de Ganhos 2026 por cliente patrocinador (somando deals multi-projeto do mesmo cliente),
        # formato BR, ordem decrescente. Filtra valor > 0 (evita "Cliente: R$ 0" no card).
        # Mesmo padrao de iteracao do nomes_clientes_indicados — so popula quando Company eh parceira.
        aporte_por_cliente = defaultdict(float)
        for did_p in deal_ids_como_parceira:
            p_did = deal_props_idx[did_p]
            stage_did = p_did.get("dealstage") or ""
            close_did = p_did.get("closedate") or ""
            try:
                valor_did = float(p_did.get("valor_do_aporte") or 0)
            except (ValueError, TypeError):
                valor_did = 0
            cid_patroc_do_deal = deal_to_company.get(did_p)
            if (stage_did in CRIAP_GANHO_STAGES
                and close_did.startswith("2026")
                and cid_patroc_do_deal
                and valor_did > 0):
                name_cliente = company_name_by_id.get(str(cid_patroc_do_deal), "")
                if name_cliente:
                    aporte_por_cliente[name_cliente] += valor_did
        linhas_apc = [
            f"{nome}: R$ {int(valor):,}".replace(",", ".")
            for nome, valor in sorted(aporte_por_cliente.items(), key=lambda kv: -kv[1])
        ]
        aporte_por_cliente_str = "\n".join(linhas_apc)

        # Comparar com valor atual da Company antes de PATCH (idempotencia)
        atual = c.get("properties", {}) or {}
        novo = {
            "criap_total_aporte_2026": str(int(total_aporte_2026)),
            "criap_total_aporte_2025": str(int(total_aporte_2025)),
            "criap_count_negocios_ativos": str(count_ativos),
            "criap_count_negocios_ganhos": str(count_ganhos),
            "criap_count_negocios_perdidos": str(count_perdidos),
            "criap_projetos_apoiados_2026": ",".join(sorted(projetos_2026)),
            "criap_nomes_clientes_indicados": "\n".join(sorted(nomes_clientes_indicados)),
            "criap_aporte_por_cliente_2026": aporte_por_cliente_str,
        }
        delta = {k: v for k, v in novo.items() if str(atual.get(k) or "") != v}
        if delta:
            inputs.append({"id": cid, "properties": delta})

    # Batch update 100/100
    patches = 0
    erros = 0
    for i in range(0, len(inputs), 100):
        chunk = inputs[i:i + 100]
        r = req("POST", "/crm/v3/objects/companies/batch/update", json={"inputs": chunk})
        if r.status_code in (200, 207):
            patches += len(chunk)
        else:
            erros += len(chunk)
            if erros <= 3:
                print(f"  [erro] batch CRIAP rollup chunk {i}: {r.status_code} {r.text[:200]}")
        time.sleep(0.1)

    print(f"compute_criap_rollups: patches={patches} | "
          f"deals_criap={len(criap_deal_ids)} | companies_sem_criap={sem_deals_criap} | erros={erros}")
    return patches


def _norm_key(s):
    """Normaliza string pra chave de join cross-pipeline (lower, sem acento/espaço extra)."""
    if not s:
        return ""
    s = str(s).strip().lower()
    for a, b in (("á", "a"), ("à", "a"), ("ã", "a"), ("â", "a"), ("é", "e"), ("ê", "e"),
                 ("í", "i"), ("ó", "o"), ("ô", "o"), ("õ", "o"), ("ú", "u"), ("ç", "c")):
        s = s.replace(a, b)
    return " ".join(s.split())


# Sprint 1 (chave canonica de NOME): estende _norm_key removendo sufixo de data, ISS/imposto e
# forma societaria (so como sufixo), tirando pontuacao residual e despacando marcas conhecidas.
# NAO e o dkey (ops/reconciliacao_planilha_cards.py), que despaca TUDO — _norm_key2 mantem os
# espacos entre palavras (pra empresa_canonica nao colar tokens). Ver Diagnostico_Modelo_Sprints_03jun.
_BRAND_DESPACE = {"nu bank": "nubank"}  # marcas cujo canonico e 1 token; extensivel
_RE_DATA = re.compile(r"\b\d{1,2}\s*[/.\-]\s*\d{1,2}\s*[/.\-]\s*\d{2,4}\b")
_RE_LIXO = re.compile(r"\b(iss|imposto)\b")
_RE_FORMA = re.compile(r"\b(ltda|s a|sa|eireli|epp|mei|me)\s*$")  # forma societaria so como sufixo


def _norm_key2(s):
    """Chave canonica de NOME. Estende _norm_key: remove data, ISS/imposto, forma societaria
    (sufixo), pontuacao residual e despaca marcas conhecidas. Mantem espacos entre palavras."""
    k = _norm_key(s)
    if not k:
        return ""
    k = _RE_DATA.sub(" ", k)             # remove datas (precisa dos separadores / . -)
    k = _RE_LIXO.sub(" ", k)             # remove iss/imposto
    k = re.sub(r"[^a-z0-9 ]", " ", k)    # pontuacao -> espaco (s/a->s a, s.a.->s a)
    k = " ".join(k.split())
    k = _RE_FORMA.sub("", k).strip()     # remove forma societaria (sufixo), ja sem pontuacao
    k = " ".join(k.split())
    return _BRAND_DESPACE.get(k, k)


# Mapa de normalização do proponente livre (nome_do_proponente, sujo) -> 1 das 8 entidades
# internas do grupo. Mais específico primeiro (egp cir antes de egp; conectados do bem/caxias
# antes de conectados). Reusado pela auto-derivação interno/externo (Sprint 0.1) e pelo
# forward-fill da property tipo_de_proponente (Sprint 2.0). Ver Diagnostico_Modelo_Sprints_03jun.
PROPONENTE_INTERNO_MATCHERS = [
    ("egp cir", "EGP Cir. Soc. Cultura IR"),
    ("egp", "EGP"),
    ("encaminhando", "Encaminhando"),
    ("conectados do bem", "Conectados do Bem"),
    ("conectados caxias", "Conectados Caxias"),
    ("circuito social", "Circuito Social de Corrida"),
    ("conectados", "Conectados"),
    ("brada digital", "Brada Digital"),
    ("proj.casa", "Brada Digital"),
    ("somos brada", "Brada Digital"),  # PENDENTE confirmação Bruno (Sprint 2.3) — interno em todo caso
    ("brada", "Brada Digital"),
]


def _map_proponente_interno(nome):
    """nome_do_proponente livre -> entidade interna canônica (1 das 8) ou None (terceiro/vazio)."""
    p = _norm_key(nome)
    if not p:
        return None
    for sub, entidade in PROPONENTE_INTERNO_MATCHERS:
        if sub in p:
            return entidade
    return None


def build_consolidado_layer(enriched, stages=None):
    """Backbone único (grão = 1 deal) do qual derivam as 3 views: reporting Luciana,
    visão 4 colunas Vitor, dashboard CRIAPE (Sprint 1).

    Reusa o dict `enriched` (produto, valor_do_aporte, valor_vendido, closedate,
    data_do_aporte, nome/numero_do_projeto, projeto_beneficiario_criap, company_name,
    e_ganho, executivo_nome, stage_ordem) e DERIVA:
      - interno_externo (do split Match interno/externo — Bruno)
      - comissão Vendas% (15/10% × 0,88 × 8/4/3) — PENDENTE validação Ivan/Luciana
      - projeto_key + tem_overlap_projeto (anti double-count cross-pipeline)
      - flags convertido/won (conversão Incentivador a partir do estágio "match";
        Proponente a partir de "ganho" — Bruno 01/06)
      - *_status (closedate/comissão/owner) pra views degradarem com elegância e
        auto-completarem quando os inputs (Ivan/Leila) chegarem.

    Função PURA (não escreve no CRM): in=enriched, out=list de dicts. O main()/teste
    escreve a aba `consolidado` via write_to_sheets.
    """
    match_ordem = None
    if stages:
        match_ordem = (stages.get(MATCH_STAGE_INCENTIVADOR_ID, {}) or {}).get("ordem")

    rows = []
    for e in enriched:
        produto = (e.get("produto") or "").strip()
        pipeline = e.get("pipeline_nome") or ""
        valor = float(e.get("valor_do_aporte") or 0)
        valor_vendido = float(e.get("valor_vendido") or 0)
        e_ganho = bool(e.get("e_ganho"))
        closedate = e.get("closedate") or ""
        nome_projeto = e.get("nome_do_projeto") or ""
        numero_projeto = e.get("numero_do_projeto") or ""
        projeto_benef = e.get("projeto_beneficiario_criap") or ""
        # Etapa 1 (02/06): chave canonica de empresa pra analise/agrupamento (nao mescla cadastro).
        empresa_canonica = _normalize_cnpj(e.get("company_cnpj") or "") or _norm_key2(e.get("company_name") or "")

        # interno/externo deriva do PROPONENTE (reunião Leila/Ivan 02/06): entidade do grupo
        # (EGP/Encaminhando/Brada/CRIAPE) = Interno (15%); terceiro (Externo) = Externo (10%).
        # Via property tipo_de_proponente (substitui o split de produto, SUPERSEDED). Vazio
        # até classificar. Ver Modelo_Interno_Externo_Tipo_Proponente.
        # Precedência: (1) classificação explícita na property tipo_de_proponente vence;
        # (2) produto==CRIAPE = sempre interno (regra estrutural); (3) AUTO-DERIVAÇÃO do
        # nome_do_proponente (proponente do grupo=Interno; terceiro=Externo); (4) vazio="".
        # A auto-derivação cobre ~89% do valor won sem trabalho manual (Sprint 0.1).
        tipo_prop = (e.get("tipo_de_proponente") or "").strip()
        nome_prop = (e.get("nome_do_proponente") or "").strip()
        if tipo_prop == "Externo":
            interno_externo = "Externo"
        elif tipo_prop:
            interno_externo = "Interno"  # classificação explícita (override) vence
        elif produto == "CRIAPE":
            interno_externo = "Interno"  # CRIAPE (Proponente) = sempre projeto interno (enum é o marcador)
        elif _map_proponente_interno(nome_prop):
            interno_externo = "Interno"  # proponente ∈ 8 entidades do grupo
        elif nome_prop:
            interno_externo = "Externo"  # proponente terceiro presente
        else:
            interno_externo = ""  # sem classificação (residual manual / planilha)

        # Comissão BRADA = valor × 15/10% → Líquido × 0,88 (CONFIRMADO na planilha do Ivan).
        # comissao_brada/liquido = camada CONFIÁVEL (alimenta valor_efetivo_brada das views).
        comissao_brada = liquido = c_ivan = c_jaque = c_externo = 0.0
        if interno_externo == "Interno":
            comissao_brada = valor * PCT_INTERNO
        elif interno_externo == "Externo":
            comissao_brada = valor * PCT_EXTERNO
        if comissao_brada:
            liquido = comissao_brada * FATOR_LIQUIDO
            # Split por pessoa (Ivan/Jaque) = stream Vendas% (Match). CRIAPE/Elaboração têm modelo
            # próprio (pendente Ivan) → per-pessoa fica 0 (valor_efetivo_brada/comissao_brada seguem
            # valendo pro "quanto" das views). PROVISÓRIO até reunião do financeiro.
            if produto == "Match":
                c_ivan = liquido * PCT_IVAN
                # Jaque 4% e externo 3% são XOR por deal (dependem de "Nome do externo", campo que
                # NÃO existe no HubSpot). Default = Jaque; externo fica 0 até esse sinal existir.
                c_jaque = liquido * PCT_JAQUE
                c_externo = 0.0

        # status deriva da classificação (tipo_de_proponente), não mais do produto.
        if interno_externo and valor > 0:
            comissao_status = "calculada"
        elif interno_externo and valor <= 0:
            comissao_status = "pendente_valor"
        elif not interno_externo:
            comissao_status = "pendente_classificacao"  # tipo_de_proponente vazio
        else:
            comissao_status = "pendente_modelo"

        if produto in ("Match", "Match interno", "Match externo"):
            fluxo_comissao = "Vendas%"
        elif produto == "CRIAPE":
            fluxo_comissao = "CRIAPE (pendente Ivan)"
        elif produto == "Elaboração":
            fluxo_comissao = "Elaboração (pendente Ivan)"
        else:
            fluxo_comissao = produto

        # projeto_key: chave de dedup cross-pipeline. CRIAPE usa o enum
        # projeto_beneficiario_criap (93% fill); os demais usam nome/numero_do_projeto.
        if produto == "CRIAPE":
            projeto_key = _norm_key(projeto_benef)
        else:
            projeto_key = _norm_key(nome_projeto) or _norm_key(numero_projeto)

        # closedate fake/null só faz sentido pro CRIAPE (Bruno 01/06: Match/Elab corretos).
        if not closedate:
            closedate_status = "null"
        elif produto == "CRIAPE" and closedate.startswith("2026-05-22"):
            closedate_status = "fake"
        else:
            closedate_status = "real"

        # Conversão (Bruno 01/06). won_ganho reusa e_ganho (won + pós-venda nos 2 pipelines).
        won_ganho = e_ganho
        convertido = e_ganho
        if pipeline == "Incentivador" and match_ordem is not None:
            try:
                convertido = (float(e.get("stage_ordem") or 999) >= float(match_ordem)) or e_ganho
            except (ValueError, TypeError):
                convertido = e_ganho

        owner_nome = e.get("executivo_nome") or "(sem owner)"
        owner_status = "atribuido" if owner_nome not in ("(sem owner)", "") else "null"

        rows.append({
            "deal_id": e.get("deal_id", ""),
            "cliente": e.get("company_name", "") or e.get("deal_name", ""),
            "cnpj": e.get("company_cnpj", ""),
            "pipeline": pipeline,
            "produto": produto,
            "interno_externo": interno_externo,
            "fluxo_comissao": fluxo_comissao,
            "projeto_key": projeto_key,
            "numero_projeto": numero_projeto,
            "nome_projeto": nome_projeto,
            "proponente": e.get("nome_do_proponente", ""),
            "stage": e.get("stage_nome", ""),
            "convertido": 1 if convertido else 0,
            "won_ganho": 1 if won_ganho else 0,
            "tem_overlap_projeto": 0,  # 2a passada
            "closedate": closedate,
            "closedate_status": closedate_status,
            "data_aporte": e.get("data_do_aporte", ""),
            "valor_bruto": valor,
            "valor_vendido": valor_vendido,
            "liquido_brada": round(liquido, 2),
            "comissao_ivan": round(c_ivan, 2),
            "comissao_jaque": round(c_jaque, 2),
            "comissao_externo": round(c_externo, 2),
            "comissao_status": comissao_status,
            "owner": owner_nome,
            "owner_status": owner_status,
            "origem_lead": e.get("origem_lead", ""),
            "lei_principal": e.get("lei_principal", ""),
            "ano": e.get("ano_fechamento", ""),
            "empresa_canonica": empresa_canonica,  # Etapa 1: coluna no FIM (protege binding Looker)
            "tipo_de_proponente": tipo_prop,  # 03/06: classificação (grupo vs Externo)
            "valor_efetivo_brada": round(comissao_brada, 2),  # 03/06: "quanto" das views (= % efetivo Brada)
            # Sprint D (12/06): contato do proponente no FIM do dict (3 ultimas colunas do consolidado;
            # o header do consolidado e list(cons[0].keys()), entao a ordem de insercao = ordem da aba).
            "nome_contato_proponente": e.get("nome_contato_proponente", ""),
            "email_proponente": e.get("email_proponente", ""),
            "telefone_proponente": e.get("telefone_proponente", ""),
            # R1 Fonte Unica de Lucro: projetado do pipeline ativo no FIM do dict
            # (protege binding Looker + guard exact-match do financeiro). Round(2) =
            # mesmo render que raw_metas_anuais (write_performance_sheet ~2475).
            "valor_projetado_ativo": round(float(e.get("valor_projetado_ativo", 0) or 0), 2),
        })

    # 2a passada: flag de overlap cross-pipeline = MESMA CAPTACAO registrada nos dois
    # lados (incentivador/Match + proponente/CRIAPE), chave (empresa_canonica,
    # valor_vendido) em >1 pipeline. valor_vendido>0 so em won, entao ja restringe a won.
    # Antes era por projeto_key (nome): dava (a) falso POSITIVO quando clientes distintos
    # compartilhavam um nome de iniciativa (ex.: "Conectados" em N CRIAPEs diferentes) e
    # (b) falso NEGATIVO quando a mesma captacao tinha nomes diferentes nos dois lados
    # (ex.: RMED "Biblioteca Para Todos" no CRIAPE x "Con. do Bem + Biblioteca" no Match).
    # Analise 18/06: 3 pares reais (RMED 204.999, SEEL 50.428, Real Pax 39.932) = R$295k
    # (~1% do total). Marca os DOIS lados do par; QUAL lado deduplicar (incentivador x
    # proponente) e' decisao de negocio pendente (Ivan) — ver Visao_Gerencial / Spec.
    def _cap_key(r):
        cn = (r.get("empresa_canonica") or "").strip()
        v = round(float(r.get("valor_vendido") or 0))
        return (cn, v) if (cn and v > 0) else None

    pipelines_por_captacao = defaultdict(set)
    for r in rows:
        k = _cap_key(r)
        if k:
            pipelines_por_captacao[k].add(r["pipeline"])
    for r in rows:
        k = _cap_key(r)
        r["tem_overlap_projeto"] = 1 if (k and len(pipelines_por_captacao[k]) > 1) else 0

    return rows


def sync_amount_para_aporte(deals_list):
    """Espelha valor_do_aporte → amount em cada Deal.

    `amount` eh property nativa do HubSpot exibida no header do registro
    e somada nos totalizadores do kanban board. Em Starter, esses dois
    elementos NAO permitem trocar a property. Workaround: manter amount
    sincronizado com valor_do_aporte (a fonte de verdade da Brada).

    Idempotente — so PATCH se amount != valor_do_aporte. Pula Deals com
    aporte vazio (nao zera amount existente — preserva possivel auto-calculo
    de line items, raro mas possivel).

    Trade-off conhecido: se algum Deal usar line items no futuro, o
    auto-calculo do HubSpot vai brigar com o cron. Como Brada nao usa
    line items hoje, nao e problema. Detectar virtualmente custa 1
    request extra por deal — nao vale a pena agora.
    """
    patches_aporte_para_amount = 0
    patches_amount_para_aporte = 0  # NOVO (26/05): reverse fill quando valor_do_aporte vazio
    pulou_correto = 0
    pulou_ambos_vazios = 0
    erros = 0

    for d in deals_list:
        did = d["id"]
        p = d.get("properties", {}) or {}
        try:
            aporte = float(p.get("valor_do_aporte") or 0)
        except (ValueError, TypeError):
            aporte = 0
        try:
            amount_atual = float(p.get("amount") or 0)
        except (ValueError, TypeError):
            amount_atual = 0

        # Caso 5: ambos vazios — nada a fazer
        if aporte <= 0 and amount_atual <= 0:
            pulou_ambos_vazios += 1
            continue

        # Caso 4 (NOVO 26/05): valor_do_aporte vazio mas amount populated.
        # Bug Sprint 1: 128 deals migrados tinham só amount preenchido. Sem
        # popular valor_do_aporte, compute_criap_rollups soma 0 (linha ~1255
        # le valor_do_aporte, nao amount). Bidirecional resolve forever:
        # Jaqueline pode criar deal manual com so amount no UI sem quebrar
        # rollup.
        if aporte <= 0 and amount_atual > 0:
            r = req("PATCH", f"/crm/v3/objects/deals/{did}",
                    json={"properties": {"valor_do_aporte": str(amount_atual)}})
            if r.status_code == 200:
                patches_amount_para_aporte += 1
            else:
                erros += 1
                if erros <= 3:
                    print(f"  [erro] PATCH valor_do_aporte deal {did}: {r.status_code} {r.text[:150]}")
            time.sleep(0.05)
            continue

        # Caso 1: ja sincronizado
        if abs(amount_atual - aporte) < 0.01:
            pulou_correto += 1
            continue

        # Caso 2-3: valor_do_aporte populated, amount diferente ou vazio.
        # valor_do_aporte e source of truth — espelha pra amount.
        r = req("PATCH", f"/crm/v3/objects/deals/{did}",
                json={"properties": {"amount": str(aporte)}})
        if r.status_code == 200:
            patches_aporte_para_amount += 1
        else:
            erros += 1
            if erros <= 3:
                print(f"  [erro] PATCH amount deal {did}: {r.status_code} {r.text[:150]}")
        time.sleep(0.05)

    total_patches = patches_aporte_para_amount + patches_amount_para_aporte
    print(f"sync_amount_para_aporte: patches={total_patches} (aporte->amount={patches_aporte_para_amount}, "
          f"amount->aporte={patches_amount_para_aporte}) | ja_correto={pulou_correto} | "
          f"ambos_vazios={pulou_ambos_vazios} | erros={erros}")
    return total_patches


def sync_parceiro_cnpj_criap(deals_list, companies_list):
    """Espelha CNPJ da Company referenciada por Deal.parceiro_indicador_criap (string company_id)
    pra Deal.parceiro_indicador_cnpj_criap. Permite Looker fazer JOIN cross-Company sem
    chamar API. Padrao espelha sync_amount_para_aporte.

    Idempotente: so PATCH se valor mudou. So toca deals com produto='CRIAP' (escopo CRIAP).
    """
    cnpj_by_company = {
        str(c["id"]): (c.get("properties", {}) or {}).get("cnpj", "") or ""
        for c in companies_list
    }
    patches = 0
    pulou_correto = 0
    pulou_sem_parceiro = 0
    pulou_nao_criap = 0
    pulou_orfao = 0
    erros = 0

    for d in deals_list:
        did = d["id"]
        p = d.get("properties", {}) or {}
        if p.get("produto") != CRIAP_PRODUTO_VALUE:
            pulou_nao_criap += 1
            continue

        parceiro_id = (p.get("parceiro_indicador_criap") or "").strip()
        if not parceiro_id:
            pulou_sem_parceiro += 1
            continue

        # Hardening (merge lossless): se a company parceira foi mesclada/aposentada,
        # parceiro_id some do dict. NÃO tratar como "limpar" (apagaria o CNPJ do deal) —
        # pular como órfão e preservar o valor atual até o remap pós-merge corrigir o ID.
        if parceiro_id not in cnpj_by_company:
            pulou_orfao += 1
            continue
        cnpj_correto = cnpj_by_company[parceiro_id]
        cnpj_atual = (p.get("parceiro_indicador_cnpj_criap") or "").strip()
        if cnpj_atual == cnpj_correto:
            pulou_correto += 1
            continue

        r = req("PATCH", f"/crm/v3/objects/deals/{did}",
                json={"properties": {"parceiro_indicador_cnpj_criap": cnpj_correto}})
        if r.status_code == 200:
            patches += 1
        else:
            erros += 1
            if erros <= 3:
                print(f"  [erro] PATCH parceiro_cnpj deal {did}: {r.status_code} {r.text[:150]}")
        time.sleep(0.05)

    print(f"sync_parceiro_cnpj_criap: patches={patches} | ja_correto={pulou_correto} | "
          f"sem_parceiro={pulou_sem_parceiro} | nao_criap={pulou_nao_criap} | orfao={pulou_orfao} | erros={erros}")
    return patches


def sync_parceiro_nome_criap(deals_list, companies_list):
    """Espelha Company.name da parceira referenciada por Deal.parceiro_indicador_criap
    pra Deal.parceiro_indicador_nome_criap. Permite leitura humana no card do deal
    sem precisar abrir o cadastro da Company (Sprint 0.5 19/05, pedido Ivan).

    Espelha sync_parceiro_cnpj_criap 1:1; so toca deals com produto='CRIAP'.
    Idempotente: PATCH so se nome mudou.
    """
    name_by_company = {
        str(c["id"]): (c.get("properties", {}) or {}).get("name", "") or ""
        for c in companies_list
    }
    patches = 0
    pulou_correto = 0
    pulou_sem_parceiro = 0
    pulou_nao_criap = 0
    pulou_orfao = 0
    erros = 0

    for d in deals_list:
        did = d["id"]
        p = d.get("properties", {}) or {}
        if p.get("produto") != CRIAP_PRODUTO_VALUE:
            pulou_nao_criap += 1
            continue

        parceiro_id = (p.get("parceiro_indicador_criap") or "").strip()
        if not parceiro_id:
            pulou_sem_parceiro += 1
            continue

        # Hardening (merge lossless): parceira mesclada/aposentada → não apagar o nome.
        if parceiro_id not in name_by_company:
            pulou_orfao += 1
            continue
        nome_correto = name_by_company[parceiro_id]
        nome_atual = (p.get("parceiro_indicador_nome_criap") or "").strip()
        if nome_atual == nome_correto:
            pulou_correto += 1
            continue

        r = req("PATCH", f"/crm/v3/objects/deals/{did}",
                json={"properties": {"parceiro_indicador_nome_criap": nome_correto}})
        if r.status_code == 200:
            patches += 1
        else:
            erros += 1
            if erros <= 3:
                print(f"  [erro] PATCH parceiro_nome deal {did}: {r.status_code} {r.text[:150]}")
        time.sleep(0.05)

    print(f"sync_parceiro_nome_criap: patches={patches} | ja_correto={pulou_correto} | "
          f"sem_parceiro={pulou_sem_parceiro} | nao_criap={pulou_nao_criap} | orfao={pulou_orfao} | erros={erros}")
    return patches


def sync_nome_projeto_criap(deals_list, dry_run=False):
    """Forward-fill (Sprint 1): espelha o label do enum projeto_beneficiario_criap pra
    Deal.nome_do_projeto nos deals CRIAPE (hoje 0/130 preenchidos). O label vem da property
    LIVE (nao do setup_criap_fields.py stale; nao da pra title-case por causa de 'Ecocine+').
    Idempotente: PATCH so se nome_do_projeto != label. Deals CRIAPE sem enum ficam vazios.
    Ver Diagnostico_Modelo_Sprints_03jun + feedback_forward_fill_property.
    dry_run=True so conta (nao escreve no CRM)."""
    try:
        rp = req("GET", "/crm/v3/properties/deals/projeto_beneficiario_criap")
        label_by_value = {o["value"]: o["label"] for o in rp.json().get("options", [])}
    except Exception as e:
        print(f"  [erro] sync_nome_projeto_criap: nao carregou enum projeto_beneficiario_criap: {e}")
        return 0

    patches = 0
    pulou_correto = 0
    pulou_nao_criap = 0
    pulou_sem_enum = 0
    erros = 0

    for d in deals_list:
        did = d["id"]
        p = d.get("properties", {}) or {}
        if p.get("produto") != CRIAP_PRODUTO_VALUE:
            pulou_nao_criap += 1
            continue

        enum_val = (p.get("projeto_beneficiario_criap") or "").strip()
        if not enum_val:
            pulou_sem_enum += 1
            continue

        nome_correto = label_by_value.get(enum_val, enum_val)
        nome_atual = (p.get("nome_do_projeto") or "").strip()
        if nome_atual == nome_correto:
            pulou_correto += 1
            continue

        if dry_run:
            patches += 1
            continue

        r = req("PATCH", f"/crm/v3/objects/deals/{did}",
                json={"properties": {"nome_do_projeto": nome_correto}})
        if r.status_code == 200:
            patches += 1
        else:
            erros += 1
            if erros <= 3:
                print(f"  [erro] PATCH nome_do_projeto deal {did}: {r.status_code} {r.text[:150]}")
        time.sleep(0.05)

    tag = "DRY-RUN " if dry_run else ""
    print(f"sync_nome_projeto_criap: {tag}patches={patches} | ja_correto={pulou_correto} | "
          f"sem_enum={pulou_sem_enum} | nao_criap={pulou_nao_criap} | erros={erros}")
    return patches


# Sprint 0.5 19/05 — pedido Ivan: vinculo cliente<->parceiro explicito no card.
# Custom association labels (Pro+) nao estao disponiveis em Starter; portanto usamos
# typeId=341 (HubSpot defined, sem label) que ao menos coloca a Company parceira na
# secao Companies do card do Deal e o Deal no tab Deals do card da Company parceira.
# Validado via GET /crm/v4/associations/deals/companies/labels (19/05): retorna apenas
# typeId=5 (Primary) e typeId=341 (sem label) — exatamente o que precisamos em Starter.
HUBSPOT_DEAL_TO_COMPANY_TYPE_ID = 341
HUBSPOT_DEAL_TO_COMPANY_PRIMARY_TYPE_ID = 5


def sync_parceiro_associations_criap(deals_list, deal_to_company, dry_run=False):
    """Mantem secondary association nativa Deal<->Company parceira (typeId=341).

    Pra cada deal CRIAP:
      1. GET associations atuais
      2. Calcula expected_341 = {parceiro_indicador_criap} se preenchido e != primary
      3. Calcula current_341 = companies associadas via typeId=341 (excluindo a primary)
      4. PUT pra cada add (expected - current)
      5. DELETE pra cada remove (current - expected) — APENAS typeId=341, nunca primary

    SEGURANCAS:
      - Filtro estrito typeId=341 (custom labels Pro+ tem outros typeIds, intocados)
      - Assert defensivo: NUNCA DELETA se cid == primary_company_id
      - Toca apenas deals com produto='CRIAP' (escopo CRIAP)
      - dry_run=True so loga, nao chama PUT/DELETE
    """
    assocs_criadas = 0
    assocs_removidas = 0
    ja_correto = 0
    pulou_nao_criap = 0
    pulou_sem_parceiro_e_sem_341 = 0
    erros = 0
    deals_processados = 0

    for d in deals_list:
        did = d["id"]
        p = d.get("properties", {}) or {}
        if p.get("produto") != CRIAP_PRODUTO_VALUE:
            pulou_nao_criap += 1
            continue

        deals_processados += 1
        primary_cid = str(deal_to_company.get(did) or "")
        parceiro_id = (p.get("parceiro_indicador_criap") or "").strip()

        # expected: secondary typeId=341 = a Company parceira (se != primary)
        expected_341 = set()
        if parceiro_id and parceiro_id != primary_cid:
            expected_341.add(parceiro_id)

        # current: companies ja associadas com typeId=341 que NAO sao a primary
        r = req("GET", f"/crm/v4/objects/deals/{did}/associations/companies")
        if r.status_code != 200:
            erros += 1
            if erros <= 3:
                print(f"  [erro] GET assocs deal {did}: {r.status_code} {r.text[:150]}")
            continue
        results = r.json().get("results", []) or []
        # Sprint 1.5 fix typeId=341 duplicado (27/05): blacklist combina primary do snapshot
        # (deal_to_company) + primary do runtime GET. Antes do fix, 14 deals (11 Baloart +
        # 3 Felipe PV) tinham Company com AMBOS typeId=5 + typeId=341 — runtime mostrava
        # so 341 (sem 5) por race condition/stale cache, entrava em current_341 e disparava
        # "[BUG] tentou remover primary". Fix: se Company eh primary em qualquer source,
        # nunca entra em current_341 — invariante semantica preservada.
        primary_cids_blacklist = {primary_cid} if primary_cid else set()
        for assoc in results:
            cid = str(assoc.get("toObjectId") or "")
            types = assoc.get("associationTypes", []) or []
            if any(t.get("typeId") == HUBSPOT_DEAL_TO_COMPANY_PRIMARY_TYPE_ID for t in types):
                primary_cids_blacklist.add(cid)
        current_341 = set()
        for assoc in results:
            cid = str(assoc.get("toObjectId") or "")
            types = assoc.get("associationTypes", []) or []
            has_341 = any(t.get("typeId") == HUBSPOT_DEAL_TO_COMPANY_TYPE_ID for t in types)
            # Secondary = typeId=341 presente E NAO eh primary em NENHUM source
            if has_341 and cid not in primary_cids_blacklist:
                current_341.add(cid)

        to_add = expected_341 - current_341
        to_remove = current_341 - expected_341

        if not to_add and not to_remove:
            if expected_341 or current_341:
                ja_correto += 1
            else:
                pulou_sem_parceiro_e_sem_341 += 1
            continue

        # ADD
        for cid in to_add:
            if dry_run:
                print(f"  [dry] ADD assoc 341: deal {did} <-> company {cid}")
                assocs_criadas += 1
                continue
            body = [{"associationCategory": "HUBSPOT_DEFINED",
                     "associationTypeId": HUBSPOT_DEAL_TO_COMPANY_TYPE_ID}]
            r2 = req("PUT", f"/crm/v4/objects/deals/{did}/associations/companies/{cid}",
                     json=body)
            if r2.status_code in (200, 201):
                assocs_criadas += 1
            else:
                erros += 1
                if erros <= 3:
                    print(f"  [erro] PUT assoc deal {did} -> company {cid}: {r2.status_code} {r2.text[:200]}")
            time.sleep(0.05)

        # REMOVE
        for cid in to_remove:
            # SEGURANCA CRITICA: nunca deletar a primary
            if cid == primary_cid:
                erros += 1
                print(f"  [BUG] tentou remover primary deal={did} cid={cid} — bloqueado")
                continue
            if dry_run:
                print(f"  [dry] DELETE assoc 341: deal {did} </> company {cid}")
                assocs_removidas += 1
                continue
            # DELETE com array de tipos a remover (preserva outros labels se existirem)
            body = [{"associationCategory": "HUBSPOT_DEFINED",
                     "associationTypeId": HUBSPOT_DEAL_TO_COMPANY_TYPE_ID}]
            r3 = req("DELETE", f"/crm/v4/objects/deals/{did}/associations/companies/{cid}/labels",
                     json=body)
            # Endpoint alternativo se 404 — DELETE simples remove a associacao inteira (sem labels custom em Starter e seguro)
            if r3.status_code == 404:
                r3 = req("DELETE", f"/crm/v4/objects/deals/{did}/associations/companies/{cid}")
            if r3.status_code in (200, 204):
                assocs_removidas += 1
            else:
                erros += 1
                if erros <= 3:
                    print(f"  [erro] DELETE assoc deal {did} </> company {cid}: {r3.status_code} {r3.text[:200]}")
            time.sleep(0.05)

    suffix = " [DRY-RUN]" if dry_run else ""
    print(f"sync_parceiro_associations_criap{suffix}: assocs_criadas={assocs_criadas} | "
          f"assocs_removidas={assocs_removidas} | ja_correto={ja_correto} | "
          f"sem_parceiro_e_sem_341={pulou_sem_parceiro_e_sem_341} | "
          f"nao_criap={pulou_nao_criap} | deals_processados={deals_processados} | erros={erros}")
    return assocs_criadas + assocs_removidas


def patch_company_localizacao_via_cnpj(companies_list):
    """Auto-preenche state/city/zip da Company via BrasilAPI quando CNPJ existe e
    o campo correspondente esta vazio.

    Regra Bruno 27/04 (reuniao FGM): se executivo ja preencheu, NUNCA sobrescreve.
    So preenche o que esta em branco. Bate BrasilAPI 1x por CNPJ por execucao
    (cache local). Rate limit conservador 0.5s entre calls.

    Args:
        companies_list: lista de dicts {id, properties:{cnpj, state, city, zip, ...}}
            no formato retornado por fetch_all_companies().

    Returns:
        count de Companies atualizadas.
    """
    candidatas = []
    for c in companies_list:
        p = c.get("properties", {}) or {}
        cnpj = _normalize_cnpj(p.get("cnpj"))
        if not cnpj or len(cnpj) != 14:
            continue
        # So enriquece se ALGUM dos 3 estiver vazio (caso contrario nao precisa)
        if (p.get("state") or "").strip() and (p.get("city") or "").strip() and (p.get("zip") or "").strip():
            continue
        candidatas.append((c["id"], cnpj, p))

    if not candidatas:
        print("patch_company_localizacao_via_cnpj: 0 Companies pra enriquecer")
        return 0

    print(f"patch_company_localizacao_via_cnpj: {len(candidatas)} Companies candidatas (cnpj preenchido + algum campo de localizacao vazio)")

    cache = {}  # cnpj -> dict BrasilAPI
    atualizados = 0
    erros = 0
    sem_dados = 0

    for cid, cnpj, props_atuais in candidatas:
        if cnpj not in cache:
            # BrasilAPI tem rate limit agressivo — retry exponencial em 429.
            # 2s entre calls "frios" + backoff em 429 cobre rate sem ser lento demais.
            dados = None
            tipo_erro = None  # "sem_dados" | "exception"
            for attempt in range(4):
                try:
                    r = requests.get(
                        f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
                        timeout=15,
                    )
                    if r.status_code == 200:
                        dados = r.json()
                        break
                    if r.status_code == 429:
                        wait = 2 ** (attempt + 2)  # 4, 8, 16, 32s
                        print(f"  [429 brasilapi] cnpj={cnpj} retry em {wait}s ({attempt+1}/4)")
                        time.sleep(wait)
                        continue
                    # 404 ou outro erro nao-retryable -> CNPJ provavelmente invalido
                    tipo_erro = "sem_dados"
                    break
                except Exception as e:
                    print(f"  [brasilapi exception] cnpj={cnpj}: {e}")
                    tipo_erro = "exception"
                    break
            cache[cnpj] = dados
            if dados is None:
                if tipo_erro == "exception":
                    erros += 1
                else:
                    sem_dados += 1
            time.sleep(2)  # respeita BrasilAPI entre CNPJs distintos

        dados = cache.get(cnpj)
        if not dados:
            continue

        # So preenche campos vazios (regra "nunca sobrescrever manual")
        patch_props = {}
        if not (props_atuais.get("state") or "").strip() and dados.get("uf"):
            patch_props["state"] = dados["uf"]
        if not (props_atuais.get("city") or "").strip() and dados.get("municipio"):
            patch_props["city"] = dados["municipio"]
        if not (props_atuais.get("zip") or "").strip() and dados.get("cep"):
            patch_props["zip"] = dados["cep"]

        if not patch_props:
            continue

        r = req("PATCH", f"/crm/v3/objects/companies/{cid}", json={"properties": patch_props})
        if r.status_code in (200, 201):
            atualizados += 1
        else:
            erros += 1
            print(f"  [PATCH erro] company={cid} status={r.status_code}: {r.text[:150]}")

    print(f"patch_company_localizacao_via_cnpj: {atualizados} atualizados, {sem_dados} sem dados na BrasilAPI, {erros} erros")
    return atualizados


# ===================================================
# GOOGLE SHEETS
# ===================================================

def get_sheets_client():
    """Cliente gspread autenticado via service account."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if SERVICE_ACCOUNT_JSON:
        info = json.loads(SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    else:
        raise Exception(
            "Credenciais Google nao encontradas. "
            "Defina GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE."
        )

    return gspread.authorize(creds)


def write_to_sheets(rows, header, worksheet_name=WORKSHEET_NAME,
                    meta_label="ultima_sync_deals", meta_range="A1:C1"):
    """Sobrescreve a aba indicada com dados frescos (padrao corridas).

    worksheet_name: aba destino (default `raw_deals`).
    meta_label / meta_range: chave + intervalo na aba `_meta` pra timestamp
    (default `ultima_sync_deals` em A1:C1). Use A2:C2 pra companies.
    """
    gc = get_sheets_client()

    if not SPREADSHEET_ID:
        raise Exception("SPREADSHEET_ID nao configurado.")

    sh = gc.open_by_key(SPREADSHEET_ID)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=max(1000, len(rows) + 100), cols=len(header))

    ws.clear()
    ws.update(values=[header] + rows, range_name="A1")

    # Timestamp de ultima sync na aba _meta (se existir)
    try:
        meta = sh.worksheet("_meta")
        now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        meta.update(values=[[meta_label, now, len(rows)]], range_name=meta_range)
    except gspread.exceptions.WorksheetNotFound:
        pass

    print(f"Sheets atualizado: {len(rows)} linhas em {worksheet_name}")


# Guard rail caso valor legacy reapareça (Bruno 13/05: separação Match
# interno/externo removida do picklist HubSpot; mapping fica como defesa).
PRODUTO_META_NORMALIZE = {
    "Match interno": "Match",
    "Match externo": "Match",
}


def _produto_meta_grupo(prod: str) -> str:
    return PRODUTO_META_NORMALIZE.get(prod, prod)


def write_performance_sheet(enriched, gc):
    """Agrega deals do ano corrente por produto e faz join com metas_anuais.

    Escreve aba raw_metas_anuais com dados pre-computados — sem necessidade
    de blend no Looker Studio.

    Colunas de saida:
      produto, vendido_brl, valor_projetado_ativo, n_ganhos_ano,
      meta_anual_brl, pct_meta

    Regras (decisao Bruno 05/05):
      - vendido_brl = SUM(valor_vendido) onde e_ganho=1 AND year(closedate)=ano_corrente.
        Bate com card "Fechado no periodo". Ganhos sem closedate ficam invisiveis
        nesta tabela — cobranca via gap_closedate na Sheet de Gaps.
      - valor_projetado_ativo = SUM snapshot do pipeline ativo, sem filtro de ano.
      - n_ganhos_ano = count Ganhos com closedate no ano corrente (consistente com vendido).
      - 1 linha por produto (ano corrente). PRODUTO_META_NORMALIZE colapsa valores legacy (Match interno/externo) em "Match" como guard rail.
    """
    sh = gc.open_by_key(SPREADSHEET_ID)
    ano_corrente = str(datetime.datetime.now().year)

    # Ler metas_anuais (preenchida manualmente). Filtra ano corrente.
    # IMPORTANTE: ler UNFORMATTED — display "90.000" gera ambiguidade locale
    # (Sheets parseia como 90.0 em vez de 90000). Unformatted retorna o valor cru.
    metas_idx = {}
    anos_meta_encontrados = set()
    try:
        ws_metas = sh.worksheet("metas_anuais")
        rows = ws_metas.get("A1:D200", value_render_option="UNFORMATTED_VALUE")
        if rows:
            header = [str(c).strip() for c in rows[0]]
            try:
                idx_prod = header.index("produto")
                idx_meta = header.index("meta_anual_brl")
                idx_ano = header.index("ano")
            except ValueError:
                print("[warn] metas_anuais: header inesperado — esperando produto, meta_anual_brl, ano")
                rows = []
            for r in rows[1:] if rows else []:
                if len(r) <= max(idx_prod, idx_meta, idx_ano):
                    continue
                prod = str(r[idx_prod]).strip()
                ano = str(r[idx_ano]).strip()
                meta_raw = r[idx_meta]
                if isinstance(meta_raw, (int, float)):
                    meta_v = float(meta_raw)
                else:
                    s = str(meta_raw).replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
                    meta_v = float(s) if s else 0.0
                if prod and ano:
                    anos_meta_encontrados.add(ano)
                    if ano == ano_corrente:
                        metas_idx[prod] = meta_v
    except gspread.exceptions.WorksheetNotFound:
        print("[warn] Aba metas_anuais nao encontrada — raw_metas_anuais sem metas")

    if anos_meta_encontrados and ano_corrente not in anos_meta_encontrados:
        print(f"[warn] metas_anuais nao tem linhas para ano corrente {ano_corrente}. "
              f"Anos encontrados: {sorted(anos_meta_encontrados)}. "
              f"raw_metas_anuais sera escrito sem metas.")

    # Agregar deals (ano corrente) por produto. Guard rail: valores legacy
    # (Match interno/externo) colapsam em "Match" via _produto_meta_grupo.
    perf = defaultdict(lambda: {
        "vendido_brl": 0.0,
        "valor_projetado_ativo": 0.0,
        "n_ganhos_ano": 0,
    })
    for d in enriched:
        prod_raw = str(d.get("produto", "")).strip()
        prod = _produto_meta_grupo(prod_raw)
        if not prod:
            continue

        # projetado_ativo: snapshot independente de ano
        perf[prod]["valor_projetado_ativo"] += float(d.get("valor_projetado_ativo", 0) or 0)

        # vendido + n_ganhos: filtra por year(closedate) == ano_corrente
        if int(d.get("e_ganho", 0) or 0) == 1:
            close_str = str(d.get("closedate", "") or "")
            ano_close = close_str[:4] if len(close_str) >= 4 else ""
            if ano_close == ano_corrente:
                perf[prod]["vendido_brl"] += float(d.get("valor_vendido", 0) or 0)
                perf[prod]["n_ganhos_ano"] += 1

    # FULL OUTER JOIN: uniao de chaves de metas e de deals (so produtos)
    all_prods = sorted(set(metas_idx.keys()) | set(perf.keys()))

    rows_out = []
    for prod in all_prods:
        meta_brl = metas_idx.get(prod, 0) or 0
        p = perf.get(prod, {})
        vendido = p.get("vendido_brl", 0) or 0
        pct_meta = round(vendido / meta_brl, 4) if meta_brl else ""
        rows_out.append({
            "produto": prod,
            "vendido_brl": round(vendido, 2),
            "valor_projetado_ativo": round(p.get("valor_projetado_ativo", 0) or 0, 2),
            "n_ganhos_ano": p.get("n_ganhos_ano", 0) or 0,
            "meta_anual_brl": round(meta_brl, 2),
            "pct_meta": pct_meta,
        })

    if not rows_out:
        print("[warn] raw_metas_anuais: nenhuma linha gerada")
        return

    header = list(rows_out[0].keys())
    data = [[r[k] for k in header] for r in rows_out]
    write_to_sheets(
        data, header,
        worksheet_name="raw_metas_anuais",
        meta_label="ultima_sync_metas_anuais",
        meta_range="A3:C3",
    )


# ===================================================
# MAIN
# ===================================================

def main():
    print(f"=== Sync HubSpot -> Sheets ({datetime.datetime.now()}) ===")

    if not HUBSPOT_TOKEN:
        raise Exception("HUBSPOT_TOKEN nao configurado.")

    stages = load_stages()
    owners = load_owner_map()
    deals = fetch_all_deals()
    if not deals:
        print("Nenhum deal encontrado. Abortando.")
        return

    deal_ids = [d["id"] for d in deals]
    deal_to_company = fetch_associated_companies(deal_ids)
    companies = fetch_companies(deal_to_company.values())

    # Contato (email/telefone do incentivador — lista Rafaela 08/06): associacoes
    # deal->contato e company->contato pro fallback em 3 niveis no enrich().
    deal_to_contacts = fetch_assoc("deals", "contacts", deal_ids)
    company_to_contacts = fetch_assoc("companies", "contacts", list(deal_to_company.values()))
    all_contact_ids = (
        {c for v in deal_to_contacts.values() for c in v}
        | {c for v in company_to_contacts.values() for c in v}
    )
    contacts = fetch_contacts(sorted(all_contact_ids))

    enriched = [
        enrich(d, stages, deal_to_company, companies, owners=owners,
               deal_to_contacts=deal_to_contacts,
               company_to_contacts=company_to_contacts, contacts=contacts)
        for d in deals
    ]

    # Fase 9 (30/04): segunda passada — preenche dup_count/severidade/keep_suggestion
    # baseado em visao global de todos deals enriched. Counter eh O(N), trivial.
    sig_h1_counts = Counter(d["dup_signature_h1"] for d in enriched if d["dup_signature_h1"])
    sig_h2_counts = Counter(d["dup_signature_h2"] for d in enriched if d["dup_signature_h2"])
    sig_h2flex_counts = Counter(d["dup_signature_h2flex"] for d in enriched if d["dup_signature_h2flex"])
    sig_h3_counts = Counter(d["dup_signature_h3"] for d in enriched if d["dup_signature_h3"])
    sig_criape_counts = Counter(d["dup_signature_criape"] for d in enriched if d["dup_signature_criape"])

    for d in enriched:
        d["dup_count_h1"] = sig_h1_counts.get(d["dup_signature_h1"], 0) if d["dup_signature_h1"] else 0
        d["dup_count_h2"] = sig_h2_counts.get(d["dup_signature_h2"], 0) if d["dup_signature_h2"] else 0
        d["dup_count_h2flex"] = sig_h2flex_counts.get(d["dup_signature_h2flex"], 0) if d["dup_signature_h2flex"] else 0
        d["dup_count_h3"] = sig_h3_counts.get(d["dup_signature_h3"], 0) if d["dup_signature_h3"] else 0
        d["dup_count_criape"] = sig_criape_counts.get(d["dup_signature_criape"], 0) if d["dup_signature_criape"] else 0
        is_h1_dup = d["dup_count_h1"] >= 2
        is_h2_dup = d["dup_count_h2"] >= 2
        is_h2flex_dup = d["dup_count_h2flex"] >= 2
        is_h3_dup = d["dup_count_h3"] >= 2
        is_criape_dup = d["dup_count_criape"] >= 2
        is_h4 = d["dealname_clone_flag"] == 1
        d["e_potencial_dup"] = 1 if (is_h1_dup or is_h2_dup or is_h2flex_dup or is_h3_dup or is_criape_dup or is_h4) else 0
        if is_h1_dup or is_h2_dup or is_h4:
            d["dup_severity"] = "ALTA"
        elif is_h2flex_dup or is_h3_dup or is_criape_dup:
            d["dup_severity"] = "MEDIA"
        else:
            d["dup_severity"] = ""

    def _resolve_keep(enriched_list, sig_field):
        """Pra cada grupo de deals com mesma signature, escolhe 1 winner como
        'manter' e marca outros como 'deletar'. Logica: se algum tem clone flag,
        os SEM clone prevalecem; senao deal mais antigo (createdate ASC) ganha."""
        groups = defaultdict(list)
        for x in enriched_list:
            sig = x.get(sig_field)
            if sig:
                groups[sig].append(x)
        keep = {}
        for sig, deals_grp in groups.items():
            if len(deals_grp) < 2:
                continue
            non_clone = [x for x in deals_grp if not x["dealname_clone_flag"]]
            candidates = non_clone if non_clone else deals_grp
            candidates.sort(key=lambda x: x.get("createdate") or "9999")
            winner_id = candidates[0]["deal_id"]
            for x in deals_grp:
                keep[x["deal_id"]] = "manter" if x["deal_id"] == winner_id else "deletar"
        return keep

    # Cascata: H1 (Ganhos mesma lei mesmo ano) > H2 (mesmo aporte+closedate) > H2flex (mesmo aporte+ano) > H3 (ativos mesma lei)
    keep_h1 = _resolve_keep(enriched, "dup_signature_h1")
    keep_h2 = _resolve_keep(enriched, "dup_signature_h2")
    keep_h2flex = _resolve_keep(enriched, "dup_signature_h2flex")
    keep_h3 = _resolve_keep(enriched, "dup_signature_h3")
    keep_criape = _resolve_keep(enriched, "dup_signature_criape")
    for d in enriched:
        did = d["deal_id"]
        if did in keep_h1:
            d["dup_keep_suggestion"] = keep_h1[did]
        elif did in keep_h2:
            d["dup_keep_suggestion"] = keep_h2[did]
        elif did in keep_h2flex:
            d["dup_keep_suggestion"] = keep_h2flex[did]
        elif did in keep_h3:
            d["dup_keep_suggestion"] = keep_h3[did]
        elif d["dealname_clone_flag"] == 1:
            d["dup_keep_suggestion"] = "deletar"  # clone standalone

    n_dups = sum(d["e_potencial_dup"] for d in enriched)
    n_alta = sum(1 for d in enriched if d["dup_severity"] == "ALTA")
    n_clones = sum(d["dealname_clone_flag"] for d in enriched)
    print(f"Fase 9 dup detection: {n_dups} potenciais ({n_alta} ALTA, {n_clones} clone_flag)")

    # PATCH back: propaga derivacoes (lei_principal / linha_de_imposto_categoria)
    # + defaults produto/e_o_primeiro_match (E6 Onda A), limitado aos deals
    # modificados nas ultimas 2h.
    raw_deals_by_id = {d["id"]: d for d in deals}
    primeiro_match_map = _build_primeiro_match_map(deals, deal_to_company)
    patch_derived_back(
        enriched, raw_deals_by_id,
        deal_to_company=deal_to_company,
        primeiro_match_map=primeiro_match_map,
        lookback_hours=2,
    )

    # Default trabalhado_por="Executivo Brada" em deals com campo vazio (Gap D,
    # ata Ivan 20/04). Starter nao suporta defaultValue nativo pra picklist custom.
    patch_default_trabalhado_por(deals)

    header = list(enriched[0].keys())
    # Converter dicts em listas na ordem do header
    rows = [[r[k] for k in header] for r in enriched]

    write_to_sheets(rows, header)

    # Aba consolidado (Sprint 1): backbone único das 3 views (reporting Luciana,
    # visão 4 colunas Vitor, dashboard CRIAPE). Deriva interno/externo + comissão
    # Vendas% + projeto_key + flags de conversão/overlap. NÃO toca o CRM.
    # Isolado por try/except pra não derrubar o pipeline existente.
    try:
        cons = build_consolidado_layer(enriched, stages=stages)
        if cons:
            cons_header = list(cons[0].keys())
            cons_rows = [[r[k] for k in cons_header] for r in cons]
            write_to_sheets(
                cons_rows, cons_header,
                worksheet_name="consolidado",
                meta_label="ultima_sync_consolidado",
                meta_range="A3:C3",
            )
    except Exception as e:
        print(f"[warn] build_consolidado_layer falhou: {e}")

    # Aba raw_companies: TODAS as Companies (incluindo orfas sem Deal)
    # Desbloqueia scorecards Ato 3 Cadastro do dashboard Qualidade (23/04 tarde).
    all_companies = fetch_all_companies()

    # Fase 4 (27/04): auto-preenche state/city/zip via BrasilAPI pra Companies
    # com CNPJ + algum campo de localizacao vazio. Respeita preenchimento manual.
    # Roda ANTES de enrich_company pra a aba raw_companies sair ja com dados frescos.
    if all_companies:
        patch_company_localizacao_via_cnpj(all_companies)
        # Re-fetch pra pegar valores recem-patchados (Companies eh objeto leve, tolerable)
        all_companies = fetch_all_companies()

        # 05/05: espelha Company.valor_total_do_diagnostico no Deal lider de cada Company.
        # Resolve UX da Jessica — cards nativos da view de Deal somam o diagnostico
        # da empresa sem duplicar (1 Deal lider por Company carrega o valor; outros = 0).
        try:
            ganho_stages_incentivador_main = (
                {sid for sid, info in stages.items()
                 if info.get("is_closed")
                 and info.get("probability") == "1.0"
                 and info.get("pipeline_id") == "default"}
                | {s for s in POS_VENDA_STAGES}
            )

            # 11/05: auto-preenche Company.valor_total_do_diagnostico via SOMA dos
            # aportes de Ganhos Incentivador. Roda ANTES do espelho pro Deal lider
            # — assim o espelho ja pega o valor recem-preenchido na mesma rodada.
            try:
                patch_company_diag_from_aporte_ganho(
                    companies_list=all_companies,
                    deals_list=deals,
                    deal_to_company=deal_to_company,
                    ganho_stages_incentivador=ganho_stages_incentivador_main,
                )
                # Re-fetch pra Deal lider pegar VTD recem-preenchido
                all_companies = fetch_all_companies()
            except Exception as e:
                print(f"[warn] patch_company_diag_from_aporte_ganho falhou: {e}")

            sync_diagnostico_para_deal_lider(
                companies_list=all_companies,
                deals_list=deals,
                deal_to_company=deal_to_company,
                ganho_stages_incentivador=ganho_stages_incentivador_main,
            )
        except Exception as e:
            print(f"[warn] sync_diagnostico_para_deal_lider falhou: {e}")

        # 05/05: espelha valor_do_aporte -> amount pra header/kanban refletirem
        # o valor real (em vez de "—"). Property nativa do HubSpot Starter nao
        # permite trocar qual campo aparece no header/totalizadores.
        try:
            sync_amount_para_aporte(deals)
        except Exception as e:
            print(f"[warn] sync_amount_para_aporte falhou: {e}")

        # CRIAP rollups (Sprint 0 / S0.4 14/05) - Caminho 1.
        # Try/except isola falha do pipeline existente Brada.
        try:
            compute_criap_rollups(
                companies_list=all_companies or [],
                deals_list=deals,
                deal_to_company=deal_to_company,
            )
        except Exception as e:
            print(f"[warn] compute_criap_rollups falhou: {e}")

        try:
            sync_parceiro_cnpj_criap(deals, all_companies or [])
        except Exception as e:
            print(f"[warn] sync_parceiro_cnpj_criap falhou: {e}")

        try:
            sync_parceiro_nome_criap(deals, all_companies or [])
        except Exception as e:
            print(f"[warn] sync_parceiro_nome_criap falhou: {e}")

        try:
            sync_nome_projeto_criap(deals)
        except Exception as e:
            print(f"[warn] sync_nome_projeto_criap falhou: {e}")

        try:
            sync_parceiro_associations_criap(deals, deal_to_company)
        except Exception as e:
            print(f"[warn] sync_parceiro_associations_criap falhou: {e}")

    if all_companies:
        num_deals_by_cid = defaultdict(int)
        for cid in deal_to_company.values():
            if cid:
                num_deals_by_cid[str(cid)] += 1

        # Flags de estado do pipeline por Company (Fase 6 28/04).
        # Conta deals ativos/ganhos/perdidos vinculados a cada Company pra alimentar
        # `tem_deal_ativo` no Hero PROJETADO. Usa `enriched` (que ja tem e_ativo/e_ganho/e_perdido).
        flags_by_cid = defaultdict(lambda: {"ativo": 0, "ganho": 0, "perdido": 0})
        for d in enriched:
            cid = d.get("company_id")
            if not cid:
                continue
            cid_str = str(cid)
            if d.get("e_ativo"):
                flags_by_cid[cid_str]["ativo"] += 1
            if d.get("e_ganho"):
                flags_by_cid[cid_str]["ganho"] += 1
            if d.get("e_perdido"):
                flags_by_cid[cid_str]["perdido"] += 1

        enriched_companies = [enrich_company(c, num_deals_by_cid, flags_by_cid) for c in all_companies]
        comp_header = list(enriched_companies[0].keys())
        comp_rows = [[r[k] for k in comp_header] for r in enriched_companies]
        write_to_sheets(
            comp_rows, comp_header,
            worksheet_name="raw_companies",
            meta_label="ultima_sync_companies",
            meta_range="A2:C2",
        )

    # Fase 5 (27/04): popula Sheet de Gaps por Executivo. Reusa fetch ja
    # feito acima — zero requests extras ao HubSpot.
    try:
        from popular_gaps_sheet import popular_gaps_sheet
        # Vendido = closed-won + pós-venda (Incentivador e Proponente).
        # Garante que gaps cobrem todos os deals já vendidos, independente do pipeline.
        ganho_stages = (
            {sid for sid, info in stages.items()
             if info.get("is_closed") and info.get("probability") == "1.0"}
            | POS_VENDA_STAGES
            | PROPONENTE_POS_VENDA_STAGES
        )
        perdido_stages = {sid for sid, info in stages.items()
                          if info.get("is_closed") and info.get("probability") == "0.0"}
        # Diagnóstico só vale pra pipeline Incentivador (Ivan 04/05).
        # Gap 12 não pode cobrar diagnóstico de Ganho Proponente.
        ganho_stages_incentivador = {sid for sid, info in stages.items()
                                     if info.get("is_closed")
                                     and info.get("probability") == "1.0"
                                     and info.get("pipeline_id") == "default"}
        gc = get_sheets_client()
        popular_gaps_sheet(
            deals=deals,
            companies=all_companies or [],
            deal_to_company=deal_to_company,
            owners=owners or {},
            ganho_stages=ganho_stages,
            perdido_stages=perdido_stages,
            ganho_stages_incentivador=ganho_stages_incentivador,
            gc=gc,
        )
    except Exception as e:
        # Nao falha o sync por causa de gaps — log e segue
        print(f"[warn] Fase 5 (gaps sheet) falhou: {e}")

    # raw_metas_anuais: join deals (ano x produto) × metas_anuais pre-computado.
    # Decisao Ivan 04/05: medicao so anual (sazonalidade IR/ISS impede trimestre).
    # Substitui raw_performance/metas_mensais (descontinuados).
    try:
        gc_perf = get_sheets_client()
        write_performance_sheet(enriched, gc_perf)
    except Exception as e:
        print(f"[warn] Performance sheet falhou: {e}")

    # Resumo
    ativos = sum(1 for r in enriched if r["e_ativo"])
    ganhos = sum(1 for r in enriched if r["e_ganho"])
    perdidos = sum(1 for r in enriched if r["e_perdido"])
    valor_ativo = sum(r["valor_projetado_ativo"] for r in enriched)
    valor_vendido = sum(r["valor_vendido"] for r in enriched)
    print(f"Ativos: {ativos} | Ganhos: {ganhos} | Perdidos: {perdidos}")
    print(f"Pipeline ativo: R$ {valor_ativo:,.2f}")
    print(f"Vendido: R$ {valor_vendido:,.2f}")
    print("=== Concluido ===")


if __name__ == "__main__":
    main()
