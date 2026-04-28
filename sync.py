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
import time
from collections import defaultdict

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
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    os.path.join(os.path.dirname(__file__), "..", "service-account-key.json"),
)

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
    "valor_total_do_diagnostico",  # valor projetado pos diagnostico (ja existe)
    "data_da_realizacao_do_diagnostico",
    "data_do_aporte",
    "executivo_responsavel",
    "executivo_match",  # OWNER do executivo Brada especifico no deal (E1)
    "trabalhado_por",  # "Executivo Brada" vs "Automatize" (E1) - chave remuneracao
    "motivo_de_perda",
    "nome_do_proponente",
    "nome_do_projeto",
    "createdate",
    "closedate",
    "hs_lastmodifieddate",
    "hs_v2_date_entered_current_stage",  # v2 preenche pra deals criados no stage (v1 só quando move)
    "e_o_primeiro_match",
    "produto",
    "valor_oportunidade",
    "origem_lead",
    "status_contato",
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
    # Legado lowercase (fallback)
    "match": "Match",
    "elaboracao": "Elaboração",
    "aprovai": "AprovAI",
    "customizacao": "Customização",
    "prestacao": "Prestação",
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
    """Retorna {deal_id: company_id}."""
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
            if deal_id and tos:
                deal_to_company[deal_id] = tos[0].get("toObjectId")
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

def enrich(deal, stages, deal_to_company, companies, owners=None):
    p = deal.get("properties", {}) or {}
    deal_id = deal["id"]
    stage_id = p.get("dealstage") or ""
    stage_info = stages.get(stage_id, {})
    pipeline_nome = stage_info.get("pipeline_nome", "")
    stage_nome = stage_info.get("nome", stage_id)

    is_closed = stage_info.get("is_closed", False)
    prob = stage_info.get("probability", "")
    e_ganho = 1 if (is_closed and prob == "1.0") else 0
    e_perdido = 1 if (is_closed and prob == "0.0") else 0
    e_ativo = 1 if not is_closed else 0

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
        # Contexto
        "nome_do_proponente": p.get("nome_do_proponente", ""),
        "nome_do_projeto": p.get("nome_do_projeto", ""),
        # Datas
        "createdate": p.get("createdate", ""),
        "closedate": p.get("closedate", ""),
        "data_da_realizacao_do_diagnostico": p.get("data_da_realizacao_do_diagnostico", ""),
        "data_do_aporte": p.get("data_do_aporte", ""),
        "ano_criacao": ano_criacao,
        "mes_criacao": mes_criacao,
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
        # Link
        "link_hubspot": f"https://app.hubspot.com/contacts/{PORTAL_ID}/deal/{deal_id}",
    }


def enrich_company(company, num_deals_by_cid):
    """Monta dict canônico para aba raw_companies do Sheet.

    Inclui:
    - company_id, company_name
    - cnpj (cru) + cnpj_efetivo (so digitos — chave de agregação)
    - domain, industry, origem, razao_social, createdate
    - state (normalizado pra sigla UF), municipio
    - num_deals_vinculados — contagem de Deals com essa Company
    """
    p = company.get("properties", {}) or {}
    cid = company["id"]
    cnpj_raw = p.get("cnpj", "") or ""
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
    }


# ===================================================
# PATCH BACK (lei_principal / linha_de_imposto_categoria)
# ===================================================

STAGES_GANHO = {"1253324968", "1253441207"}  # Incentivador + Proponente
PIPELINE_TO_PRODUTO = {"default": "Match", "839644419": "Elaboração"}  # value==label validado 22/04

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

    enriched = [enrich(d, stages, deal_to_company, companies, owners=owners) for d in deals]

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

    if all_companies:
        num_deals_by_cid = defaultdict(int)
        for cid in deal_to_company.values():
            if cid:
                num_deals_by_cid[str(cid)] += 1
        enriched_companies = [enrich_company(c, num_deals_by_cid) for c in all_companies]
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
        ganho_stages = {sid for sid, info in stages.items()
                        if info.get("is_closed") and info.get("probability") == "1.0"}
        perdido_stages = {sid for sid, info in stages.items()
                          if info.get("is_closed") and info.get("probability") == "0.0"}
        gc = get_sheets_client()
        popular_gaps_sheet(
            deals=deals,
            companies=all_companies or [],
            deal_to_company=deal_to_company,
            owners=owners or {},
            ganho_stages=ganho_stages,
            perdido_stages=perdido_stages,
            gc=gc,
        )
    except Exception as e:
        # Nao falha o sync por causa de gaps — log e segue
        print(f"[warn] Fase 5 (gaps sheet) falhou: {e}")

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
