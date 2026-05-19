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
    # CRIAP (Sprint 0 / S0.4 — Caminho 1 reuso Proponente)
    "projeto_beneficiario_criap",
    "origem_deal_criap",
    "parceiro_indicador_criap",
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
    "CRIAP": "CRIAP",  # Sprint 0 / S0.3 14/05: discrimina deals CRIAP dentro do pipeline Proponente
    # Legado lowercase (fallback)
    "match": "Match",
    "elaboracao": "Elaboração",
    "aprovai": "AprovAI",
    "customizacao": "Customização",
    "prestacao": "Prestação",
    "lei do bem": "Lei do bem",
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
    # CRIAP (Sprint 0 / S0.4 — Caminho 1)
    "papel_criap",  # multi-select: patrocinador, parceiro_indicador
    "criap_total_aporte_2026",  # AUTO sync.py via compute_criap_rollups
    "criap_count_negocios_ativos",  # AUTO
    "criap_count_negocios_ganhos",  # AUTO
    "criap_projetos_apoiados_2026",  # AUTO (CSV)
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

    dn_low = (p.get("dealname") or "").lower()
    dealname_clone_flag = 1 if any(
        t in dn_low for t in ["(clone)", "(copia)", "(copy)", "_copy", "(cópia)"]
    ) else 0

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
        "ano_fechamento": ano_fechamento,
        "mes_fechamento": mes_fechamento,
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
        # Flags de estado do pipeline (1 se Company tem >=1 deal naquele estado).
        # Usadas no Hero "PROJETADO" pra filtrar Companies com pipeline ativo.
        "tem_deal_ativo": 1 if flags.get("ativo", 0) > 0 else 0,
        "tem_deal_ganho": 1 if flags.get("ganho", 0) > 0 else 0,
        "tem_deal_perdido": 1 if flags.get("perdido", 0) > 0 else 0,
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

# CRIAP (Sprint 0 / S0.4 14/05 — Caminho 1 reuso pipeline Proponente)
# Filtro CRIAP em qualquer rollup: deal.pipeline == PROPONENTE_PIPELINE_ID AND deal.produto == CRIAP_PRODUTO_VALUE
PROPONENTE_PIPELINE_ID = "839644419"
CRIAP_PRODUTO_VALUE = "CRIAP"      # ASCII puro
CRIAP_GANHO_STAGE_ID = "1246571362"   # = "Fechado" no Proponente (isClosed=true, prob=1.0)
CRIAP_PERDIDO_STAGE_ID = "1246571364"  # = "Perdido" no Proponente
# NOTA: stage "Ganho " (com 2 espacos, id 1253441207) tem isClosed=false. NAO usar como
# fechado CRIAP. Usar CRIAP_GANHO_STAGE_ID (Fechado) hard-coded. Bug documentado em
# CRIAP_CONFIGURACAO_COMPLETA.md apendice C.

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
    """Calcula 4 props rollup CRIAP no Company-level (Sprint 0 / S0.4 14/05).

    Caminho 1: deals CRIAP vivem no pipeline Proponente com produto='CRIAP'.
    Filtro duplo: deal.pipeline == PROPONENTE_PIPELINE_ID AND deal.produto == 'CRIAP'.

    Agregacao dupla: Company aparece como
      (a) patrocinador via deal_to_company (associacao primaria)
      (b) parceiro indicador via deal.parceiro_indicador_criap (company_id em string)

    4 props calculadas:
      - criap_total_aporte_2026: soma valor_do_aporte de Ganhos 2026
      - criap_count_negocios_ativos: count deals nao-fechados (stage != Ganho/Perdido)
      - criap_count_negocios_ganhos: count deals em Ganho (qualquer data)
      - criap_projetos_apoiados_2026: CSV projeto_beneficiario_criap distintos de Ganhos 2026

    Idempotente: PATCH so se valor mudou. Batch 100/100.
    Padrao: espelha patch_company_diag_from_aporte_ganho.
    """
    deal_props_idx = {d["id"]: d.get("properties", {}) or {} for d in deals_list}

    # Filtra deals CRIAP (pipeline+produto)
    criap_deal_ids = set()
    for did, p in deal_props_idx.items():
        if p.get("pipeline") == PROPONENTE_PIPELINE_ID and p.get("produto") == CRIAP_PRODUTO_VALUE:
            criap_deal_ids.add(did)

    # Index: company_id -> set(deal_ids) onde Company aparece como patrocinador OU parceiro
    by_company = defaultdict(set)
    for did in criap_deal_ids:
        p = deal_props_idx[did]
        cid_patroc = deal_to_company.get(did)
        if cid_patroc:
            by_company[str(cid_patroc)].add(did)
        cid_parceiro = (p.get("parceiro_indicador_criap") or "").strip()
        if cid_parceiro:
            by_company[str(cid_parceiro)].add(did)

    inputs = []
    sem_deals_criap = 0

    for c in companies_list:
        cid = str(c.get("id") or "")
        deal_ids = by_company.get(cid, set())
        if not deal_ids:
            sem_deals_criap += 1
            continue

        total_aporte_2026 = 0.0
        count_ativos = 0
        count_ganhos = 0
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

            if stage == CRIAP_GANHO_STAGE_ID:
                count_ganhos += 1
                if close.startswith("2026"):
                    total_aporte_2026 += valor
                    if projeto:
                        projetos_2026.add(projeto)
            elif stage not in (CRIAP_GANHO_STAGE_ID, CRIAP_PERDIDO_STAGE_ID):
                count_ativos += 1

        # Comparar com valor atual da Company antes de PATCH (idempotencia)
        atual = c.get("properties", {}) or {}
        novo = {
            "criap_total_aporte_2026": str(int(total_aporte_2026)),
            "criap_count_negocios_ativos": str(count_ativos),
            "criap_count_negocios_ganhos": str(count_ganhos),
            "criap_projetos_apoiados_2026": ",".join(sorted(projetos_2026)),
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
    patches = 0
    pulou_correto = 0
    pulou_aporte_vazio = 0
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

        if aporte <= 0:
            pulou_aporte_vazio += 1
            continue

        if abs(amount_atual - aporte) < 0.01:
            pulou_correto += 1
            continue

        r = req("PATCH", f"/crm/v3/objects/deals/{did}",
                json={"properties": {"amount": str(aporte)}})
        if r.status_code == 200:
            patches += 1
        else:
            erros += 1
            if erros <= 3:
                print(f"  [erro] PATCH amount deal {did}: {r.status_code} {r.text[:150]}")
        time.sleep(0.05)

    print(f"sync_amount_para_aporte: patches={patches} | ja_correto={pulou_correto} | "
          f"aporte_vazio={pulou_aporte_vazio} | erros={erros}")
    return patches


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

        cnpj_correto = cnpj_by_company.get(parceiro_id, "")
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
          f"sem_parceiro={pulou_sem_parceiro} | nao_criap={pulou_nao_criap} | erros={erros}")
    return patches


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

    enriched = [enrich(d, stages, deal_to_company, companies, owners=owners) for d in deals]

    # Fase 9 (30/04): segunda passada — preenche dup_count/severidade/keep_suggestion
    # baseado em visao global de todos deals enriched. Counter eh O(N), trivial.
    sig_h1_counts = Counter(d["dup_signature_h1"] for d in enriched if d["dup_signature_h1"])
    sig_h2_counts = Counter(d["dup_signature_h2"] for d in enriched if d["dup_signature_h2"])
    sig_h2flex_counts = Counter(d["dup_signature_h2flex"] for d in enriched if d["dup_signature_h2flex"])
    sig_h3_counts = Counter(d["dup_signature_h3"] for d in enriched if d["dup_signature_h3"])

    for d in enriched:
        d["dup_count_h1"] = sig_h1_counts.get(d["dup_signature_h1"], 0) if d["dup_signature_h1"] else 0
        d["dup_count_h2"] = sig_h2_counts.get(d["dup_signature_h2"], 0) if d["dup_signature_h2"] else 0
        d["dup_count_h2flex"] = sig_h2flex_counts.get(d["dup_signature_h2flex"], 0) if d["dup_signature_h2flex"] else 0
        d["dup_count_h3"] = sig_h3_counts.get(d["dup_signature_h3"], 0) if d["dup_signature_h3"] else 0
        is_h1_dup = d["dup_count_h1"] >= 2
        is_h2_dup = d["dup_count_h2"] >= 2
        is_h2flex_dup = d["dup_count_h2flex"] >= 2
        is_h3_dup = d["dup_count_h3"] >= 2
        is_h4 = d["dealname_clone_flag"] == 1
        d["e_potencial_dup"] = 1 if (is_h1_dup or is_h2_dup or is_h2flex_dup or is_h3_dup or is_h4) else 0
        if is_h1_dup or is_h2_dup or is_h4:
            d["dup_severity"] = "ALTA"
        elif is_h2flex_dup or is_h3_dup:
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
