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
    "hs_date_entered_current_stage",
    "e_o_primeiro_match",
    "produto",
    "valor_oportunidade",
    "origem_lead",
    "status_contato",
    "lei_principal",  # criado em E1 - puxa do HubSpot, argmax vira fallback
    "linha_de_imposto_categoria",  # criado em E1 (IR/ICMS/ISS)
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

# Picklist do HubSpot retorna o value interno na Search API (ex: "rouanet"),
# nao o label ("Rouanet"). Convertemos pra manter output consistente com argmax.
LEI_PICKLIST_VALUE_TO_LABEL = {
    "rouanet": "Rouanet",
    "esporte_federal": "Esporte Federal",
    "esporte_estadual": "Esporte Estadual",
    "lei_do_bem": "Lei do Bem",
    "cultura_estadual": "Cultura Estadual",
    "cultura_municipal": "Cultura Municipal",
    "fia": "FIA (Crianca e Adolescente)",
    "fundo_idoso": "Fundo do Idoso",
    "reciclagem": "Reciclagem",
    "pronas": "PRONAS",
    "pronon": "PRONON",
}
LEI_LABEL_TO_PICKLIST_VALUE = {v: k for k, v in LEI_PICKLIST_VALUE_TO_LABEL.items()}

CATEGORIA_PICKLIST_VALUE_TO_LABEL = {"ir": "IR", "icms": "ICMS", "iss": "ISS"}
CATEGORIA_LABEL_TO_PICKLIST_VALUE = {v: k for k, v in CATEGORIA_PICKLIST_VALUE_TO_LABEL.items()}

# Normalizacao cosmetica do campo produto (value HubSpot -> label).
# Mantem output do Sheet consistente independente de preenchimento manual vs. inferencia.
PRODUTO_PICKLIST_VALUE_TO_LABEL = {
    "match": "Match",
    "elaboracao": "Elaboracao",
    "aprovai": "AprovAI",
    "customizacao": "Customizacao",
    "prestacao": "Prestacao",
}

COMPANY_PROPERTIES = [
    "name",
    "cnpj",
    "origem",
    "domain",
    "industry",
    "state",  # UF - E2 popula via BrasilAPI
    "municipio",
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


# ===================================================
# ENRIQUECIMENTO
# ===================================================

def enrich(deal, stages, deal_to_company, companies):
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

    entered_stage = parse_dt(p.get("hs_date_entered_current_stage"))
    dias_no_stage = (now - entered_stage).days if entered_stage else None

    company_id = deal_to_company.get(deal_id)
    comp = companies.get(str(company_id), {}) if company_id else {}

    produto_hubspot_value = p.get("produto") or ""
    produto_hubspot_label = PRODUTO_PICKLIST_VALUE_TO_LABEL.get(produto_hubspot_value, "")
    produto = produto_hubspot_label or ("Match" if pipeline_nome == "Incentivador" else "Elaboracao")
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
        "company_origem": comp.get("origem", ""),
        "company_industry": comp.get("industry", ""),
        "company_state": comp.get("state", "") or "(em preenchimento)",
        "company_municipio": comp.get("municipio", ""),
        "company_razao_social": comp.get("razao_social", ""),
        # Link
        "link_hubspot": f"https://app.hubspot.com/contacts/{PORTAL_ID}/deal/{deal_id}",
    }


# ===================================================
# PATCH BACK (lei_principal / linha_de_imposto_categoria)
# ===================================================

def patch_derived_back(deals_enriched, raw_deals_by_id, lookback_hours=2):
    """
    Captura movimento continuo do comercial: quando executivo muda valor_lei_X,
    a derivacao argmax/categoria no enrich() muda. Esta funcao propaga de volta
    pro HubSpot, so em deals modificados nas ultimas N horas (reduz blast radius
    e carga na API).

    Regras:
    - So faz PATCH se derivacao ≠ valor atual no HubSpot
    - Nunca sobrescreve valor existente com '(sem ...)' / vazio
    - Converte label interno ('Rouanet') pro picklist value do HubSpot ('rouanet')
    """
    agora = datetime.datetime.now(datetime.timezone.utc)
    cutoff = agora - datetime.timedelta(hours=lookback_hours)
    atualizados = 0
    erros = 0

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

        lei_atual = (props.get("lei_principal") or "").lower()
        categoria_atual = (props.get("linha_de_imposto_categoria") or "").lower()

        patch_payload = {}
        if lei_value_novo and lei_value_novo != lei_atual:
            patch_payload["lei_principal"] = lei_value_novo
        if categoria_value_novo and categoria_value_novo != categoria_atual:
            patch_payload["linha_de_imposto_categoria"] = categoria_value_novo

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

    print(f"PATCH back: {atualizados} deals atualizados, {erros} erros (lookback {lookback_hours}h)")
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


def write_to_sheets(rows, header):
    """Sobrescreve a aba raw_deals com os dados frescos (padrao corridas)."""
    gc = get_sheets_client()

    if not SPREADSHEET_ID:
        raise Exception("SPREADSHEET_ID nao configurado.")

    sh = gc.open_by_key(SPREADSHEET_ID)

    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=max(1000, len(rows) + 100), cols=len(header))

    ws.clear()
    ws.update(values=[header] + rows, range_name="A1")

    # Timestamp de ultima sync na aba _meta (se existir)
    try:
        meta = sh.worksheet("_meta")
        now = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        meta.update(values=[["ultima_sync_deals", now, len(rows)]], range_name="A1:C1")
    except gspread.exceptions.WorksheetNotFound:
        pass

    print(f"Sheets atualizado: {len(rows)} linhas em {WORKSHEET_NAME}")


# ===================================================
# MAIN
# ===================================================

def main():
    print(f"=== Sync HubSpot -> Sheets ({datetime.datetime.now()}) ===")

    if not HUBSPOT_TOKEN:
        raise Exception("HUBSPOT_TOKEN nao configurado.")

    stages = load_stages()
    deals = fetch_all_deals()
    if not deals:
        print("Nenhum deal encontrado. Abortando.")
        return

    deal_ids = [d["id"] for d in deals]
    deal_to_company = fetch_associated_companies(deal_ids)
    companies = fetch_companies(deal_to_company.values())

    enriched = [enrich(d, stages, deal_to_company, companies) for d in deals]

    # PATCH back: propaga derivacoes (lei_principal / linha_de_imposto_categoria)
    # de volta pro HubSpot, limitado aos deals modificados nas ultimas 2h.
    raw_deals_by_id = {d["id"]: d for d in deals}
    patch_derived_back(enriched, raw_deals_by_id, lookback_hours=2)

    header = list(enriched[0].keys())
    # Converter dicts em listas na ordem do header
    rows = [[r[k] for k in header] for r in enriched]

    write_to_sheets(rows, header)

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
