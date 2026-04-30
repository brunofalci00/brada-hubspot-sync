"""
Sprint 2 follow-up (30/04) — gera aba "Resolucoes CNPJ — Ivan" na Sheet
Brada_Comercial_Gaps_por_Executivo. 1 linha por Company MEDIA do Sprint 2,
3 candidatos pre-rankeados, dropdown de escolha. Reduz friccao: Ivan le,
escolhe, fecha em <30min.

Workflow:
  1. Le sprint2_media.csv (output process_sprint2.py)
  2. Joina com gaps_companies_sem_cnpj_*.csv (owner, aporte, ganhos)
  3. Top 3 candidatos pre-rankeados (mesma logica de escolher_cnpj
     mas mantendo 3 melhores em vez de 1 so)
  4. BrasilAPI lookup pra cada candidato (UF, razao, situacao, matriz/filial)
  5. Escreve aba na Sheet com hyperlinks pra HubSpot e Casa dos Dados
  6. Sort por aporte DESC (criticos primeiro)
  7. Data validation: dropdown na coluna ESCOLHA

Uso:
    python gerar_resolucoes_cnpj.py
"""

import csv
import os
import time
from collections import defaultdict

import gspread

from process_sprint2 import (
    brasilapi_lookup, load_cache, save_cache, validar_cnpj,
)
from sync import get_sheets_client

GAPS_SHEET_ID = os.environ.get("GAPS_SHEET_ID", "1GQe6ksTrQnoWNtFm2BF3WblkHiaNGdKK7ycf1qx-oSs")
ABA_NAME = "Resolucoes CNPJ — Ivan"
DIR = os.path.dirname(__file__)
GAPS_CSV = os.path.join(DIR, "gaps_companies_sem_cnpj_20260429.csv")
MEDIA_CSV = os.path.join(DIR, "sprint2_media.csv")


# ===================================================
# DADOS DE SUPORTE
# ===================================================

def load_companies_index():
    """Index company_id -> linha original do gaps CSV."""
    idx = {}
    with open(GAPS_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            idx[r["company_id"]] = r
    return idx


def cdd_url(cnpj, razao):
    """URL Casa dos Dados padrao /solucao/cnpj/{slug}-{cnpj}."""
    slug = (razao or "empresa").lower()
    slug = "".join(c if c.isalnum() else "-" for c in slug)
    slug = "-".join(t for t in slug.split("-") if t)
    return f"https://casadosdados.com.br/solucao/cnpj/{slug}-{cnpj}"


def candidate_info(cnpj, cache):
    """Retorna dict com info pra exibir inline na celula."""
    if not validar_cnpj(cnpj):
        return None
    info = brasilapi_lookup(cnpj, cache)
    if not info:
        return {
            "cnpj": cnpj, "razao": "(?)", "uf": "", "sit": "?",
            "tipo": "", "url": cdd_url(cnpj, ""),
        }
    razao = info.get("razao_social") or ""
    return {
        "cnpj": cnpj,
        "razao": razao,
        "uf": info.get("uf") or "",
        "sit": info.get("descricao_situacao_cadastral") or "",
        "tipo": info.get("descricao_identificador_matriz_filial") or "",
        "url": cdd_url(cnpj, razao),
    }


def fmt_cnpj(cnpj):
    """Formata 14 digitos -> XX.XXX.XXX/XXXX-XX."""
    if not cnpj or len(cnpj) != 14:
        return cnpj or ""
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


def fmt_candidate_cell(info):
    """Retorna formula HYPERLINK pra celula de candidato."""
    if not info:
        return ""
    parts = [fmt_cnpj(info["cnpj"]), info["razao"][:35]]
    extras = [x for x in (info["uf"], info["sit"], info["tipo"]) if x]
    if extras:
        parts.append(" · ".join(extras))
    text = " · ".join(parts).replace('"', "'")
    return f'=HYPERLINK("{info["url"]}";"{text}")'


def fmt_company_cell(name, link):
    """HYPERLINK pra Company HubSpot."""
    label = (name or "(sem nome)").replace('"', "'")[:60]
    return f'=HYPERLINK("{link}";"{label}")'


# ===================================================
# RANKING DOS CANDIDATOS
# ===================================================

def top3_candidates(candidatos_brutos):
    """Dado csv string com ate 5 CNPJs, retorna top 3 com prioridade pra matriz.

    Logica:
      1. Filtra invalidos (checksum)
      2. Agrupa por raiz; raiz com mais hits primeiro
      3. Dentro de cada raiz, matriz (0001-XX) primeiro
      4. Achata em lista de no max 3
    """
    cnpjs = [c.strip() for c in (candidatos_brutos or "").split(";") if c.strip()]
    cnpjs = [c for c in cnpjs if validar_cnpj(c)]
    if not cnpjs:
        return []

    raizes = defaultdict(list)
    for c in cnpjs:
        raizes[c[:8]].append(c)

    raizes_ordenadas = sorted(raizes.items(), key=lambda x: -len(x[1]))
    out = []
    for raiz, cs in raizes_ordenadas:
        cs_sorted = sorted(cs, key=lambda c: 0 if c[8:12] == "0001" else 1)
        for c in cs_sorted:
            if c not in out:
                out.append(c)
            if len(out) >= 3:
                return out
    return out[:3]


# ===================================================
# MAIN
# ===================================================

def main():
    cache = load_cache()
    companies_idx = load_companies_index()
    with open(MEDIA_CSV, encoding="utf-8") as f:
        media_rows = list(csv.DictReader(f))

    print(f"Processing {len(media_rows)} MEDIA companies...")
    rows_out = []
    for r in media_rows:
        cid = r["company_id"]
        gap = companies_idx.get(cid, {})
        top3 = top3_candidates(r.get("candidatos_brutos", ""))
        infos = [candidate_info(c, cache) for c in top3]
        while len(infos) < 3:
            infos.append(None)

        aporte = float(gap.get("valor_aporte_total") or 0)
        ganhos = int(gap.get("num_deals_ganho") or 0)
        aporte_str = f'R$ {aporte:,.0f} · {ganhos} Ganho(s)' if aporte > 0 else f'{ganhos} Ganho(s)'

        rows_out.append({
            "_aporte": aporte,
            "company": fmt_company_cell(r["name_hubspot"], gap.get("link_hubspot", "")),
            "aporte_ganhos": aporte_str,
            "owner": gap.get("owner_executivo", ""),
            "cand1": fmt_candidate_cell(infos[0]),
            "cand2": fmt_candidate_cell(infos[1]),
            "cand3": fmt_candidate_cell(infos[2]),
            "escolha": "",
            "outro_cnpj": "",
            "notas": "",
            "company_id": cid,
            "_cnpj1": infos[0]["cnpj"] if infos[0] else "",
            "_cnpj2": infos[1]["cnpj"] if infos[1] else "",
            "_cnpj3": infos[2]["cnpj"] if infos[2] else "",
        })
        time.sleep(0.2)  # BrasilAPI throttle

    rows_out.sort(key=lambda r: -r["_aporte"])
    save_cache(cache)

    print(f"Writing to Sheet aba '{ABA_NAME}'...")
    gc = get_sheets_client()
    sh = gc.open_by_key(GAPS_SHEET_ID)
    try:
        aba = sh.worksheet(ABA_NAME)
        aba.clear()
    except gspread.exceptions.WorksheetNotFound:
        aba = sh.add_worksheet(title=ABA_NAME, rows=len(rows_out) + 10, cols=15)

    header = [
        "Company (link HubSpot)", "Aporte · Ganhos", "Owner",
        "Cand 1 ⭐ (recomendado)", "Cand 2", "Cand 3",
        "ESCOLHA", "Outro CNPJ (14 dig)", "Notas",
        "company_id", "_cnpj1", "_cnpj2", "_cnpj3",
    ]
    matrix = [header]
    for r in rows_out:
        matrix.append([
            r["company"], r["aporte_ganhos"], r["owner"],
            r["cand1"], r["cand2"], r["cand3"],
            r["escolha"], r["outro_cnpj"], r["notas"],
            r["company_id"], r["_cnpj1"], r["_cnpj2"], r["_cnpj3"],
        ])
    aba.update("A1", matrix, value_input_option="USER_ENTERED")

    # Header bold + cor laranja Brada
    aba.format("A1:M1", {
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "backgroundColor": {"red": 0.77, "green": 0.36, "blue": 0.07},
    })
    aba.freeze(rows=1)

    # Esconde colunas auxiliares (J-M)
    sheet_id = aba.id
    sh.batch_update({"requests": [
        # Data validation dropdown na coluna ESCOLHA (G, index 6)
        {"setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1, "endRowIndex": len(matrix),
                "startColumnIndex": 6, "endColumnIndex": 7,
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [
                        {"userEnteredValue": "Cand 1 ⭐"},
                        {"userEnteredValue": "Cand 2"},
                        {"userEnteredValue": "Cand 3"},
                        {"userEnteredValue": "Outro"},
                        {"userEnteredValue": "Não é nenhum"},
                        {"userEnteredValue": "Pular"},
                    ],
                },
                "showCustomUi": True, "strict": False,
            },
        }},
        # Larguras: Company larga (340), Cand 1-3 largas (380), demais ajustadas
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 340}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": 3, "endIndex": 6},
            "properties": {"pixelSize": 380}, "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": 6, "endIndex": 7},
            "properties": {"pixelSize": 130}, "fields": "pixelSize",
        }},
        # Esconde colunas J-M (auxiliares pra script extrair escolha)
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": 9, "endIndex": 13},
            "properties": {"hiddenByUser": True}, "fields": "hiddenByUser",
        }},
    ]})

    print(f"OK - {len(rows_out)} linhas escritas em '{ABA_NAME}'")
    print(f"Link: https://docs.google.com/spreadsheets/d/{GAPS_SHEET_ID}")


if __name__ == "__main__":
    main()
