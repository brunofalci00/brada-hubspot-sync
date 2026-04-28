"""
Fase 5 (27/04) — Popula Sheet de Gaps por Executivo.

Computa 12 tipos de gap (Deal + Company), agrupa por owner, escreve numa
Sheet separada (1 aba "Resumo" + 1 aba por executivo).

Filtro Bruno 27/04 (opcao B): so reporta Companies com >=1 deal associado.
Companies sem deal sao registros legados/importados em massa que nao
impactam o dashboard.

Designed pra ser chamado do sync.py — recebe `deals`, `companies`,
`deal_to_company`, `owners`, `ganho_stages`, `perdido_stages` ja fetched
pra evitar requests duplicados ao HubSpot.

Sheet ID via env GAPS_SHEET_ID (default = a Sheet usada hoje).
"""
import csv
import os
import time
from collections import defaultdict
from datetime import datetime

import gspread

PORTAL_ID = "50771078"
DEFAULT_GAPS_SHEET_ID = "1GQe6ksTrQnoWNtFm2BF3WblkHiaNGdKK7ycf1qx-oSs"

# CSV no proprio repo. Casos onde so o Ivan sabe qual e' a empresa correta
# (multiplas opcoes de CNPJ, BAIXADAs, ambiguidade) — atribui esses gaps
# explicitamente a "Ivan Amaral" na Sheet, com opcoes pre-listadas.
OVERRIDES_IVAN_PATH = os.path.join(os.path.dirname(__file__), "overrides_ivan_companies.csv")

LEIS = ["valor_lei_rouanet", "valor_lei_do_esporte", "valor_lei_do_esporte_estadual",
        "valor_lei_do_bem", "valor_lei_da_cultura", "valor_lei_da_cultura_municipal",
        "valor_lei_da_crianca_e_do_adolescente", "valor_lei_do_idoso",
        "valor_lei_da_reciclagem", "valor_pronas", "valor_pronon"]


def _num(x):
    try:
        return float(x) if x not in (None, "") else 0.0
    except (ValueError, TypeError):
        return 0.0


def _parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def _safe_aba_name(nome):
    invalid = set(":\\/?*[]")
    s = "".join(c for c in nome if c not in invalid).strip()
    return s[:99] or "Sem owner"


def _load_overrides_ivan():
    """Carrega CSV de overrides Ivan e retorna {company_id: row_dict}."""
    if not os.path.exists(OVERRIDES_IVAN_PATH):
        return {}
    try:
        with open(OVERRIDES_IVAN_PATH, encoding="utf-8") as f:
            return {r["company_id"]: r for r in csv.DictReader(f)}
    except Exception as e:
        print(f"[warn] overrides_ivan_companies.csv falhou: {e}")
        return {}


def compute_gaps(deals, companies, deal_to_company, owners, ganho_stages, perdido_stages):
    """Retorna lista de dicts: owner_nome, tipo, entidade, id, nome, link, descricao, prioridade.

    Filtro: gaps de Company so se Company tem >=1 deal associado (decisao Bruno 27/04).
    Companies em overrides_ivan_companies.csv pulam gaps 8/10/11 (cobertos pelo gap 13).
    """
    gaps = []
    overrides_ivan = _load_overrides_ivan()

    # Index Company → list of deals (pra atribuir owner)
    company_to_deals = defaultdict(list)
    for d in deals:
        cid = deal_to_company.get(d["id"])
        if cid:
            company_to_deals[str(cid)].append(d)

    company_tem_ganho = set()
    for d in deals:
        if d["properties"].get("dealstage", "") in ganho_stages:
            cid = deal_to_company.get(d["id"])
            if cid:
                company_tem_ganho.add(str(cid))

    deal_link = lambda did: f"https://app.hubspot.com/contacts/{PORTAL_ID}/deal/{did}"
    company_link = lambda cid: f"https://app.hubspot.com/contacts/{PORTAL_ID}/company/{cid}"

    # ========== GAPS DE DEAL ==========
    for d in deals:
        p = d.get("properties", {}) or {}
        did = d["id"]
        dname = p.get("dealname", "") or "(sem nome)"
        owner_id = p.get("hubspot_owner_id", "") or ""
        owner_nome = owners.get(owner_id, "(sem owner)")
        is_ganho = p.get("dealstage", "") in ganho_stages
        is_perdido = p.get("dealstage", "") in perdido_stages

        # 1. deal sem company
        if did not in deal_to_company:
            gaps.append({"owner_nome": owner_nome, "tipo": "1. Deal sem company vinculada",
                         "entidade": "Deal", "id": did, "nome": dname,
                         "link": deal_link(did), "descricao": "Vincular ou criar Company",
                         "prioridade": "ALTA" if is_ganho else "MEDIA"})

        if is_ganho:
            # 2. ganho sem closedate
            if not (p.get("closedate") or "").strip():
                gaps.append({"owner_nome": owner_nome, "tipo": "2. Ganho sem closedate",
                             "entidade": "Deal", "id": did, "nome": dname,
                             "link": deal_link(did),
                             "descricao": "Preencher data de fechamento",
                             "prioridade": "ALTA"})
            # 3. ganho sem aporte
            if _num(p.get("valor_do_aporte")) <= 0:
                gaps.append({"owner_nome": owner_nome, "tipo": "3. Ganho sem valor_do_aporte",
                             "entidade": "Deal", "id": did, "nome": dname,
                             "link": deal_link(did),
                             "descricao": "Preencher valor que entrou (R$ vendido pra essa lei)",
                             "prioridade": "ALTA"})
            # 4. ganho sem lei principal
            if not (p.get("lei_principal") or "").strip():
                leis_vals = [(l, _num(p.get(l))) for l in LEIS]
                leis_validas = [(l, v) for l, v in leis_vals if v > 0]
                if leis_validas:
                    sugestao = f"argmax: {max(leis_validas, key=lambda x: x[1])[0]} (auto-PATCH no proximo cron)"
                else:
                    sugestao = "Sem dado-fonte — preencher lei manualmente"
                gaps.append({"owner_nome": owner_nome, "tipo": "4. Ganho sem lei_principal",
                             "entidade": "Deal", "id": did, "nome": dname,
                             "link": deal_link(did), "descricao": sugestao,
                             "prioridade": "ALTA"})
            # 5. ganho sem proponente
            if not (p.get("nome_do_proponente") or "").strip():
                gaps.append({"owner_nome": owner_nome, "tipo": "5. Ganho sem nome_do_proponente",
                             "entidade": "Deal", "id": did, "nome": dname,
                             "link": deal_link(did),
                             "descricao": "Preencher nome do empreendedor que deu o match",
                             "prioridade": "MEDIA"})
            # 6. ganho sem projeto
            if not (p.get("nome_do_projeto") or "").strip():
                gaps.append({"owner_nome": owner_nome, "tipo": "6. Ganho sem nome_do_projeto",
                             "entidade": "Deal", "id": did, "nome": dname,
                             "link": deal_link(did),
                             "descricao": "Preencher nome do projeto incentivado",
                             "prioridade": "MEDIA"})

        # 7. perdido sem motivo
        if is_perdido and not (p.get("motivo_de_perda") or "").strip():
            gaps.append({"owner_nome": owner_nome, "tipo": "7. Perdido sem motivo_de_perda",
                         "entidade": "Deal", "id": did, "nome": dname,
                         "link": deal_link(did),
                         "descricao": "Preencher motivo da perda",
                         "prioridade": "MEDIA"})

    # ========== GAPS DE COMPANY (so se ha >=1 deal associado) ==========
    for c in companies:
        p = c.get("properties", {}) or {}
        cid = c["id"]
        cname = p.get("name", "") or "(sem nome)"

        deals_da_company = company_to_deals.get(cid, [])
        if not deals_da_company:
            continue  # filtro B
        deals_da_company.sort(
            key=lambda d: _parse_dt(d["properties"].get("createdate")) or datetime.min.replace(tzinfo=None),
            reverse=True,
        )
        owner_id = deals_da_company[0]["properties"].get("hubspot_owner_id", "") or ""
        owner_nome = owners.get(owner_id, "(sem owner)")

        cnpj = (p.get("cnpj") or "").strip()
        state = (p.get("state") or "").strip()
        origem = (p.get("origem") or "").strip()
        em_override_ivan = cid in overrides_ivan

        # 8. company sem cnpj — pula se Ivan tem override (gap 13 cobre)
        if not cnpj and not em_override_ivan:
            gaps.append({"owner_nome": owner_nome, "tipo": "8. Company sem cnpj",
                         "entidade": "Company", "id": cid, "nome": cname,
                         "link": company_link(cid),
                         "descricao": "Preencher CNPJ (estado/cidade/CEP enchem sozinho via BrasilAPI)",
                         "prioridade": "ALTA" if cid in company_tem_ganho else "MEDIA"})

        # 9. company sem origem
        if not origem:
            gaps.append({"owner_nome": owner_nome, "tipo": "9. Company sem origem",
                         "entidade": "Company", "id": cid, "nome": cname,
                         "link": company_link(cid),
                         "descricao": "Preencher origem do lead (canal de aquisicao)",
                         "prioridade": "MEDIA"})

        # 10. company sem estado E sem cnpj — pula se Ivan tem override
        if not state and not cnpj and not em_override_ivan:
            gaps.append({"owner_nome": owner_nome, "tipo": "10. Company sem estado E sem cnpj",
                         "entidade": "Company", "id": cid, "nome": cname,
                         "link": company_link(cid),
                         "descricao": "Preencher CNPJ pra estado encher sozinho, OU estado manual",
                         "prioridade": "MEDIA"})

        # 11. company com cnpj invalido (BrasilAPI nao retorna)
        # Heuristica: criada > 2h E tem cnpj E nao tem state (Fase 4 ja deveria ter rodado)
        cdate = _parse_dt(p.get("createdate"))
        if cnpj and not state and cdate:
            try:
                idade_h = (datetime.now(cdate.tzinfo) - cdate).total_seconds() / 3600
            except Exception:
                idade_h = 0
            if idade_h > 2 and not em_override_ivan:
                gaps.append({"owner_nome": owner_nome, "tipo": "11. Company com cnpj invalido",
                             "entidade": "Company", "id": cid, "nome": cname,
                             "link": company_link(cid),
                             "descricao": f"CNPJ {cnpj} nao retorna na BrasilAPI — provavel typo, validar e corrigir",
                             "prioridade": "MEDIA"})

        # 12. company com Match mas sem diagnostico
        if cid in company_tem_ganho:
            valor_diag = _num(p.get("valor_total_do_diagnostico"))
            soma_leis = sum(_num(p.get(l)) for l in LEIS)
            if valor_diag <= 0 and soma_leis <= 0:
                gaps.append({"owner_nome": owner_nome, "tipo": "12. Company com Match mas sem diagnostico",
                             "entidade": "Company", "id": cid, "nome": cname,
                             "link": company_link(cid),
                             "descricao": "Registrar valor_total_do_diagnostico + decomposicao por lei",
                             "prioridade": "ALTA"})

    # ========== GAP 13: OVERRIDE IVAN (CNPJ ambiguo - so Ivan sabe) ==========
    # Casos onde fuzzy match + WebSearch nao chegaram a CNPJ unico —
    # Ivan resolve manualmente. Atribuido EXPLICITAMENTE ao Ivan na Sheet.
    for cid, r in overrides_ivan.items():
        motivo = r.get("motivo", "")
        opcoes = r.get("opcoes_cnpj", "")
        acao = r.get("acao", "")
        gaps.append({
            "owner_nome": "Ivan Amaral",
            "tipo": "13. Company com CNPJ ambiguo (Ivan decide)",
            "entidade": "Company", "id": cid,
            "nome": r.get("company_name", ""),
            "link": company_link(cid),
            "descricao": f"[{acao}] {motivo} | OPCOES: {opcoes}",
            "prioridade": "ALTA",
        })

    return gaps


def _ensure_aba(sh, name, rows, cols):
    try:
        aba = sh.worksheet(name)
        aba.clear()
        return aba
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=name, rows=rows, cols=cols)


def write_gaps_to_sheet(gaps, sh):
    """Escreve gaps na Sheet `sh` (aberta com gspread). 1 aba Resumo + 1 por executivo."""
    # === Aba Resumo ===
    matrix = defaultdict(lambda: defaultdict(int))
    todos_tipos = sorted({g["tipo"] for g in gaps})
    todos_owners = sorted({g["owner_nome"] for g in gaps})
    for g in gaps:
        matrix[g["owner_nome"]][g["tipo"]] += 1

    header = ["Executivo / Tipo de gap"] + todos_tipos + ["TOTAL"]
    rows = []
    for owner in todos_owners:
        row = [owner]
        total = 0
        for tipo in todos_tipos:
            n = matrix[owner][tipo]
            row.append(n if n else "")
            total += n
        row.append(total)
        rows.append(row)
    total_row = ["TOTAL GERAL"]
    grand_total = 0
    for tipo in todos_tipos:
        n = sum(matrix[owner][tipo] for owner in todos_owners)
        total_row.append(n)
        grand_total += n
    total_row.append(grand_total)
    rows.append(total_row)

    aba = _ensure_aba(sh, "Resumo", rows=len(rows) + 5, cols=len(header) + 2)
    aba.update(values=[header] + rows, range_name="A1", value_input_option="USER_ENTERED")
    aba.format("A1:Z1", {"textFormat": {"bold": True},
                         "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}})
    last_row = len(rows) + 1
    aba.format(f"A{last_row}:Z{last_row}", {"textFormat": {"bold": True},
                                            "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}})
    aba.update(values=[[f"Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M')}"]],
               range_name=f"A{last_row + 2}", value_input_option="USER_ENTERED")
    print(f"  Resumo: {len(todos_owners)} executivos, {len(todos_tipos)} tipos, {grand_total} gaps")
    time.sleep(1.5)

    # === Aba por executivo ===
    gap_header = ["Tipo", "Prioridade", "Entidade", "ID", "Nome", "Link HubSpot", "O que fazer"]
    by_owner = defaultdict(list)
    for g in gaps:
        by_owner[g["owner_nome"]].append(g)

    for owner_nome, owner_gaps in by_owner.items():
        owner_gaps.sort(key=lambda g: (0 if g["prioridade"] == "ALTA" else 1, g["tipo"]))
        aba_name = _safe_aba_name(owner_nome)
        rows = [[g["tipo"], g["prioridade"], g["entidade"], g["id"], g["nome"],
                 g["link"], g["descricao"]] for g in owner_gaps]
        aba = _ensure_aba(sh, aba_name, rows=len(rows) + 5, cols=len(gap_header))
        aba.update(values=[gap_header] + rows, range_name="A1", value_input_option="USER_ENTERED")
        aba.format("A1:G1", {"textFormat": {"bold": True},
                             "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}})
        # Conditional format: ALTA prioridade laranja
        try:
            sheet_id_internal = aba._properties["sheetId"]
            sh.batch_update({
                "requests": [{
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [{"sheetId": sheet_id_internal,
                                        "startRowIndex": 1, "endRowIndex": len(rows) + 1,
                                        "startColumnIndex": 1, "endColumnIndex": 2}],
                            "booleanRule": {
                                "condition": {"type": "TEXT_EQ",
                                              "values": [{"userEnteredValue": "ALTA"}]},
                                "format": {"backgroundColor": {"red": 1.0, "green": 0.85, "blue": 0.7}},
                            }
                        },
                        "index": 0,
                    }
                }]
            })
        except Exception as e:
            print(f"  [warn] format conditional ({aba_name}): {e}")
        time.sleep(1.5)
        print(f"  Aba '{aba_name}': {len(owner_gaps)} gaps")


def popular_gaps_sheet(deals, companies, deal_to_company, owners,
                       ganho_stages, perdido_stages, gc):
    """Entrypoint chamado de sync.py. `gc` e' o gspread client ja autenticado."""
    sheet_id = os.environ.get("GAPS_SHEET_ID", DEFAULT_GAPS_SHEET_ID)
    print(f"=== Fase 5: Sheet de Gaps (sheet_id={sheet_id[:12]}...) ===")
    gaps = compute_gaps(deals, companies, deal_to_company, owners,
                        ganho_stages, perdido_stages)
    print(f"  Total de gaps computados: {len(gaps)}")
    sh = gc.open_by_key(sheet_id)
    write_gaps_to_sheet(gaps, sh)
    print(f"=== Fase 5 done — {len(gaps)} gaps escritos ===")
    return len(gaps)
