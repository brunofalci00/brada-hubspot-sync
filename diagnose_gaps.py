"""
Sprint 0 (29/04) - Diagnostico dos gaps Companies sem CNPJ + Deals sem Company.

Read-only: nao faz PATCH no HubSpot. Reusa helpers de sync.py pra puxar
deals/companies/associations/owners e exporta dois CSVs locais com tudo
que se sabe de cada registro do gap, mais um resumo de segmentacao no
terminal pra calibrar volume das sprints seguintes.

Filtros (espelham popular_gaps_sheet.compute_gaps):
  - Gap 8 "Company sem CNPJ":
      cnpj vazio AND >=1 deal associado AND nao em overrides_ivan
  - Gap 1 "Deal sem company vinculada":
      deal_id not in deal_to_company

Output:
  - gaps_companies_sem_cnpj_YYYYMMDD.csv  (1 linha por Company do gap)
  - gaps_deals_sem_company_YYYYMMDD.csv   (1 linha por Deal do gap)

Uso:
    python diagnose_gaps.py
"""

import csv
import datetime
import os
import re
from collections import defaultdict

# Reusa helpers e constantes de sync.py (mesmo diretorio).
from sync import (
    HUBSPOT_TOKEN,
    fetch_all_companies,
    fetch_all_deals,
    fetch_associated_companies,
    load_owner_map,
    load_stages,
)
from popular_gaps_sheet import _load_overrides_ivan


OUT_DIR = os.path.dirname(__file__)


# ===================================================
# HELPERS
# ===================================================

def _num(x):
    try:
        return float(x) if x not in (None, "") else 0.0
    except (ValueError, TypeError):
        return 0.0


def _classificar_company(p):
    """Bucketiza Company sem CNPJ pelo dado disponivel.

    Retorna ("sprint_1a"|"sprint_2"|"sprint_4_limpeza", motivo_curto).
    """
    name = (p.get("name") or "").strip()
    domain = (p.get("domain") or "").strip()

    name_norm = name.lower()
    if not name or len(name) < 3:
        return ("sprint_4_limpeza", "nome_vazio_ou_curto")
    palavras_lixo = {"test", "teste", "dummy", "exemplo", "(sem", "xxx", "asdf"}
    if any(w in name_norm for w in palavras_lixo):
        return ("sprint_4_limpeza", "nome_lixo")

    if domain:
        return ("sprint_1a", "tem_domain")

    if len(name.split()) >= 2:
        return ("sprint_2", "nome_rico")

    return ("sprint_4_limpeza", "nome_unico_token")


def _classificar_deal(p):
    """Bucketiza Deal sem Company.

    Retorna ("sprint_1b"|"sprint_1c"|"sprint_4", motivo_curto).
    """
    cnpj_inc = (p.get("cnpj_do_incentivador") or "").strip()
    dealname = (p.get("dealname") or "").strip()
    proponente = (p.get("nome_do_proponente") or "").strip()

    if cnpj_inc and re.search(r"\d", cnpj_inc):
        return ("sprint_1b", "tem_cnpj_incentivador")

    if dealname and len(dealname.split()) >= 2:
        return ("sprint_1c", "tem_dealname_rico")
    if proponente and len(proponente.split()) >= 2:
        return ("sprint_1c", "tem_proponente")

    return ("sprint_4", "orfao_completo")


# ===================================================
# COLETA
# ===================================================

def coletar():
    """Puxa deals + companies + associations + owners + stages do HubSpot."""
    if not HUBSPOT_TOKEN:
        raise SystemExit("HUBSPOT_TOKEN nao configurado.")

    print(f"=== Diagnose gaps ({datetime.datetime.now()}) ===")

    stages = load_stages()
    owners = load_owner_map()
    deals = fetch_all_deals()
    if not deals:
        raise SystemExit("Nenhum deal encontrado.")
    deal_ids = [d["id"] for d in deals]
    deal_to_company = fetch_associated_companies(deal_ids)
    all_companies = fetch_all_companies()

    ganho_stages = {sid for sid, info in stages.items()
                    if info.get("is_closed") and info.get("probability") == "1.0"}

    return {
        "stages": stages,
        "owners": owners,
        "deals": deals,
        "deal_to_company": deal_to_company,
        "all_companies": all_companies,
        "ganho_stages": ganho_stages,
    }


# ===================================================
# COMPANIES SEM CNPJ
# ===================================================

def gerar_csv_companies(ctx):
    """Exporta Companies sem CNPJ que tem >=1 deal e nao estao em override Ivan."""
    deals = ctx["deals"]
    companies = ctx["all_companies"]
    deal_to_company = ctx["deal_to_company"]
    owners = ctx["owners"]
    stages = ctx["stages"]
    ganho_stages = ctx["ganho_stages"]
    overrides_ivan = _load_overrides_ivan()

    # Index Company -> deals associados
    company_to_deals = defaultdict(list)
    for d in deals:
        cid = deal_to_company.get(d["id"])
        if cid:
            company_to_deals[str(cid)].append(d)

    rows = []
    contagem_bucket = defaultdict(int)
    contagem_motivo = defaultdict(int)

    for c in companies:
        p = c.get("properties", {}) or {}
        cid = c["id"]
        cnpj = (p.get("cnpj") or "").strip()
        if cnpj:
            continue
        if cid in overrides_ivan:
            continue
        deals_da_company = company_to_deals.get(str(cid), [])
        if not deals_da_company:
            continue  # filtro B (mesma decisao Bruno 27/04)

        # Owner do deal mais recente
        deals_da_company.sort(
            key=lambda d: (d["properties"].get("createdate") or ""),
            reverse=True,
        )
        owner_id = deals_da_company[0]["properties"].get("hubspot_owner_id", "") or ""
        owner_nome = owners.get(owner_id, "(sem owner)")

        num_deals_total = len(deals_da_company)
        num_deals_ganho = sum(
            1 for d in deals_da_company
            if d["properties"].get("dealstage", "") in ganho_stages
        )
        valor_aporte_total = sum(_num(d["properties"].get("valor_do_aporte"))
                                 for d in deals_da_company)
        prioridade = "ALTA" if num_deals_ganho > 0 else "MEDIA"

        bucket, motivo = _classificar_company(p)
        contagem_bucket[bucket] += 1
        contagem_motivo[motivo] += 1

        rows.append({
            "company_id": cid,
            "name": p.get("name", ""),
            "domain": p.get("domain", ""),
            "industry": p.get("industry", ""),
            "razao_social": p.get("razao_social", ""),
            "state": p.get("state", ""),
            "city": p.get("city", ""),
            "origem": p.get("origem", ""),
            "owner_executivo": owner_nome,
            "num_deals_total": num_deals_total,
            "num_deals_ganho": num_deals_ganho,
            "valor_aporte_total": round(valor_aporte_total, 2),
            "prioridade": prioridade,
            "bucket_sprint": bucket,
            "motivo_bucket": motivo,
            "link_hubspot": f"https://app.hubspot.com/contacts/50771078/company/{cid}",
        })

    return rows, contagem_bucket, contagem_motivo


# ===================================================
# DEALS SEM COMPANY
# ===================================================

def gerar_csv_deals(ctx):
    """Exporta Deals sem Company associada."""
    deals = ctx["deals"]
    deal_to_company = ctx["deal_to_company"]
    owners = ctx["owners"]
    stages = ctx["stages"]
    ganho_stages = ctx["ganho_stages"]

    rows = []
    contagem_bucket = defaultdict(int)
    contagem_motivo = defaultdict(int)

    for d in deals:
        did = d["id"]
        if did in deal_to_company:
            continue
        p = d.get("properties", {}) or {}
        stage_id = p.get("dealstage", "")
        stage_info = stages.get(stage_id, {})
        owner_id = p.get("hubspot_owner_id", "") or ""

        bucket, motivo = _classificar_deal(p)
        contagem_bucket[bucket] += 1
        contagem_motivo[motivo] += 1

        rows.append({
            "deal_id": did,
            "dealname": p.get("dealname", ""),
            "nome_do_proponente": p.get("nome_do_proponente", ""),
            "cnpj_do_incentivador": p.get("cnpj_do_incentivador", ""),
            "executivo_nome": owners.get(owner_id, "(sem owner)"),
            "executivo_match": p.get("executivo_match", ""),
            "dealstage_nome": stage_info.get("nome", stage_id),
            "pipeline_nome": stage_info.get("pipeline_nome", ""),
            "valor_do_aporte": _num(p.get("valor_do_aporte")),
            "is_ganho": 1 if stage_id in ganho_stages else 0,
            "createdate": p.get("createdate", ""),
            "bucket_sprint": bucket,
            "motivo_bucket": motivo,
            "link_hubspot": f"https://app.hubspot.com/contacts/50771078/deal/{did}",
        })

    return rows, contagem_bucket, contagem_motivo


# ===================================================
# OUTPUT
# ===================================================

def escrever_csv(path, rows):
    if not rows:
        print(f"[warn] nenhum row pra {path}")
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"[ok] {len(rows)} linhas -> {path}")


def imprimir_resumo(comp_rows, comp_bucket, comp_motivo,
                    deal_rows, deal_bucket, deal_motivo):
    print()
    print(f"=== {len(comp_rows)} Companies sem CNPJ ===")
    for bucket in ("sprint_1a", "sprint_2", "sprint_4_limpeza"):
        n = comp_bucket.get(bucket, 0)
        print(f"  {bucket:22s}: {n:4d}")
    print("  motivos:")
    for motivo, n in sorted(comp_motivo.items(), key=lambda x: -x[1]):
        print(f"    {motivo:25s}: {n:4d}")
    n_alta = sum(1 for r in comp_rows if r["prioridade"] == "ALTA")
    print(f"  prioridade ALTA (>=1 Ganho): {n_alta}")

    print()
    print(f"=== {len(deal_rows)} Deals sem Company ===")
    for bucket in ("sprint_1b", "sprint_1c", "sprint_4"):
        n = deal_bucket.get(bucket, 0)
        print(f"  {bucket:22s}: {n:4d}")
    print("  motivos:")
    for motivo, n in sorted(deal_motivo.items(), key=lambda x: -x[1]):
        print(f"    {motivo:25s}: {n:4d}")
    n_ganho = sum(1 for r in deal_rows if r["is_ganho"])
    print(f"  is_ganho: {n_ganho}")


def main():
    ctx = coletar()

    comp_rows, comp_bucket, comp_motivo = gerar_csv_companies(ctx)
    deal_rows, deal_bucket, deal_motivo = gerar_csv_deals(ctx)

    stamp = datetime.datetime.now().strftime("%Y%m%d")
    comp_path = os.path.join(OUT_DIR, f"gaps_companies_sem_cnpj_{stamp}.csv")
    deal_path = os.path.join(OUT_DIR, f"gaps_deals_sem_company_{stamp}.csv")

    escrever_csv(comp_path, comp_rows)
    escrever_csv(deal_path, deal_rows)

    imprimir_resumo(comp_rows, comp_bucket, comp_motivo,
                    deal_rows, deal_bucket, deal_motivo)


if __name__ == "__main__":
    main()
