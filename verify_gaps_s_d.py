"""Harness de dry-run da sessao S-D (Ivan 22/06): valida que os 3 gaps novos
(14, 15, 16) disparam e que os tipos existentes (1-13) continuam.

Reusa os fetchers de sync.py (load_stages, load_owner_map, fetch_all_deals,
fetch_associated_companies, fetch_all_companies, POS_VENDA_STAGES,
PROPONENTE_POS_VENDA_STAGES, DEAL_PROPERTIES). NAO escreve em Sheet nenhuma.

Uso: python verify_gaps_s_d.py
"""
from collections import Counter

import sync
from popular_gaps_sheet import compute_gaps


def main():
    print("[dry-run] load_stages / load_owner_map ...")
    stages = sync.load_stages()
    owners = sync.load_owner_map()

    print("[dry-run] fetch_all_deals ...")
    date_props = sorted({f"hs_v2_date_entered_{sid}" for sid in stages})
    deals = sync.fetch_all_deals(
        properties=sync.DEAL_PROPERTIES
        + [p for p in date_props if p not in sync.DEAL_PROPERTIES]
    )
    print(f"  deals: {len(deals)}")

    print("[dry-run] fetch_associated_companies + fetch_all_companies ...")
    deal_ids = [d["id"] for d in deals]
    deal_to_company = sync.fetch_associated_companies(deal_ids)
    companies = sync.fetch_all_companies() or []
    print(f"  deal_to_company: {len(deal_to_company)}  companies: {len(companies)}")

    ganho_stages = (
        {sid for sid, info in stages.items()
         if info.get("is_closed") and info.get("probability") == "1.0"}
        | sync.POS_VENDA_STAGES
        | sync.PROPONENTE_POS_VENDA_STAGES
    )
    perdido_stages = {sid for sid, info in stages.items()
                      if info.get("is_closed") and info.get("probability") == "0.0"}
    ganho_stages_incentivador = {sid for sid, info in stages.items()
                                 if info.get("is_closed")
                                 and info.get("probability") == "1.0"
                                 and info.get("pipeline_id") == "default"}

    print(f"[dry-run] ganho={len(ganho_stages)} perdido={len(perdido_stages)} "
          f"ganho_inc={len(ganho_stages_incentivador)}")

    gaps = compute_gaps(
        deals=deals,
        companies=companies,
        deal_to_company=deal_to_company,
        owners=owners,
        ganho_stages=ganho_stages,
        perdido_stages=perdido_stages,
        ganho_stages_incentivador=ganho_stages_incentivador,
    )

    counts = Counter(g["tipo"] for g in gaps)
    print(f"\n=== Total: {len(gaps)} gaps em {len(counts)} tipos ===")
    print(f"{'COUNT':>6}  TIPO")
    for tipo in sorted(counts, key=lambda t: (int(t.split('.')[0]) if t[0].isdigit() else 99, t)):
        print(f"{counts[tipo]:>6}  {tipo}")

    print("\n=== Smoke gap 16 (top 5 closedate-pre-match) ===")
    gap16 = [g for g in gaps if g["tipo"].startswith("16.")]
    for g in gap16[:5]:
        deal = next((d for d in deals if d["id"] == g["id"]), None)
        p = deal.get("properties", {}) if deal else {}
        print(f"  {g['id']:>12}  stage={p.get('dealstage', ''):<20}  "
              f"closedate={p.get('closedate', ''):<30}  {g['nome'][:40]}")
    if not gap16:
        print("  (vazio)")

    novos = ["14. Ganho sem tipo_de_proponente",
             "15. Ganho sem numero_do_projeto",
             "16. Closedate antes do estagio [Match]-Projetos"]
    print("\n=== Sanity dos 3 novos ===")
    for n in novos:
        c = counts.get(n, 0)
        sinal = "OK" if c > 0 else "ZERO (revisar)"
        print(f"  {sinal:<15}  {c:>4}  {n}")


if __name__ == "__main__":
    main()
