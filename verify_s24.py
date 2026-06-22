#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Harness S2.4 (dry-run, read-only): valida o filtro de cards-zumbis Rafa pos-venda.

Confirma:
  - Cards-zumbis identificados (esperado >= 13: owner Rafa 86273315 AND stage in VENDIDO_POS_VENDA).
  - enrich() agora marca e_ganho=0 e e_ativo=0 pros zumbis (vs antes onde e_ganho=1 forcado).
  - compute_gaps com pos_venda_stages=VENDIDO_POS_VENDA NAO dispara gaps 2/3 pros zumbis.
  - gaps 4-6 continuam disparando pros zumbis (KPIs organizacionais).
  - ZERO regressao fora dos zumbis (B3: enrich() antes/depois bate exato pros 745+).
  - Calibragem delta meta (B7): quantos dos zumbis tem valor_do_aporte > 0 + soma.

NAO escreve em Sheet nenhuma. Para encerrar antes do dry-run, Ctrl+C.

Uso:
  cd C:/tmp/brada-s24
  python verify_s24.py
"""
from __future__ import annotations
import os
import sys
from collections import Counter
from pathlib import Path

# Carrega HUBSPOT_TOKEN antes de importar sync.
SECRETS = Path.home() / ".brada-secrets" / "hubspot.env"
if SECRETS.exists():
    for line in SECRETS.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

if not os.environ.get("HUBSPOT_TOKEN"):
    print("ERRO: HUBSPOT_TOKEN ausente. Confira ~/.brada-secrets/hubspot.env", file=sys.stderr)
    sys.exit(1)

import sync  # noqa: E402
from popular_gaps_sheet import compute_gaps  # noqa: E402

RAFA_OWNER_ID = "86273315"


def fetch_all():
    print("[1/4] load_stages ...", flush=True)
    stages = sync.load_stages()
    print(f"      OK: {len(stages)} stages")

    print("[2/4] load_owner_map ...", flush=True)
    owners = sync.load_owner_map()
    print(f"      OK: {len(owners)} owners")

    print("[3/4] fetch_all_deals ...", flush=True)
    deals = sync.fetch_all_deals(sync.DEAL_PROPERTIES)
    print(f"      OK: {len(deals)} deals")

    print("[4/4] fetch_all_companies + associations ...", flush=True)
    all_companies = sync.fetch_all_companies()
    deal_to_company = sync.fetch_associated_companies([d["id"] for d in deals])
    companies = {str(c["id"]): (c.get("properties", {}) or {}) for c in all_companies}
    print(f"      OK: {len(all_companies)} companies, {len(deal_to_company)} assocs")

    return stages, owners, deals, all_companies, companies, deal_to_company


def identify_zumbis(deals, stages):
    """Retorna lista (deal_id, dealname, dealstage, closedate, valor_do_aporte) dos zumbis Rafa."""
    zumbis = []
    for d in deals:
        p = d.get("properties", {}) or {}
        owner = p.get("hubspot_owner_id", "") or ""
        stage = p.get("dealstage", "") or ""
        if owner == RAFA_OWNER_ID and stage in sync.VENDIDO_POS_VENDA:
            try:
                valor = float(p.get("valor_do_aporte") or 0)
            except (TypeError, ValueError):
                valor = 0.0
            zumbis.append({
                "deal_id": d["id"],
                "dealname": p.get("dealname", "(sem nome)"),
                "dealstage": stage,
                "stage_nome": stages.get(stage, {}).get("nome", stage),
                "closedate": p.get("closedate", "") or "",
                "valor_do_aporte": valor,
            })
    return zumbis


def recompute_enrich_legacy(deal, stages):
    """Recomputa e_ganho/e_ativo com a LOGICA ANTIGA (sem filtro de zumbi).
    Espelha sync.py:602-604 ANTES do S2.4.
    """
    p = deal.get("properties", {}) or {}
    stage_id = p.get("dealstage", "") or ""
    stage_info = stages.get(stage_id, {})
    is_closed = stage_info.get("is_closed", False)
    prob = stage_info.get("probability", "")
    e_ganho_legacy = 1 if (is_closed and prob == "1.0") or stage_id in sync.VENDIDO_POS_VENDA else 0
    e_ativo_legacy = 1 if not is_closed and stage_id not in sync.VENDIDO_POS_VENDA else 0
    return e_ganho_legacy, e_ativo_legacy


def main():
    print("=" * 70)
    print("  S2.4 — Verificacao Cards Rafa pos-venda (DRY-RUN, read-only)")
    print("=" * 70)

    stages, owners, deals, all_companies, companies, deal_to_company = fetch_all()

    # === 1. Zumbis identificados ===
    print()
    print("=" * 70)
    print("  1. ZUMBIS IDENTIFICADOS (owner=Rafa 86273315 AND stage in VENDIDO_POS_VENDA)")
    print("=" * 70)
    zumbis = identify_zumbis(deals, stages)
    print(f"Total: {len(zumbis)} cards-zumbis (esperado >= 13)")
    print()
    print(f"  {'deal_id':<14} {'stage_nome':<35} {'closedate':<12} {'valor_do_aporte':>15}")
    print(f"  {'-'*14} {'-'*35} {'-'*12} {'-'*15}")
    for z in zumbis[:15]:
        close_short = z["closedate"][:10] if z["closedate"] else "(vazio)"
        print(f"  {z['deal_id']:<14} {z['stage_nome'][:35]:<35} {close_short:<12} {z['valor_do_aporte']:>15.2f}")
    if len(zumbis) > 15:
        print(f"  ... + {len(zumbis)-15} mais")

    # === 2. enrich() antes vs depois (B3 — defesa contra regressao fora dos 13) ===
    print()
    print("=" * 70)
    print("  2. enrich() ANTES vs DEPOIS — regressao fora dos zumbis (B3)")
    print("=" * 70)
    deal_to_contacts = {}
    company_to_contacts = {}
    contacts = {}

    enriched_novo = [
        sync.enrich(d, stages, deal_to_company, companies, owners=owners,
                    deal_to_contacts=deal_to_contacts,
                    company_to_contacts=company_to_contacts, contacts=contacts)
        for d in deals
    ]
    enriched_novo_by_id = {e["deal_id"]: e for e in enriched_novo}

    deltas_fora_zumbi = []
    zumbi_ids = {z["deal_id"] for z in zumbis}
    for d in deals:
        eg_legacy, ea_legacy = recompute_enrich_legacy(d, stages)
        eg_novo = enriched_novo_by_id[d["id"]]["e_ganho"]
        ea_novo = enriched_novo_by_id[d["id"]]["e_ativo"]
        if (eg_legacy != eg_novo) or (ea_legacy != ea_novo):
            if d["id"] not in zumbi_ids:
                deltas_fora_zumbi.append((d["id"], eg_legacy, eg_novo, ea_legacy, ea_novo))

    n_e_ganho_legacy = sum(recompute_enrich_legacy(d, stages)[0] for d in deals)
    n_e_ganho_novo = sum(int(e["e_ganho"]) for e in enriched_novo)
    n_e_ativo_legacy = sum(recompute_enrich_legacy(d, stages)[1] for d in deals)
    n_e_ativo_novo = sum(int(e["e_ativo"]) for e in enriched_novo)

    print(f"  e_ganho   antes={n_e_ganho_legacy}   depois={n_e_ganho_novo}   delta={n_e_ganho_novo - n_e_ganho_legacy}")
    print(f"  e_ativo   antes={n_e_ativo_legacy}   depois={n_e_ativo_novo}   delta={n_e_ativo_novo - n_e_ativo_legacy}")
    print(f"  Deltas FORA dos zumbis: {len(deltas_fora_zumbi)} (esperado: 0)")
    if deltas_fora_zumbi:
        print("  AVISO: regressao detectada fora dos zumbis:")
        for did, egL, egN, eaL, eaN in deltas_fora_zumbi[:5]:
            print(f"    deal {did}: e_ganho {egL}->{egN}, e_ativo {eaL}->{eaN}")

    # === 3. Calibracao delta meta (B7) ===
    print()
    print("=" * 70)
    print("  3. CALIBRACAO DELTA META (B7) — efetivo_brl perde quanto?")
    print("=" * 70)
    zumbis_com_valor = [z for z in zumbis if z["valor_do_aporte"] > 0]
    soma_valor = sum(z["valor_do_aporte"] for z in zumbis_com_valor)
    print(f"  Zumbis com valor_do_aporte > 0: {len(zumbis_com_valor)} de {len(zumbis)}")
    print(f"  Soma valor_do_aporte (ANTES contribuia a 'efetivo_brl' bruto): R$ {soma_valor:,.2f}")
    print(f"  Delta efetivo_brl esperado pos-merge: -R$ {soma_valor:,.2f} (bruto)")
    print(f"  Em meta liquida 10/15%, delta real e' 10-15% dessa soma.")
    if soma_valor < 1000:
        print("  [B7] Delta IRRELEVANTE — Bruno NAO precisa avisar Vanessa.")
    elif soma_valor < 50_000:
        print("  [B7] Delta PEQUENO (<R$ 50k bruto) — avisar opcionalmente.")
    else:
        print("  [B7] Delta MATERIAL (>R$ 50k bruto) — PRE-AVISAR Vanessa antes do cron.")

    # === 4. compute_gaps antes vs depois ===
    print()
    print("=" * 70)
    print("  4. compute_gaps ANTES vs DEPOIS — gaps 2/3 zerados pra zumbis?")
    print("=" * 70)
    ganho_stages = (
        {sid for sid, info in stages.items()
         if info.get("is_closed") and info.get("probability") == "1.0"}
        | sync.POS_VENDA_STAGES | sync.PROPONENTE_POS_VENDA_STAGES
    )
    perdido_stages = {sid for sid, info in stages.items()
                      if info.get("is_closed") and info.get("probability") == "0.0"}
    ganho_stages_inc = {sid for sid, info in stages.items()
                        if info.get("is_closed") and info.get("probability") == "1.0"
                        and info.get("pipeline_id") == "default"}

    gaps_antes = compute_gaps(deals, all_companies, deal_to_company, owners,
                              ganho_stages, perdido_stages,
                              ganho_stages_incentivador=ganho_stages_inc,
                              pos_venda_stages=None)  # legacy
    gaps_depois = compute_gaps(deals, all_companies, deal_to_company, owners,
                               ganho_stages, perdido_stages,
                               ganho_stages_incentivador=ganho_stages_inc,
                               pos_venda_stages=sync.VENDIDO_POS_VENDA)

    cnt_antes = Counter(g["tipo"] for g in gaps_antes)
    cnt_depois = Counter(g["tipo"] for g in gaps_depois)
    todos_tipos = sorted(set(cnt_antes) | set(cnt_depois))

    print(f"  {'TIPO':<60} {'antes':>7} {'depois':>7} {'delta':>7}")
    print(f"  {'-'*60} {'-'*7} {'-'*7} {'-'*7}")
    for t in todos_tipos:
        a = cnt_antes.get(t, 0)
        d = cnt_depois.get(t, 0)
        delta = d - a
        flag = "  <<<" if delta != 0 else ""
        print(f"  {t[:60]:<60} {a:>7} {d:>7} {delta:>+7}{flag}")

    # Zumbis: dos 13 zumbis, contar quantos disparavam gap 2/3 antes vs depois
    gaps_antes_zumbi = [g for g in gaps_antes if g.get("id") in zumbi_ids and g["tipo"] in
                        ("2. Ganho sem closedate", "3. Ganho sem valor_do_aporte")]
    gaps_depois_zumbi = [g for g in gaps_depois if g.get("id") in zumbi_ids and g["tipo"] in
                         ("2. Ganho sem closedate", "3. Ganho sem valor_do_aporte")]
    print()
    print(f"  Gaps 2/3 ATRIBUIDOS A ZUMBIS: antes={len(gaps_antes_zumbi)}, depois={len(gaps_depois_zumbi)}")
    print(f"  (esperado depois=0)")

    # Gaps 4-6 nos zumbis: precisam permanecer estaveis
    gaps_4_6_zumbi_antes = [g for g in gaps_antes if g.get("id") in zumbi_ids and g["tipo"] in
                            ("4. Ganho sem lei_principal", "5. Ganho sem nome_do_proponente",
                             "6. Ganho sem nome_do_projeto")]
    gaps_4_6_zumbi_depois = [g for g in gaps_depois if g.get("id") in zumbi_ids and g["tipo"] in
                             ("4. Ganho sem lei_principal", "5. Ganho sem nome_do_proponente",
                              "6. Ganho sem nome_do_projeto")]
    print(f"  Gaps 4/5/6 ATRIBUIDOS A ZUMBIS: antes={len(gaps_4_6_zumbi_antes)}, depois={len(gaps_4_6_zumbi_depois)}")
    print(f"  (esperado: iguais — KPIs organizacionais continuam)")

    print()
    print("=" * 70)
    print("  RESUMO PRA SIGN-OFF:")
    print("=" * 70)
    ok = True
    if len(zumbis) < 13:
        print(f"  FAIL: Apenas {len(zumbis)} zumbis identificados (esperado >= 13)")
        ok = False
    if deltas_fora_zumbi:
        print(f"  FAIL: {len(deltas_fora_zumbi)} regressoes fora dos zumbis (B3)")
        ok = False
    if len(gaps_depois_zumbi) > 0:
        print(f"  FAIL: {len(gaps_depois_zumbi)} gaps 2/3 ainda atribuidos a zumbis (esperado 0)")
        ok = False
    if len(gaps_4_6_zumbi_antes) != len(gaps_4_6_zumbi_depois):
        print(f"  FAIL: gaps 4/5/6 mudaram pros zumbis (esperado iguais)")
        ok = False
    if ok:
        print("  OK: Pronto pra commit.")
        sys.exit(0)
    else:
        print("  NAO PRONTO. Investigar antes de commit.")
        sys.exit(2)


if __name__ == "__main__":
    main()
