#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Harness S2.6 (dry-run, read-only): valida a exclusao de TODO deal de pos-venda
(estagios apos o ganho, os 5 de VENDIDO_POS_VENDA) da sheet de gaps, p/ todos os owners.

Metodo: invariante + paridade. Chama o compute_gaps NOVO duas vezes:
  (a) pos_venda_stages=None            -> baseline (nada filtrado)
  (b) pos_venda_stages=VENDIDO_POS_VENDA -> comportamento novo (pos-venda pulada)

Asserts:
  1. INVARIANTE: nenhum gap de deal em (b) referencia deal cujo stage in VENDIDO_POS_VENDA.
  2. PARIDADE:   o conjunto de gaps de (b) == gaps de (a) menos os gaps de DEAL dos deals
                 pos-venda. (is_zumbi so afetava pos-venda; nao-pos-venda tem que bater exato.)
  3. WON REAIS:  deals em ganho real (1253324968 / 1246571362) seguem podendo ser flagados.
  4. GAP-1 (C1): quantos gaps "1. Deal sem company" somem por serem de deal pos-venda.

NAO escreve em Sheet nenhuma.

Uso:
  cd C:/tmp/brada-s26
  python verify_s26.py
"""
from __future__ import annotations
import os
import sys
from collections import Counter, defaultdict
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

WON_REAL = {"1253324968", "1246571362"}  # Ganho-Incentivador, Fechado-Proponente


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
    print(f"      OK: {len(all_companies)} companies, {len(deal_to_company)} assocs")
    return stages, owners, deals, all_companies, deal_to_company


def key(g):
    return (g["tipo"], g["entidade"], g["id"])


def main():
    print("=" * 74)
    print("  S2.6 — Exclusao de deals pos-venda da sheet de gaps (DRY-RUN, read-only)")
    print("=" * 74)

    stages, owners, deals, all_companies, deal_to_company = fetch_all()

    POS = sync.VENDIDO_POS_VENDA
    deal_stage = {d["id"]: (d.get("properties", {}) or {}).get("dealstage", "") or "" for d in deals}
    deal_owner = {d["id"]: (d.get("properties", {}) or {}).get("hubspot_owner_id", "") or "" for d in deals}
    pos_deal_ids = {did for did, st in deal_stage.items() if st in POS}
    won_deal_ids = {did for did, st in deal_stage.items() if st in WON_REAL}

    print()
    print(f"  Deals em pos-venda (VENDIDO_POS_VENDA): {len(pos_deal_ids)}")
    print(f"  Deals em ganho REAL (won-stages):       {len(won_deal_ids)}")

    # Mesma construcao do sync.py main (linhas ~2991-3004)
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

    gaps_baseline = compute_gaps(deals, all_companies, deal_to_company, owners,
                                 ganho_stages, perdido_stages,
                                 ganho_stages_incentivador=ganho_stages_inc,
                                 pos_venda_stages=None)          # nada filtrado
    gaps_novo = compute_gaps(deals, all_companies, deal_to_company, owners,
                             ganho_stages, perdido_stages,
                             ganho_stages_incentivador=ganho_stages_inc,
                             pos_venda_stages=POS)               # comportamento novo

    # === Tabela por tipo (baseline vs novo) ===
    cnt_b = Counter(g["tipo"] for g in gaps_baseline)
    cnt_n = Counter(g["tipo"] for g in gaps_novo)
    print()
    print("=" * 74)
    print("  CONTAGEM POR TIPO (baseline sem filtro vs novo)")
    print("=" * 74)
    print(f"  {'TIPO':<52} {'base':>6} {'novo':>6} {'delta':>7}")
    print(f"  {'-'*52} {'-'*6} {'-'*6} {'-'*7}")
    for t in sorted(set(cnt_b) | set(cnt_n)):
        a, d = cnt_b.get(t, 0), cnt_n.get(t, 0)
        flag = "  <<<" if d != a else ""
        print(f"  {t[:52]:<52} {a:>6} {d:>6} {d-a:>+7}{flag}")

    # === 1. INVARIANTE ===
    viol = [g for g in gaps_novo if g["entidade"] == "Deal" and deal_stage.get(g["id"], "") in POS]
    print()
    print("=" * 74)
    print("  1. INVARIANTE — nenhum gap de DEAL no novo referencia deal pos-venda")
    print("=" * 74)
    print(f"  Violacoes: {len(viol)} (esperado 0)")
    for g in viol[:8]:
        print(f"    {g['tipo'][:40]:<40} deal={g['id']} stage={deal_stage.get(g['id'])}")

    # === 2. PARIDADE ===
    keys_base = {key(g) for g in gaps_baseline}
    keys_novo = {key(g) for g in gaps_novo}
    esperado_novo = {k for k in keys_base if not (k[1] == "Deal" and deal_stage.get(k[2], "") in POS)}
    faltando = esperado_novo - keys_novo   # sumiu algo que devia ficar (regressao)
    sobrando = keys_novo - esperado_novo    # apareceu algo que nao devia
    print()
    print("=" * 74)
    print("  2. PARIDADE — novo == baseline menos gaps de DEAL pos-venda")
    print("=" * 74)
    print(f"  Faltando (devia ficar e sumiu): {len(faltando)} (esperado 0)")
    for k in list(faltando)[:8]:
        print(f"    FALTA: {k}")
    print(f"  Sobrando (nao devia aparecer):  {len(sobrando)} (esperado 0)")
    for k in list(sobrando)[:8]:
        print(f"    SOBRA: {k}")

    # === 3. WON reais seguem flagaveis ===
    won_flag = {g["id"] for g in gaps_novo if g["entidade"] == "Deal" and g["id"] in won_deal_ids}
    print()
    print("=" * 74)
    print("  3. WON REAIS — deals em ganho real ainda podem ser flagados")
    print("=" * 74)
    print(f"  Deals won com >=1 flag no novo: {len(won_flag)} de {len(won_deal_ids)} won")

    # === 4. GAP-1 (C1) — quantos "1. Deal sem company" somem por serem pos-venda ===
    gap1_pos = [g for g in gaps_baseline
                if g["tipo"].startswith("1.") and g["entidade"] == "Deal" and g["id"] in pos_deal_ids]
    print()
    print("=" * 74)
    print("  4. GAP-1 pos-venda (C1) — 'Deal sem company' suprimidos por serem pos-venda")
    print("=" * 74)
    print(f"  Gap-1 em deals pos-venda: {len(gap1_pos)} (informativo; se >0, decisao C1 aplica)")

    # === Delta de flags de DEAL por owner (o que some) ===
    removed = [g for g in gaps_baseline if g["entidade"] == "Deal" and g["id"] in pos_deal_ids]
    por_owner = defaultdict(int)
    for g in removed:
        por_owner[owners.get(deal_owner.get(g["id"], ""), "(sem owner)")] += 1
    print()
    print("=" * 74)
    print(f"  FLAGS DE DEAL REMOVIDAS (pos-venda): {len(removed)}")
    print("=" * 74)
    for onome, n in sorted(por_owner.items(), key=lambda x: -x[1]):
        print(f"  {onome:<28} {n:>4}")

    # === Sign-off ===
    print()
    print("=" * 74)
    print("  RESUMO PRA SIGN-OFF")
    print("=" * 74)
    ok = True
    if viol:
        print(f"  FAIL: {len(viol)} gaps de deal pos-venda no novo (invariante quebrada)"); ok = False
    if faltando:
        print(f"  FAIL: {len(faltando)} gaps de nao-pos-venda sumiram (regressao / paridade)"); ok = False
    if sobrando:
        print(f"  FAIL: {len(sobrando)} gaps inesperados no novo (paridade)"); ok = False
    if len(won_deal_ids) == 0:
        print("  FAIL: 0 deals won-reais — construcao de stages suspeita"); ok = False
    if ok:
        print("  OK: Invariante + paridade batem. Won intactos. Pronto pra commit.")
        sys.exit(0)
    else:
        print("  NAO PRONTO. Investigar antes de commit.")
        sys.exit(2)


if __name__ == "__main__":
    main()
