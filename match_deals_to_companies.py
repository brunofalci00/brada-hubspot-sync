"""
Sprint 1C (30/04) — Associa Deals sem Company a Companies HubSpot existentes
via match exato/prefix de nome (conservador).

Sprint 1B foi pulado (0 deals com cnpj_do_incentivador). Sprint 1C eh o caminho
automatico. Casos ambiguos / sem match ficam pra Sprint 4 manual.

Logica de match (cascata, conservador):
  1. EXATO: cname_norm == dealname_norm
  2. PREFIX: dealname_norm.startswith(cname_norm) AND len(cname_norm) >= 10
  3. PREFIX_REV: cname_norm.startswith(dealname_norm) AND len(dealname_norm) >= 10
  - 1 candidato level=1 ou 2 -> ALTA, associa
  - >=2 candidatos level=1 -> MEDIA, skip (ambiguidade)
  - 0 candidatos -> BAIXA, skip

Endpoint:
  POST /crm/v4/associations/deals/companies/batch/create
  associationTypeId=5 (HUBSPOT_DEFINED, deal->primary_company)

Uso:
    python match_deals_to_companies.py            # dry-run
    python match_deals_to_companies.py --execute  # PATCH real
"""

import argparse
import csv
import os
import re
import sys
import time
from collections import defaultdict

import requests

from sync import (
    fetch_all_companies, fetch_all_deals, fetch_associated_companies,
    HEADERS, BASE,
)


DIR = os.path.dirname(__file__)
LOG_PATH = os.path.join(DIR, "sprint1c_matches.csv")

SUFIXOS_CORP = [
    " ltda.", " ltda", " s.a.", " s/a", " sa", " s a",
    " me", " eireli", " epp", " mei", " s/c",
    " - me", " - epp", " - eireli", " - ltda",
]

SEPS_NOISE = [" / ", " - ", " | ", " (", "  -  "]


def _strip_accents(s):
    rep = {"á":"a","à":"a","â":"a","ã":"a","ä":"a","Á":"a","Â":"a","Ã":"a",
           "é":"e","ê":"e","ë":"e","É":"e","Ê":"e",
           "í":"i","î":"i","ï":"i","Í":"i","Î":"i",
           "ó":"o","ô":"o","õ":"o","ö":"o","Ó":"o","Ô":"o","Õ":"o",
           "ú":"u","û":"u","ü":"u","Ú":"u",
           "ç":"c","Ç":"c"}
    for k, v in rep.items():
        s = s.replace(k, v)
    return s


def normalize(s):
    """Lowercase, sem acento, strip sufixo corporativo, strip noise comum."""
    if not s:
        return ""
    s = _strip_accents(str(s)).lower().strip()

    # Strip noise (corta no primeiro separador conhecido)
    for sep in SEPS_NOISE:
        if sep in s:
            s = s.split(sep)[0]

    # Strip sufixos corporativos no final
    changed = True
    while changed:
        changed = False
        for suf in SUFIXOS_CORP:
            if s.endswith(suf):
                s = s[:-len(suf)].strip()
                changed = True
                break

    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def match_level(cname, dealname):
    """Retorna (level, motivo) ou (None, None) se nao bate."""
    if not cname or not dealname:
        return None, None
    if cname == dealname:
        return 1, "exato"
    if dealname.startswith(cname) and len(cname) >= 10:
        return 2, "company_prefix_de_deal"
    if cname.startswith(dealname) and len(dealname) >= 10:
        return 2, "deal_prefix_de_company"
    return None, None


def find_candidates(dealname_norm, companies_norm):
    """Retorna lista de (level, motivo, company_id, company_name) ordenada por level asc."""
    out = []
    for cid, cname_norm, cname_raw in companies_norm:
        lv, motivo = match_level(cname_norm, dealname_norm)
        if lv is not None:
            out.append((lv, motivo, cid, cname_raw))
    out.sort(key=lambda x: x[0])
    return out


def decide(candidates):
    """Aplica logica de decisao: 1 cand level<=2 -> associar; resto skip."""
    if not candidates:
        return ("skip_sem_match", None, None, None)
    by_level = defaultdict(list)
    for lv, motivo, cid, cname in candidates:
        by_level[lv].append((motivo, cid, cname))
    if 1 in by_level:
        if len(by_level[1]) == 1:
            motivo, cid, cname = by_level[1][0]
            return ("associar", motivo, cid, cname)
        return ("skip_ambiguo", f"{len(by_level[1])}_matches_exatos", None, None)
    if 2 in by_level:
        if len(by_level[2]) == 1:
            motivo, cid, cname = by_level[2][0]
            return ("associar", motivo, cid, cname)
        return ("skip_ambiguo", f"{len(by_level[2])}_matches_prefix", None, None)
    return ("skip_sem_match", None, None, None)


def batch_associate(pairs, dry_run=True):
    """pairs = [(deal_id, company_id), ...]. POST batch/create. Retorna (ok, fail)."""
    if dry_run or not pairs:
        return len(pairs), 0
    ok = fail = 0
    for i in range(0, len(pairs), 100):
        chunk = pairs[i:i+100]
        body = {"inputs": [
            {"from": {"id": did}, "to": {"id": cid},
             "types": [{"associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 5}]}
            for did, cid in chunk
        ]}
        for attempt in range(3):
            r = requests.post(
                f"{BASE}/crm/v4/associations/deals/companies/batch/create",
                headers=HEADERS, json=body, timeout=30,
            )
            if r.status_code in (200, 201):
                results = r.json().get("results", [])
                ok += len(results)
                fail += len(chunk) - len(results)
                if r.json().get("errors"):
                    print(f"  [warn] errors no batch: {r.json().get('errors')[:2]}")
                break
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            print(f"  [erro] batch/create {r.status_code}: {r.text[:300]}")
            fail += len(chunk)
            break
        time.sleep(0.5)
    return ok, fail


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    print(f"=== Sprint 1C — Match deals to companies ({'EXECUTE' if args.execute else 'DRY-RUN'}) ===")

    deals = fetch_all_deals()
    companies = fetch_all_companies()
    deal_to_company = fetch_associated_companies([d["id"] for d in deals])

    sem_company = [d for d in deals if d["id"] not in deal_to_company]
    print(f"Deals sem Company: {len(sem_company)}")

    # Pre-normaliza Companies pra match em memoria
    companies_norm = []
    for c in companies:
        cname_raw = (c.get("properties", {}).get("name") or "").strip()
        cname_norm = normalize(cname_raw)
        if cname_norm:  # ignora Companies sem nome
            companies_norm.append((c["id"], cname_norm, cname_raw))
    print(f"Companies indexadas: {len(companies_norm)}")

    rows_log = []
    pairs_to_associate = []

    for d in sem_company:
        p = d.get("properties", {}) or {}
        did = d["id"]
        dealname = (p.get("dealname") or "").strip()
        dn_norm = normalize(dealname)

        candidates = find_candidates(dn_norm, companies_norm)
        decisao, motivo, cid, cname = decide(candidates)

        rows_log.append({
            "deal_id": did,
            "dealname": dealname,
            "dealname_norm": dn_norm,
            "decisao": decisao,
            "motivo": motivo or "",
            "company_id": cid or "",
            "company_name": cname or "",
            "num_candidates": len(candidates),
        })

        if decisao == "associar":
            pairs_to_associate.append((did, cid))

    # Salvar log
    fields = ["deal_id", "dealname", "dealname_norm", "decisao", "motivo",
              "company_id", "company_name", "num_candidates"]
    with open(LOG_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows_log)
    print(f"Log salvo: {LOG_PATH}")

    # Resumo
    cnt = defaultdict(int)
    for r in rows_log:
        cnt[r["decisao"]] += 1
    print()
    print("=== Resumo ===")
    for k, v in sorted(cnt.items()):
        print(f"  {k:20s}: {v}")
    print(f"  total a associar: {len(pairs_to_associate)}")

    # Mostra os que vao associar
    if pairs_to_associate:
        print()
        print("=== A associar ===")
        for r in rows_log:
            if r["decisao"] == "associar":
                print(f"  {r['deal_id']:18s} {r['dealname'][:50]:50s} -> {r['company_id']:18s} {r['company_name'][:30]:30s} ({r['motivo']})")

    if not args.execute:
        print()
        print("DRY-RUN: nada foi associado. Revise o CSV e rode com --execute.")
        return

    print()
    print(f"=== Executando associacoes ({len(pairs_to_associate)}) ===")
    ok, fail = batch_associate(pairs_to_associate, dry_run=False)
    print(f"  ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
