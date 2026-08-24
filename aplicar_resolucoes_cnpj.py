"""
Sprint 2 follow-up — le aba "Resolucoes CNPJ — Ivan" e aplica PATCHs
no HubSpot conforme a coluna ESCOLHA preenchida pelo Ivan.

Logica:
  - Cand 1 / Cand 2 / Cand 3  -> usa _cnpj1/2/3 (col J/K/L) -> PATCH
  - Outro                      -> usa "Outro CNPJ" (col H) -> PATCH (valida checksum)
  - Não é nenhum               -> append em overrides_ivan_companies.csv (motivo da Sheet)
  - Pular / vazio              -> nao faz nada

Uso:
    python aplicar_resolucoes_cnpj.py            # dry-run
    python aplicar_resolucoes_cnpj.py --execute  # PATCH real
"""

import argparse
import csv
import os
import sys

import gspread

from scrape_cnpj_from_domain import patch_cnpj_hubspot, validar_cnpj, TOKEN
from sync import get_sheets_client

GAPS_SHEET_ID = os.environ.get("GAPS_SHEET_ID", "1GQe6ksTrQnoWNtFm2BF3WblkHiaNGdKK7ycf1qx-oSs")
ABA_NAME = "Resolucoes CNPJ — Ivan"
DIR = os.path.dirname(__file__)
# O arquivo saiu do repo: este e PUBLICO, e o CSV carregava nome de empresa, CNPJ e nota
# comercial interna ("mover deal pra Perdido") de 12 clientes e prospects. Nenhum consumidor
# le as COLUNAS — o unico uso e `if cid in overrides_ivan`, pertencimento pela chave — entao
# tirar o arquivo daqui nao custa funcionalidade. O loader ja trata ausencia devolvendo {}.
OVERRIDES_PATH = os.environ.get(
    "BRADA_OVERRIDES_IVAN_PATH",
    os.path.expanduser("~/.brada-secrets/overrides_ivan_companies.csv"),
)


def parse_companies():
    """Le aba e retorna lista de dicts (1 por linha com escolha != vazio)."""
    gc = get_sheets_client()
    sh = gc.open_by_key(GAPS_SHEET_ID)
    aba = sh.worksheet(ABA_NAME)
    rows = aba.get_all_values()
    if not rows:
        return []
    header = rows[0]
    idx = {h: i for i, h in enumerate(header)}
    out = []
    for row in rows[1:]:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        escolha = (row[idx["ESCOLHA"]] or "").strip()
        if not escolha or escolha.lower() == "pular":
            continue
        out.append({
            "company_id": row[idx["company_id"]],
            "company": row[idx["Company (link HubSpot)"]] if "Company (link HubSpot)" in idx else "",
            "escolha": escolha,
            "cnpj1": row[idx["_cnpj1"]],
            "cnpj2": row[idx["_cnpj2"]],
            "cnpj3": row[idx["_cnpj3"]],
            "outro": (row[idx["Outro CNPJ (14 dig)"]] or "").strip() if "Outro CNPJ (14 dig)" in idx else "",
            "notas": (row[idx["Notas"]] or "").strip() if "Notas" in idx else "",
        })
    return out


def append_override(company_id, company_name, motivo, notas):
    """Adiciona linha em overrides_ivan_companies.csv. Idempotente por company_id."""
    existing = []
    if os.path.exists(OVERRIDES_PATH):
        with open(OVERRIDES_PATH, encoding="utf-8") as f:
            existing = list(csv.DictReader(f))
    if any(r.get("company_id") == company_id for r in existing):
        return False
    fields = ["company_id", "company_name", "motivo", "opcoes_cnpj", "acao"]
    new = {
        "company_id": company_id,
        "company_name": company_name[:80],
        "motivo": motivo,
        "opcoes_cnpj": "",
        "acao": notas[:80] if notas else "DECIDIR",
    }
    with open(OVERRIDES_PATH, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not existing:
            w.writeheader()
        w.writerow(new)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    if not TOKEN:
        sys.exit("HUBSPOT_TOKEN nao setado.")

    items = parse_companies()
    print(f"=== Aplicar resolucoes ({'EXECUTE' if args.execute else 'DRY-RUN'}) ===")
    print(f"{len(items)} linhas com escolha preenchida")
    print()

    ok = fail = override = skip = 0
    for it in items:
        cid = it["company_id"]
        escolha = it["escolha"]
        cnpj_alvo = ""
        if escolha.startswith("Cand 1"):
            cnpj_alvo = it["cnpj1"]
        elif escolha.startswith("Cand 2"):
            cnpj_alvo = it["cnpj2"]
        elif escolha.startswith("Cand 3"):
            cnpj_alvo = it["cnpj3"]
        elif escolha.lower() == "outro":
            cnpj_alvo = "".join(c for c in it["outro"] if c.isdigit())
        elif "nenhum" in escolha.lower():
            added = append_override(cid, it["company"], "Sprint2: nenhum candidato bateu", it["notas"])
            print(f"  OVERRIDE {cid:18s} {it['company'][:40]:40s} ({'novo' if added else 'ja existia'})")
            override += 1
            continue

        if not cnpj_alvo or not validar_cnpj(cnpj_alvo):
            print(f"  SKIP {cid:18s} {it['company'][:40]:40s} cnpj_invalido escolha={escolha}")
            skip += 1
            continue

        if not args.execute:
            print(f"  WOULD-PATCH {cid:18s} {it['company'][:40]:40s} -> {cnpj_alvo} ({escolha})")
            ok += 1
            continue

        success, code = patch_cnpj_hubspot(cid, cnpj_alvo)
        if success:
            print(f"  OK {code} {cid:18s} {it['company'][:40]:40s} -> {cnpj_alvo}")
            ok += 1
        else:
            print(f"  FAIL {code} {cid:18s} {it['company'][:40]:40s} -> {cnpj_alvo}")
            fail += 1

    print()
    print(f"=== Resumo: ok={ok} override={override} fail={fail} skip={skip} ===")


if __name__ == "__main__":
    main()
