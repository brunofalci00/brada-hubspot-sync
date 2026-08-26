# -*- coding: utf-8 -*-
"""Autoriza o service account apenas nas protecoes P e Q:S da Controle de Vendas."""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sync import get_sheets_client

SHEET_ID = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"
WS = "Controle de Vendas"
EMAIL = "brada-sheets@brada-tickets.iam.gserviceaccount.com"
REQUIRED = [(15, 19)]


def overlapping(protected):
    grid = protected.get("range", {})
    start = grid.get("startColumnIndex", 0)
    end = grid.get("endColumnIndex", 10**6)
    return any(max(start, a) < min(end, b) for a, b in REQUIRED) and not protected.get("warningOnly")


def fetch(sh):
    metadata = sh.fetch_sheet_metadata(params={"includeGridData": False})
    sheet = next(s for s in metadata["sheets"] if s["properties"]["title"] == WS)
    return [p for p in sheet.get("protectedRanges", []) if overlapping(p)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--sheet-id", default=SHEET_ID)
    args = ap.parse_args()
    gc = get_sheets_client()
    sh = gc.open_by_key(args.sheet_id)
    protections = fetch(sh)
    if not protections:
        raise SystemExit("nenhuma protecao P:S encontrada")
    changes = []
    for protected in protections:
        editors = protected.get("editors", {})
        users = list(dict.fromkeys(editors.get("users", []) + [EMAIL]))
        if EMAIL.lower() not in {u.lower() for u in editors.get("users", [])}:
            changes.append({"updateProtectedRange": {
                "protectedRange": {"protectedRangeId": protected["protectedRangeId"],
                                   "editors": {"users": users,
                                               "domainUsersCanEdit": bool(editors.get("domainUsersCanEdit", False))}},
                "fields": "editors"}})
            print(f"[AUTORIZAR] protection={protected['protectedRangeId']} cols={protected.get('range', {}).get('startColumnIndex')}:{protected.get('range', {}).get('endColumnIndex')}")
        else:
            print(f"[OK] protection={protected['protectedRangeId']} ja autoriza {EMAIL}")
    if not args.apply:
        print(f"[dry-run] mudancas={len(changes)}")
        return
    if changes:
        sh.batch_update({"requests": changes})
    remaining = []
    for protected in fetch(sh):
        users = {u.lower() for u in protected.get("editors", {}).get("users", [])}
        if EMAIL.lower() not in users:
            remaining.append(protected["protectedRangeId"])
    if remaining:
        raise SystemExit(f"read-back falhou; sem permissao em {remaining}")
    print(f"[OK read-back] {EMAIL} autorizado em {len(protections)} protecoes P:S")


if __name__ == "__main__":
    main()
