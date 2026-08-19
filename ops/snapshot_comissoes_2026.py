# -*- coding: utf-8 -*-
"""Snapshot/compare da planilha Comissões 2026 antes da carga HubSpot."""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sync import get_sheets_client

SHEET_ID = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"
TABS = {
    "Controle de Vendas": {"range": "A1:AF2000", "manual": [8, 9, 12, 13, 20, 21, 22, 23, 24, 25], "formula": [16, 17, 18, 19]},
    "Controle de Cobranças - Bia": {"range": "A1:AC2000", "manual": [10, 13, 17, 18], "formula": []},
}


def digest(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def capture(sheet_id=SHEET_ID):
    gc = get_sheets_client()
    sh = gc.open_by_key(sheet_id)
    metadata = sh.fetch_sheet_metadata(params={"includeGridData": False})
    by_title = {s["properties"]["title"]: s for s in metadata.get("sheets", [])}
    out = {"spreadsheet_id": sheet_id, "tabs": {}}
    for title, config in TABS.items():
        raw = sh.values_get(f"'{title}'!{config['range']}", params={"valueRenderOption": "UNFORMATTED_VALUE"}).get("values", [])
        formulas = sh.values_get(f"'{title}'!{config['range']}", params={"valueRenderOption": "FORMULA"}).get("values", [])
        width = 32 if title == "Controle de Vendas" else 29
        def projected(rows, indices):
            return [[(row + [""] * width)[idx] for idx in indices] for row in rows[1:]]
        sheet_meta = by_title[title]
        structure = {
            "properties": sheet_meta.get("properties", {}),
            "protectedRanges": sheet_meta.get("protectedRanges", []),
            "merges": sheet_meta.get("merges", []),
            "conditionalFormats": sheet_meta.get("conditionalFormats", []),
            "basicFilter": sheet_meta.get("basicFilter"),
        }
        manual = projected(raw, config["manual"])
        formula_cells = projected(formulas, config["formula"])
        out["tabs"][title] = {
            "range": config["range"], "values": raw, "formulas": formulas,
            "manual_indices_zero_based": config["manual"], "manual_hash": digest(manual),
            "formula_indices_zero_based": config["formula"], "formula_hash": digest(formula_cells),
            "structure": structure, "structure_hash": digest(structure),
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet-id", default=SHEET_ID)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--output")
    group.add_argument("--compare")
    args = ap.parse_args()
    current = capture(args.sheet_id)
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"snapshot OK: {path}")
        for title, data in current["tabs"].items():
            print(f"  {title}: manual={data['manual_hash']} formulas={data['formula_hash']} estrutura={data['structure_hash']}")
        return
    before = json.loads(Path(args.compare).read_text(encoding="utf-8"))
    failures = []

    def project(rows, indices, count=None):
        selected = rows[1:] if count is None else rows[1:1 + count]
        width = max(indices) + 1 if indices else 1
        return [[(list(row) + [""] * width)[idx] for idx in indices] for row in selected]

    def stable_structure(structure):
        props = structure.get("properties", {})
        return {
            "sheetId": props.get("sheetId"), "title": props.get("title"), "sheetType": props.get("sheetType"),
            "protectedRanges": structure.get("protectedRanges", []), "merges": structure.get("merges", []),
            "conditionalFormats": structure.get("conditionalFormats", []), "basicFilter": structure.get("basicFilter"),
        }

    for title, config in TABS.items():
        old, now = before["tabs"][title], current["tabs"][title]
        old_count = max(0, len(old["values"]) - 1)
        if project(old["values"], config["manual"], old_count) != project(now["values"], config["manual"], old_count):
            failures.append(f"{title}: colunas manuais existentes mudaram")
        if title == "Controle de Vendas":
            if project(old["formulas"], config["formula"], old_count) != project(now["formulas"], config["formula"], old_count):
                failures.append(f"{title}: formulas Q:T existentes mudaram")
            for offset, row in enumerate(now["formulas"][old_count + 1:], start=old_count + 2):
                padded = list(row) + [""] * 32
                if any(str(c).strip() for c in padded[:16]):
                    formulas = [padded[i] for i in config["formula"]]
                    if not all(str(value).startswith("=") for value in formulas):
                        failures.append(f"{title}: Q:T incompletas na nova linha {offset}")
        if stable_structure(old["structure"]) != stable_structure(now["structure"]):
            failures.append(f"{title}: protecoes/filtros/merges/formatacao condicional mudaram")
        width = 32 if title == "Controle de Vendas" else 29
        header = list(now["values"][0]) + [""] * width
        tech_idx = 31 if title == "Controle de Vendas" else 28
        # Antes da carga o header tecnico pode nao existir; depois deve existir.
        if len(now["values"]) > len(old["values"]) and str(header[tech_idx]).strip() != "hubspot_deal_id":
            failures.append(f"{title}: hubspot_deal_id tecnico ausente")
    if failures:
        raise SystemExit("[FALHA] " + "; ".join(failures))
    print("[OK] manuais/formulas existentes preservadas; estrutura estavel; novas Q:T validas quando presentes")


if __name__ == "__main__":
    main()
