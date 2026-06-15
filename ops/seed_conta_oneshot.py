"""Seed ONE-SHOT dos Nº conta M/C na planilha Financeiro_Dados (Sprint D, Block 2).

Reconciliacao legado 1FbvQqb -> 54 deals feita por numero + cross-check de Valor,
com sign-off do Bruno 12/06 (so os 7 de alta confianca; os 3 do overlap
46506-2/46508-9 e o won=0 ficaram de fora). Depois deste seed, a camada de
PRESERVACAO do sheets_reporting_financeiro_mensal.py mantem os valores entre runs
-> este script nao precisa rodar de novo (nao virar cron).

Escreve so as celulas Nº conta M / Nº conta C dos 7 deals, casando por deal_id
(coluna tecnica), lookup de coluna por NOME do header (robusto a layout).

Uso:
  python ops/seed_conta_oneshot.py            # dry-run (default)
  python ops/seed_conta_oneshot.py --write    # aplica
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gspread  # noqa: E402
from gspread.utils import rowcol_to_a1  # noqa: E402

REPORT_SHEET_ID = "1pHbTmyKv9OTZjAiTYgVKNbF0Iq8H2NKElR_D9rsRMe4"   # Financeiro_Dados
TAB = "Controle de Vendas"
SA = r"C:\Users\bruno\.brada-secrets\sheets-sa.json"

# deal_id -> (Nº conta M, Nº conta C). Os 7 de alta confianca (sign-off 12/06).
SEED = {
    "60962880397": ("46109-1", "46119-9"),   # Casa do Alemao
    "60963989269": ("46506-2", "46508-9"),   # MedWriters (projeto SLI2402243)
    "60964155770": ("46506-2", "46508-9"),   # RMed (mesmo projeto SLI2402243)
    "58370791452": ("46751-0", "46753-7"),   # Nubank 2.575.309,88
    "58367431683": ("47106-2", "47108-9"),   # Nubank 2.4M (2500104)
    "58369873589": ("47698-6", "47699-4"),   # Nubank 2.4M (2501114)
    "58370185795": ("46868-1", "46867-3"),   # Nubank 1.025M (250023)
}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    apply = "--write" in sys.argv
    gc = gspread.service_account(filename=SA)
    ws = gc.open_by_key(REPORT_SHEET_ID).worksheet(TAB)
    vals = ws.get_all_values()
    header = vals[0]
    i_deal = header.index("deal_id")
    i_m = header.index("Nº conta M")
    i_c = header.index("Nº conta C")

    updates = []
    seen = set()
    for ri, row in enumerate(vals[1:], start=2):   # row 2 = primeira linha de dado
        did = (row[i_deal] if i_deal < len(row) else "").strip()
        if did not in SEED:
            continue
        seen.add(did)
        cm, cc = SEED[did]
        cliente = row[0] if row else ""
        cur_m = (row[i_m] if i_m < len(row) else "").strip()
        cur_c = (row[i_c] if i_c < len(row) else "").strip()
        a1_m, a1_c = rowcol_to_a1(ri, i_m + 1), rowcol_to_a1(ri, i_c + 1)
        print(f"  linha {ri:>3} | {did} | {cliente[:26]:26} | M {a1_m}: '{cur_m}' -> '{cm}' | C {a1_c}: '{cur_c}' -> '{cc}'")
        updates.append({"range": a1_m, "values": [[cm]]})
        updates.append({"range": a1_c, "values": [[cc]]})

    faltando = set(SEED) - seen
    if faltando:
        print(f"\n[aviso] deal_id do seed NAO encontrado no sheet: {sorted(faltando)}")
    print(f"\n{len(seen)}/{len(SEED)} deals casados | {len(updates)} celulas a escrever")

    if not apply:
        print("\n[dry-run] nada escrito. Use --write pra aplicar.")
        return
    if not updates:
        print("nada a escrever.")
        return
    ws.batch_update(updates, value_input_option="RAW")
    print(f"OK: {len(updates)} celulas Nº conta escritas em {TAB}.")


if __name__ == "__main__":
    main()
