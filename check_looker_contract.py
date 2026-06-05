#!/usr/bin/env python3
"""
check_looker_contract.py - guardrail de schema das planilhas Looker-facing.

Roda no CI do sync (DEPOIS da escrita). Garante que mudancas no sync nao quebrem
silenciosamente o Looker Studio. Por (planilha, aba) verifica:
  1. Colunas REQUERIDAS presentes -> pega rename/remocao de coluna que o Looker
     amarra (modo de falha historico, ex. commit f52669c).
  2. Colunas de data-dimensao em AAAA-MM-DD com fill nao-degenerado. O Looker so
     usa data via PARSE_DATE sobre essas colunas (coluna ISO/esparsa vira Texto).
     Ver PLAYBOOK_datas_sheets_looker no vault.

Falha (exit 1) se algum contrato quebrar -> step vermelho no GitHub Actions
(alerta por email), SEM bloquear o dado ja gravado pelo sync.

Config = subconjunto REQUERIDO (nao o header inteiro), entao coluna nova no sync
nunca dispara falso-positivo.

Auth: mesma do sync (GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE).
"""
import os
import re
import sys
import json

SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE", r"C:/Users/bruno/.brada-secrets/sheets-sa.json"
)
SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID", "1bBGfZIkjuqBBQL9U79cMWQUgz9FLrbIKm2SObAXEKF8"
)

# Contrato por (planilha, aba).
# 'required' = subconjunto que o Looker amarra; rename/remocao quebra widgets.
# 'date_dims' = colunas AAAA-MM-DD usadas como dimensao de periodo via PARSE_DATE.
# 'min_fill' = piso de preenchimento (so pra colunas que deveriam ser densas;
#              data_fechamento e legitimamente esparsa, entao fica de fora).
CONTRACTS = [
    {
        "label": "Comercial (raw_deals)",
        "spreadsheet_id": SPREADSHEET_ID,
        "tab": "raw_deals",
        "required": [
            "deal_id", "produto", "valor_vendido", "valor_projetado_ativo",
            "e_ganho", "e_perdido", "e_ativo", "linha_de_imposto_categoria",
            "company_state", "motivo_de_perda", "executivo_nome", "trabalhado_por",
            "origem_lead", "createdate", "closedate",
        ],
        "date_dims": ["data_criacao", "data_fechamento"],
        "min_fill": {"data_criacao": 0.80},
    },
]

YMD = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def get_client():
    from google.oauth2.service_account import Credentials
    import gspread

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if SERVICE_ACCOUNT_JSON:
        creds = Credentials.from_service_account_info(
            json.loads(SERVICE_ACCOUNT_JSON), scopes=scopes
        )
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    else:
        print("ERRO: credenciais Google ausentes "
              "(GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE).")
        sys.exit(2)
    return gspread.authorize(creds)


def check_contract(gc, c):
    fails = []
    sh = gc.open_by_key(c["spreadsheet_id"])
    ws = sh.worksheet(c["tab"])
    vals = ws.get_all_values()
    if not vals:
        return [f"{c['label']}: aba vazia"]
    header, body = vals[0], vals[1:]
    idx = {h: i for i, h in enumerate(header)}

    missing = [col for col in c["required"] if col not in idx]
    if missing:
        fails.append(f"{c['label']}: colunas REQUERIDAS ausentes "
                     f"(rename/remocao?): {missing}")

    n = len(body) or 1
    for col in c["date_dims"]:
        if col not in idx:
            fails.append(f"{c['label']}: coluna de data '{col}' AUSENTE "
                         f"(o sync deve emiti-la em AAAA-MM-DD)")
            continue
        i = idx[col]
        nonblank = [r[i].strip() for r in body if i < len(r) and r[i].strip()]
        bad = [v for v in nonblank if not YMD.match(v)]
        if bad:
            fails.append(f"{c['label']}: '{col}' tem {len(bad)} valor(es) fora de "
                         f"AAAA-MM-DD (ex: {bad[:3]})")
        fill = len(nonblank) / n
        floor = c.get("min_fill", {}).get(col)
        if floor is not None and fill < floor:
            fails.append(f"{c['label']}: '{col}' fill {fill:.0%} < minimo "
                         f"{floor:.0%} (coluna degenerada?)")
        print(f"  OK {c['label']} :: {col} -> {len(nonblank)}/{n} em AAAA-MM-DD")
    return fails


def main():
    gc = get_client()
    all_fails = []
    for c in CONTRACTS:
        print(f"== Contrato: {c['label']} ({c['tab']}) ==")
        try:
            all_fails += check_contract(gc, c)
        except Exception as e:
            all_fails.append(f"{c['label']}: erro ao checar: {e!r}")
    print()
    if all_fails:
        print("CONTRATO LOOKER QUEBRADO:")
        for f in all_fails:
            print("  - " + f)
        print("\nVer PLAYBOOK_datas_sheets_looker no vault. "
              "Nao editar a planilha na mao; corrigir no sync.py.")
        sys.exit(1)
    print("Contrato Looker OK em todas as planilhas.")


if __name__ == "__main__":
    main()
