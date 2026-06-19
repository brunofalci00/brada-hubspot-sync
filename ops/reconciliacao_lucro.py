#!/usr/bin/env python3
"""
reconciliacao_lucro.py - teste "UM numero so" do lucro/comissao Brada (read-only).

Norte (roadmap Fonte Unica de Lucro): o MESMO numero de lucro nao pode divergir
entre as fontes. Este script prova consistencia entre:
  A = aba `consolidado` (FONTE UNICA do lucro: valor_bruto/aporte, valor_efetivo_brada
      = comissao bruta 10-15%, liquido_brada = bruto x 0.88, splits ivan/jaque)
  B = planilha financeira `Financeiro_Dados` (derivada do consolidado, recorte
      so-`Vendas%`; mapeia colunas, NAO recalcula)

Compara o subconjunto comparavel (A filtrado a `fluxo_comissao=="Vendas%"` won x B),
mais a deal-ancora e o snapshot por produto x ano. C (cards do Looker) e' checagem
MANUAL do Bruno (nao ha API do canvas).

Limites honestos: prova que A e B nao DIVERGEM; NAO prova completude (deals won sem
`tipo_de_proponente` tem comissao 0 e sub-dimensionam o lucro de forma consistente nos
dois lados). Completude e' outro gate (classificar os ~26 won).

Uso:
  python ops/reconciliacao_lucro.py              # relatorio + baseline
  python ops/reconciliacao_lucro.py --strict     # exit 1 se divergir alem de TOL (gate CI)
Auth: mesma do sync (GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE).
"""
import os
import sys
import json
from collections import defaultdict

TOL = 0.01  # R$
DEALS_ID = os.environ.get("SPREADSHEET_ID", "1bBGfZIkjuqBBQL9U79cMWQUgz9FLrbIKm2SObAXEKF8")
FIN_ID = os.environ.get("FINANCEIRO_DADOS_ID", "1pHbTmyKv9OTZjAiTYgVKNbF0Iq8H2NKElR_D9rsRMe4")
ANCHOR_DEAL = "60962880397"  # Casa do Alemao: bruto 6.985,15 / liquido 6.146,93

SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE", r"C:/Users/bruno/.brada-secrets/sheets-sa.json"
)


def num(x):
    try:
        return float(x) if str(x).strip() not in ("", "None") else 0.0
    except (ValueError, TypeError):
        return 0.0


def get_client():
    from google.oauth2.service_account import Credentials
    import gspread
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    if SERVICE_ACCOUNT_JSON:
        creds = Credentials.from_service_account_info(json.loads(SERVICE_ACCOUNT_JSON), scopes=scopes)
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    else:
        print("ERRO: credenciais Google ausentes."); sys.exit(2)
    return gspread.authorize(creds)


def read_unformatted(ws):
    # SEMPRE UNFORMATTED: a planilha e' pt_BR; leitura formatada corrompe decimais (x100).
    vals = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    header = vals[0]
    idx = {c: i for i, c in enumerate(header)}
    rows = [r for r in vals[1:] if any(str(x).strip() for x in r)]
    return header, idx, rows


def main():
    strict = "--strict" in sys.argv
    gc = get_client()

    # ---- A: consolidado (fonte unica do lucro) ----
    _, ia, ra = read_unformatted(gc.open_by_key(DEALS_ID).worksheet("consolidado"))
    def a(r, c):
        i = ia.get(c); return r[i] if i is not None and i < len(r) else ""
    won = [r for r in ra if str(a(r, "won_ganho")).strip() in ("1", "1.0")]

    print("== SNAPSHOT por produto x ano (consolidado won) ==")
    agg = defaultdict(lambda: [0.0, 0.0, 0.0, 0])
    for r in won:
        k = (str(a(r, "produto")).strip(), str(a(r, "ano")).strip() or "(s/ano)")
        agg[k][0] += num(a(r, "valor_vendido")); agg[k][1] += num(a(r, "valor_efetivo_brada"))
        agg[k][2] += num(a(r, "liquido_brada")); agg[k][3] += 1
    for k in sorted(agg):
        v = agg[k]
        print(f"  {k[0]:10} {k[1]:8} aporte={v[0]:>14,.2f} bruto={v[1]:>12,.2f} liquido={v[2]:>12,.2f} n={v[3]}")

    # subconjunto comparavel ao financeiro
    vp = [r for r in won if str(a(r, "fluxo_comissao")).strip() == "Vendas%"]
    A = {
        "ap": sum(num(a(r, "valor_vendido")) for r in vp),
        "br": sum(num(a(r, "valor_efetivo_brada")) for r in vp),
        "li": sum(num(a(r, "liquido_brada")) for r in vp),
        "iv": sum(num(a(r, "comissao_ivan")) for r in vp),
        "ja": sum(num(a(r, "comissao_jaque")) for r in vp),
    }

    # ---- B: Financeiro_Dados (derivada, recorte Vendas%) ----
    _, ib, rb = read_unformatted(gc.open_by_key(FIN_ID).worksheet("Controle de Vendas"))
    def b(r, c):
        i = ib.get(c); return r[i] if i is not None and i < len(r) else ""
    B = {
        "ap": sum(num(b(r, "Valor")) for r in rb),
        "br": sum(num(b(r, "Comissão BRADA")) for r in rb),
        "li": sum(num(b(r, "Líquido Brada")) for r in rb),
        "iv": sum(num(b(r, "Comissão Ivan 8%")) for r in rb),
        "ja": sum(num(b(r, "Comissão Jaque 4%")) for r in rb),
    }

    print(f"\n== A (consolidado Vendas% won, {len(vp)} linhas) x B (Financeiro_Dados, {len(rb)} linhas) ==")
    labels = [("Aporte", "ap"), ("Comissao BRADA (bruto)", "br"),
              ("Liquido Brada", "li"), ("Comissao Ivan", "iv"), ("Comissao Jaque", "ja")]
    diverge = False
    for label, k in labels:
        d = A[k] - B[k]
        ok = abs(d) <= TOL
        diverge = diverge or not ok
        print(f"  {label:24} A={A[k]:>14,.2f}  B={B[k]:>14,.2f}  dif={d:>12,.2f}  [{'OK' if ok else 'DIVERGE'}]")

    # ---- ancora ----
    anc = [r for r in ra if str(a(r, "deal_id")).strip() == ANCHOR_DEAL]
    if anc:
        r = anc[0]
        print(f"\n== Ancora {ANCHOR_DEAL} ({str(a(r,'cliente'))[:24]}) ==")
        print(f"  aporte={num(a(r,'valor_bruto')):,.2f} bruto={num(a(r,'valor_efetivo_brada')):,.2f} "
              f"liquido={num(a(r,'liquido_brada')):,.2f} ivan={num(a(r,'comissao_ivan')):,.2f} jaque={num(a(r,'comissao_jaque')):,.2f}")

    print("\nNota: C (cards Looker) = checagem MANUAL. A divergencia A x B comumente e' "
          "STALENESS do Financeiro_Dados (cron dia 20 desligado) ou deal won sem valor "
          "(financeiro filtra valor>0). Investigar o delta antes de tratar como erro.")

    if diverge and strict:
        print("\nRECONCILIACAO DIVERGENTE (--strict): exit 1")
        sys.exit(1)
    print("\nReconciliacao concluida.")


if __name__ == "__main__":
    main()
