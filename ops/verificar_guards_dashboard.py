#!/usr/bin/env python3
"""
verificar_guards_dashboard.py - confere os numeros-alvo do dashboard comercial
e da view do Vitor ANTES de o Bruno aplicar no Looker (read-only).

Motivo (diagnostico 19-20/06): os cards do Looker estavam errados (sem filtro de
ano, createdate poluido pela migracao, "projetado ativo" mal rotulado). Os
checklists de correcao prometem numeros exatos; este script prova que esses
numeros NASCEM da planilha (sync), pra ninguem cacar fantasma clicando no canvas.
Codifica a verificacao empirica feita em 20/06.

Le a Brada_Dashboard_Deals (abas raw_deals, raw_metas_anuais, consolidado) SEMPRE
em UNFORMATTED (pt_BR; FORMATTED corrompe decimais x100 -> feedback_sheets_ptbr_
unformatted_read). NAO escreve nada.

Guards (esperado | fonte):
  raw_deals:
    - Leads ativos (e_ativo=1, produto != CRIAPE)                 339
    - Em reuniao agora (e_ativo=1 + stage "Reuniao Agendada")      11
    - Em diagnostico agora (e_ativo=1 + stage "Diagnostico")       19
    - Match won 2026 (e_ganho=1 + produto=Match + ano_fech=2026)   19
  raw_metas_anuais:
    - Fechado nao-CRIAPE (soma vendido_brl, produto != CRIAPE)     R$ 3.081.830,63
    - n_ganhos_ano Match                                            19
  consolidado (won_ganho=1 + ano=2026, view do Vitor = grupo, inclui CRIAPE):
    - linhas                                                        89
    - soma valor_vendido                                           R$ 9.145.890,72
    - por produto: CRIAPE 62/6.064.060,09 | Match 19/3.071.830,63 | Elaboracao 8/10.000,00
    - valor_efetivo_brada > 0                                       70 (gap conhecido)

Uso:
  python ops/verificar_guards_dashboard.py            # relatorio
  python ops/verificar_guards_dashboard.py --strict   # exit 1 se algum guard HARD falhar
Auth: mesma do sync (GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE).
"""
import os
import sys
import json

TOL = 0.01  # R$
DEALS_ID = os.environ.get("SPREADSHEET_ID", "1bBGfZIkjuqBBQL9U79cMWQUgz9FLrbIKm2SObAXEKF8")

SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE", r"C:/Users/bruno/.brada-secrets/sheets-sa.json"
)

# Stages do snapshot de funil (match por substring casefold; entre os e_ativo=1 so
# "[EV] - Reuniao Agendada" casa "reuni" e so "[EV] - Diagnostico" casa "diagn").
STAGE_REUNIAO = "reuni"
STAGE_DIAGNOSTICO = "diagn"
ANO_ALVO = "2026"

# Expectativas (do diagnostico 20/06). HARD = entra no --strict; INFO = so reporta.
EXP_LEADS_SEM_CRIAPE = 339
EXP_REUNIAO = 11
EXP_DIAGNOSTICO = 19
EXP_MATCH_WON = 19
EXP_FECHADO_NAO_CRIAPE = 3_081_830.63
EXP_NGANHOS_MATCH = 19
EXP_GRUPO_LINHAS = 89
EXP_GRUPO_VALOR = 9_145_890.72
EXP_POR_PRODUTO = {
    "CRIAPE": (62, 6_064_060.09),
    "Match": (19, 3_071_830.63),
    "Elaboração": (8, 10_000.00),
}
EXP_EFETIVO_POSITIVO = 70
# Funil "passou pela etapa em 2026" (limpo da carga inicial do CRM). SOFT: varia
# semana a semana com o pipeline. Semear com o valor real do 1o run pos-deploy.
EXP_PASSOU_REUNIAO_2026 = 68    # ~147 bruto menos a carga 02/01 (57) + 29/01 (22)
EXP_PASSOU_DIAG_2026 = 82       # diagnostico nao tem rajada de carga


def num(x):
    try:
        return float(x) if str(x).strip() not in ("", "None") else 0.0
    except (ValueError, TypeError):
        return 0.0


def truthy(x):
    return str(x).strip() in ("1", "1.0", "TRUE", "True", "true")


def ano_de(val):
    """Normaliza ano_fechamento UNFORMATTED ('2026', 2026, 2026.0) -> '2026'."""
    s = str(val).strip()
    if not s:
        return ""
    return s.split(".")[0]


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
        print("ERRO: credenciais Google ausentes "
              "(GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE).")
        sys.exit(2)
    return gspread.authorize(creds)


def read_unformatted(ws):
    vals = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    if not vals:
        raise SystemExit(f"aba '{ws.title}' vazia — sync no meio do clear+write? tente de novo.")
    header = vals[0]
    idx = {c: i for i, c in enumerate(header)}
    rows = [r for r in vals[1:] if any(str(x).strip() for x in r)]
    return idx, rows


# resultado de cada guard pro --strict / sumario
_results = []


def guard(label, got, expected, hard=True, money=False):
    if money:
        ok = abs(num(got) - num(expected)) <= TOL
        gs = f"R$ {num(got):,.2f}"
        es = f"R$ {num(expected):,.2f}"
    else:
        ok = got == expected
        gs, es = str(got), str(expected)
    tag = "OK  " if ok else ("FAIL" if hard else "DIFF")
    _results.append((ok or not hard, hard, label, gs, es))
    print(f"  [{tag}] {label:46} obtido={gs:>18}  esperado={es:>18}")
    return ok


def info(label, value):
    print(f"  [INFO] {label:46} {value}")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    strict = "--strict" in sys.argv
    gc = get_client()
    sh = gc.open_by_key(DEALS_ID)

    print("=" * 100)
    print("VERIFICACAO DE GUARDS — Dashboard Comercial + View Vitor (read-only, UNFORMATTED)")
    print(f"fonte: {DEALS_ID}")
    print("=" * 100)

    # ---------- raw_deals: funil snapshot + leads + match won ----------
    rd_idx, rd = read_unformatted(sh.worksheet("raw_deals"))

    def rd_get(r, c):
        i = rd_idx.get(c)
        return r[i] if i is not None and i < len(r) else ""

    for col in ("e_ativo", "e_ganho", "produto", "stage_nome", "ano_fechamento"):
        if col not in rd_idx:
            print(f"\n[ERRO] coluna '{col}' ausente em raw_deals — o sync mudou? "
                  "Sem ela o card correspondente nao monta.")
            if strict:
                sys.exit(2)

    ativos = [r for r in rd if truthy(rd_get(r, "e_ativo"))]
    ativos_sem_criape = [r for r in ativos if str(rd_get(r, "produto")).strip() != "CRIAPE"]
    reuniao = [r for r in ativos if STAGE_REUNIAO in str(rd_get(r, "stage_nome")).casefold()]
    diagnostico = [r for r in ativos if STAGE_DIAGNOSTICO in str(rd_get(r, "stage_nome")).casefold()]
    match_won = [r for r in rd
                 if truthy(rd_get(r, "e_ganho"))
                 and str(rd_get(r, "produto")).strip() == "Match"
                 and ano_de(rd_get(r, "ano_fechamento")) == ANO_ALVO]

    # Leads/Reuniao/Diagnostico = SNAPSHOT vivo (e_ativo + stage): variam de hora em
    # hora conforme o pipeline anda (cron horario). Sao guard SOFT (referencia ~X, nao
    # trava o --strict). Match won 2026 = contagem de FECHADO no ano -> estavel -> HARD.
    print("\n-- raw_deals (funil = SNAPSHOT do pipeline; createdate NAO entra) --")
    guard("Leads ativos (e_ativo=1, exclui CRIAPE) ~snapshot", len(ativos_sem_criape), EXP_LEADS_SEM_CRIAPE, hard=False)
    info("Leads ativos SEM excluir CRIAPE", f"{len(ativos)}  (delta CRIAPE = {len(ativos) - len(ativos_sem_criape)})")
    guard("Em reuniao agora (e_ativo=1 + Reuniao Agendada) ~snapshot", len(reuniao), EXP_REUNIAO, hard=False)
    guard("Em diagnostico agora (e_ativo=1 + Diagnostico) ~snapshot", len(diagnostico), EXP_DIAGNOSTICO, hard=False)
    guard("Match won 2026 (e_ganho=1, produto=Match, ano=2026)", len(match_won), EXP_MATCH_WON)

    # ---------- funil REAL: passou pela etapa em 2026 (limpo da carga inicial) ----------
    # Colunas novas (sync do funil, 21/06). Pre-deploy elas nao existem -> pula com aviso.
    if "data_entrou_reuniao" in rd_idx and "entrou_reuniao_em_carga" in rd_idx:
        def passou(data_col, carga_col):
            # data_entrou_* sai AAAA-MM-DD; ano = 4 primeiros chars (NAO usar ano_de,
            # que e' p/ campo de ano puro). Exclui quem caiu na carga inicial.
            return [r for r in rd
                    if str(rd_get(r, data_col))[:4] == ANO_ALVO
                    and not truthy(rd_get(r, carga_col))]
        passou_r = passou("data_entrou_reuniao", "entrou_reuniao_em_carga")
        passou_d = passou("data_entrou_diagnostico", "entrou_diagnostico_em_carga")
        bruto_r = sum(1 for r in rd if str(rd_get(r, "data_entrou_reuniao"))[:4] == ANO_ALVO)
        bruto_d = sum(1 for r in rd if str(rd_get(r, "data_entrou_diagnostico"))[:4] == ANO_ALVO)
        print("\n-- raw_deals (funil REAL: passou pela ETAPA em 2026, limpo da carga) --")
        guard("Passaram pela etapa de reuniao 2026 (limpo) ~drift", len(passou_r), EXP_PASSOU_REUNIAO_2026, hard=False)
        info("Reuniao 2026 bruto vs carga", f"{bruto_r} bruto, {bruto_r - len(passou_r)} marcados carga")
        guard("Passaram por diagnostico 2026 (limpo) ~drift", len(passou_d), EXP_PASSOU_DIAG_2026, hard=False)
        info("Diagnostico 2026 bruto vs carga", f"{bruto_d} bruto, {bruto_d - len(passou_d)} marcados carga")
    else:
        print("\n-- raw_deals (funil REAL) --")
        info("Funil passou-pela-etapa", "colunas data_entrou_* ausentes (sync do funil ainda nao deployado)")

    # ---------- raw_metas_anuais: cards de valor ----------
    rm_idx, rm = read_unformatted(sh.worksheet("raw_metas_anuais"))

    def rm_get(r, c):
        i = rm_idx.get(c)
        return r[i] if i is not None and i < len(r) else ""

    nao_criape = [r for r in rm if str(rm_get(r, "produto")).strip() != "CRIAPE"]
    fechado_nao_criape = round(sum(num(rm_get(r, "vendido_brl")) for r in nao_criape), 2)
    nganhos_match = None
    for r in rm:
        if str(rm_get(r, "produto")).strip() == "Match":
            nganhos_match = int(num(rm_get(r, "n_ganhos_ano")))
            break

    print("\n-- raw_metas_anuais (cards de valor; ja e ano-corrente, Looker so exibe) --")
    guard("Fechado nao-CRIAPE (soma vendido_brl)", fechado_nao_criape, EXP_FECHADO_NAO_CRIAPE, money=True)
    guard("n_ganhos_ano Match", nganhos_match, EXP_NGANHOS_MATCH)
    print("    breakdown por produto (vendido_brl | meta_anual_brl | n_ganhos_ano):")
    for r in rm:
        p = str(rm_get(r, "produto")).strip()
        print(f"      {p:14} {num(rm_get(r, 'vendido_brl')):>14,.2f} | "
              f"{num(rm_get(r, 'meta_anual_brl')):>12,.2f} | {int(num(rm_get(r, 'n_ganhos_ano')))}")

    # ---------- consolidado: view do Vitor (grupo, won 2026, inclui CRIAPE) ----------
    co_idx, co = read_unformatted(sh.worksheet("consolidado"))

    def co_get(r, c):
        i = co_idx.get(c)
        return r[i] if i is not None and i < len(r) else ""

    grupo = [r for r in co if truthy(co_get(r, "won_ganho")) and ano_de(co_get(r, "ano")) == ANO_ALVO]
    soma_grupo = round(sum(num(co_get(r, "valor_vendido")) for r in grupo), 2)
    efetivo_pos = sum(1 for r in grupo if num(co_get(r, "valor_efetivo_brada")) > 0)
    overlap = sum(1 for r in grupo if truthy(co_get(r, "tem_overlap_projeto")))

    por_produto = {}
    for r in grupo:
        p = str(co_get(r, "produto")).strip()
        c, v = por_produto.get(p, (0, 0.0))
        por_produto[p] = (c + 1, v + num(co_get(r, "valor_vendido")))

    print("\n-- consolidado (View do Vitor = won 2026, grupo, INCLUI CRIAPE) --")
    guard("Linhas won 2026", len(grupo), EXP_GRUPO_LINHAS)
    guard("Soma valor_vendido (grupo)", soma_grupo, EXP_GRUPO_VALOR, money=True)
    for prod, (ec, ev) in EXP_POR_PRODUTO.items():
        gc_, gv = por_produto.get(prod, (0, 0.0))
        guard(f"  {prod}: linhas", gc_, ec)
        guard(f"  {prod}: soma valor_vendido", round(gv, 2), ev, money=True)
    guard("valor_efetivo_brada > 0 (gap conhecido = 70/89)", efetivo_pos, EXP_EFETIVO_POSITIVO, hard=False)
    info("tem_overlap_projeto truthy (dedup CRIAPExMatch, gate Ivan)", overlap)
    extras = {p for p in por_produto} - set(EXP_POR_PRODUTO)
    if extras:
        info("produtos won 2026 fora do esperado", sorted(extras))

    # ---------- sumario ----------
    hard_fails = [r for r in _results if not r[0]]
    # soft = guard informativo (hard=False) cujo obtido difere do snapshot 20/06.
    soft_diffs = [(ok, hard, lbl, g, e) for (ok, hard, lbl, g, e) in _results
                  if not hard and g != e]
    print("\n" + "=" * 100)
    if hard_fails:
        print(f"GUARDS HARD FALHARAM ({len(hard_fails)}):")
        for _, _, lbl, g, e in hard_fails:
            print(f"  - {lbl}: obtido {g}, esperado {e}")
        print("\nInvestigar antes de aplicar no Looker (numero pode ter mudado na fonte, "
              "ou o filtro do card precisa ajuste). NAO clicar contra dado errado.")
        if strict:
            sys.exit(1)
    else:
        print("Todos os guards HARD OK — os numeros dos checklists nascem da planilha.")
    if soft_diffs:
        print(f"(soft) {len(soft_diffs)} guard(s) informativo(s) divergiram do snapshot 20/06 — ok, so monitorar.")
    print("=" * 100)


if __name__ == "__main__":
    main()
