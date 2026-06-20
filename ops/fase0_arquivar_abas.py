#!/usr/bin/env python3
"""
fase0_arquivar_abas.py - Fase 0 da limpeza da Brada_Dashboard_Deals (gated).

Decisao Bruno 20/06: ARQUIVAR POR RENOME (nao excluir) — reversivel.
  - 4 abas mortas `_arquivo_*` (snapshots de dedup ja consolidados, sem consumidor)
    -> prefixo `zzz_` (vao pro fim da planilha, somem do uso).
  - 3 abas one-shot (consumidas 1x) -> prefixo `zzz1shot_` (marcadas arquivaveis).

Seguranca:
  - DRY-RUN por default. So `--apply` renomeia.
  - Guarda dura: recusa renomear qualquer aba CANONICA (raw_deals, consolidado, ...).
  - Check estatico: confirma que nenhuma aba-alvo aparece no writer HORARIO (sync.py).
    Se aparecer, ABORTA (a aba estaria viva no cron). Tambem reporta em que .py do
    repo cada aba e referenciada (rastro do one-shot que a criou).
  - Idempotente: aba ja prefixada `zzz` e pulada.

Uso:
  python ops/fase0_arquivar_abas.py            # dry-run (mostra o plano)
  python ops/fase0_arquivar_abas.py --apply    # renomeia (CONFIRMAR o dry-run antes)
Auth: mesma do sync (GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE).
"""
import os
import sys
import glob
import json

DEALS_ID = os.environ.get("SPREADSHEET_ID", "1bBGfZIkjuqBBQL9U79cMWQUgz9FLrbIKm2SObAXEKF8")

SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE", r"C:/Users/bruno/.brada-secrets/sheets-sa.json"
)

# 4 abas mortas (sem consumidor, nao escritas pelo cron) -> arquivar.
DEAD_TABS = [
    "_arquivo_revisao_dedup_empresas",
    "_arquivo_revisao_dedup_deals",
    "_arquivo_reconciliacao_merge",
    "_arquivo_revisao_dedup_sem_cnpj",
]
# 3 abas one-shot (consumidas 1x) -> marcar arquivaveis.
ONESHOT_TABS = [
    "classificacao_match_ivan",
    "mapa_nomes",
    "reconciliacao_planilha_cards",
]
DEAD_PREFIX = "zzz_"
ONESHOT_PREFIX = "zzz1shot_"

# Guarda dura: NUNCA renomear estas (espinha do reporting).
CANONICAL = {
    "raw_deals", "consolidado", "raw_companies", "metas_anuais",
    "raw_metas_anuais", "_meta", "mapa_projetos",
}
# Writer HORARIO (cron). Se uma aba-alvo aparecer aqui, ela esta VIVA -> abortar.
HOURLY_WRITER = "sync.py"

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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


def grep_repo(tab):
    """Lista (arquivo, n_ocorrencias) onde o nome literal da aba aparece em .py do repo."""
    hits = []
    for path in glob.glob(os.path.join(REPO_ROOT, "*.py")) + \
            glob.glob(os.path.join(REPO_ROOT, "ops", "*.py")):
        if os.path.basename(path) == os.path.basename(__file__):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                txt = f.read()
        except (OSError, UnicodeDecodeError):
            continue
        n = txt.count(f'"{tab}"') + txt.count(f"'{tab}'")
        if n:
            hits.append((os.path.relpath(path, REPO_ROOT), n))
    return hits


def in_hourly_writer(tab):
    path = os.path.join(REPO_ROOT, HOURLY_WRITER)
    try:
        with open(path, encoding="utf-8") as f:
            txt = f.read()
    except OSError:
        return None  # writer nao encontrado -> nao da pra afirmar; trata como aviso
    return (f'"{tab}"' in txt) or (f"'{tab}'" in txt)


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    apply = "--apply" in sys.argv
    gc = get_client()
    sh = gc.open_by_key(DEALS_ID)
    existing = {w.title: w for w in sh.worksheets()}

    print("=" * 96)
    print(f"FASE 0 — arquivar abas por renome (Brada_Dashboard_Deals) [{'APPLY' if apply else 'DRY-RUN'}]")
    print(f"fonte: {DEALS_ID}")
    print("=" * 96)

    plan = []   # (ws, old, new, tipo)
    abort = False

    for tab, prefix, tipo in (
        [(t, DEAD_PREFIX, "morta") for t in DEAD_TABS] +
        [(t, ONESHOT_PREFIX, "one-shot") for t in ONESHOT_TABS]
    ):
        if tab in CANONICAL:
            print(f"  [ABORT]  {tab}: e CANONICA — nunca arquivar.")
            abort = True
            continue
        ws = existing.get(tab)
        if ws is None:
            # ja renomeada? procura o alvo
            if (prefix + tab) in existing:
                print(f"  [JA-OK]  {tab}: ja arquivada como '{prefix + tab}'.")
            else:
                print(f"  [SKIP]   {tab}: nao existe na planilha.")
            continue

        hourly = in_hourly_writer(tab)
        hits = grep_repo(tab)
        rastro = ", ".join(f"{f}({n})" for f, n in hits) or "nenhum .py"
        if hourly:
            print(f"  [ABORT]  {tab}: referenciada em {HOURLY_WRITER} (writer horario) — VIVA, nao arquivar.")
            abort = True
            continue
        new = prefix + tab
        if new in existing:
            print(f"  [SKIP]   {tab}: alvo '{new}' ja existe.")
            continue
        flag = "" if hourly is False else "  [!] nao validei o writer horario"
        print(f"  [{tipo.upper():8}] {tab}  ->  {new}   (rastro: {rastro}){flag}")
        plan.append((ws, tab, new, tipo))

    print("-" * 96)
    if abort:
        print("ABORTADO: ha aba canonica/viva na lista — corrija a config antes de aplicar. Nada foi mudado.")
        sys.exit(2)
    if not plan:
        print("Nada a fazer (tudo ja arquivado ou inexistente).")
        return
    print(f"{len(plan)} aba(s) a arquivar. Renome e REVERSIVEL (basta tirar o prefixo).")

    if not apply:
        print("\n[dry-run] nada foi alterado. Confirme este plano com o Bruno e rode com --apply.")
        return

    for ws, old, new, _ in plan:
        ws.update_title(new)
        print(f"  [renomeada] {old} -> {new}")
    print("\nOK: abas arquivadas. Conferir o Looker (a6ff3777-...) — nenhuma aba canonica foi tocada.")


if __name__ == "__main__":
    main()
