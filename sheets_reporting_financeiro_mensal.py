"""
Reporting financeiro mensal (Frente A, Fase 1 — stream Vendas%).

Le a aba `consolidado` da Brada_Dashboard_Deals (comissao JA calculada pelo
`build_consolidado_layer` do sync.py — este script NAO recalcula nada, so
mapeia colunas) e gera a planilha financeira PARALELA no formato da aba
"Controle de Vendas" do Ivan (18 colunas A-R + 3 tecnicas), pro Ivan
reconciliar 1-2 ciclos antes de substituir a planilha manual.

Abas geradas na planilha paralela (Financeiro_Dados):
  - Controle de Vendas   espelho cumulativo (won Vendas% com valor > 0)
  - {YYYY-MM}_Vendas     corte do ciclo 21/mes-1 a 20/mes — HIPOTESE de corte
                         (as abas mensais do Ivan agrupam por ciclo de FOLHA,
                         nao derivavel do HubSpot; validar com Ivan)
  - Totais               agregacao por pessoa (Fase 1: so Ivan/Jaque/Externo)
  - _meta                proveniencia, contagens, validacoes, avisos

Uso:
  python sheets_reporting_financeiro_mensal.py                # dry-run (default)
  python sheets_reporting_financeiro_mensal.py --write        # escreve na paralela
  python sheets_reporting_financeiro_mensal.py --cycle 2026-06 --write

Fase 2 (pendente modelo Ivan): streams MATCH-fixo / Elaboracao / Reunioes.
Comissao = folha de pagamento: escrita gated (--write, amostra precisa PASSar),
cron do dia 20 so entra com sign-off do Ivan (workflow_dispatch ate la).
"""

import argparse
import datetime
import re
import sys

import gspread

from sync import get_sheets_client, PORTAL_ID

# ===================================================
# CONFIG
# ===================================================

import os

SOURCE_SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or \
    "1bBGfZIkjuqBBQL9U79cMWQUgz9FLrbIKm2SObAXEKF8"   # Brada_Dashboard_Deals
REPORT_SHEET_ID = os.environ.get("REPORT_FINANCEIRO_SHEET_ID") or \
    "1pHbTmyKv9OTZjAiTYgVKNbF0Iq8H2NKElR_D9rsRMe4"   # Financeiro_Dados (paralela, dono Bruno)

CONSOLIDADO_WS = "consolidado"

# Contrato com build_consolidado_layer (sync.py). Divergencia = hard-fail:
# a leitura e posicional e um shift silencioso poria numero errado em folha.
CONSOLIDADO_HEADER = [
    "deal_id", "cliente", "cnpj", "pipeline", "produto", "interno_externo",
    "fluxo_comissao", "projeto_key", "numero_projeto", "nome_projeto",
    "proponente", "stage", "convertido", "won_ganho", "tem_overlap_projeto",
    "closedate", "closedate_status", "data_aporte", "valor_bruto",
    "valor_vendido", "liquido_brada", "comissao_ivan", "comissao_jaque",
    "comissao_externo", "comissao_status", "owner", "owner_status",
    "origem_lead", "lei_principal", "ano", "empresa_canonica",
    "tipo_de_proponente", "valor_efetivo_brada",
    # Sprint D (12/06): contato do proponente, 3 ultimas colunas do consolidado.
    "nome_contato_proponente", "email_proponente", "telefone_proponente",
]

# Layout exato da aba "Controle de Vendas" do Ivan (A-R) + colunas tecnicas.
TARGET_HEADER = [
    "Cliente", "Fonte de recurso", "Proponente", "Dados para Cobrança",
    "Projeto", "Numero do projeto", "Nº conta M", "Nº conta C", "Valor",
    "Data do aporte", "DATA na Conta Movimentação", "Interno ou externo?",
    "Comissão BRADA", "Líquido Brada", "Comissão Ivan 8%", "Comissão Jaque 4%",
    "Comissão externo 3%", "Nome do externo",
]
# Sprint D: contato do proponente, inserido ENTRE o layout do Ivan (A-R) e as
# colunas tecnicas. Vira colunas S/T/U; desloca link/deal_id/status pra V/W/X/Y.
CONTATO_HEADER = ["Nome contato proponente", "Email proponente", "Telefone proponente"]
TECH_HEADER = ["link_hubspot", "deal_id", "comissao_status", "closedate_status"]

# Traducao lei_principal -> "Fonte de recurso" do Ivan. APROXIMACAO (os
# rotulos do Ivan sao texto livre: "Esporte IR", "IR esporte", "ISS RJ"...).
# Valor desconhecido passa cru (fica visivel na reconciliacao).
LEI_FONTE_MAP = {
    "Rouanet": "IR Cultura",
    "Esporte Federal": "Esporte IR",
    "Esporte Estadual": "ICMS Esporte",
    "Cultura Estadual": "ICMS Cultura",
    "Cultura Municipal": "ISS",
    "(sem lei preenchida)": "",
}

# Sprint D: Carina Ferreira substituiu Jéssica (09/06). Letícia NÃO entra (sem comissão).
PESSOAS_FASE2 = ["Carina", "Daniele", "Rafaela", "Ricardo"]

# Validacao: linha Casa do Alemao 46.567,65 (== linha 2 do mestre do Ivan,
# conferida 12/06: 6.985,15 / 6.146,93 / 491,75 / 245,88).
SAMPLE_DEAL_ID = "60962880397"
SAMPLE_CLIENTE_CONTAINS = "casa do alemao"
SAMPLE_VALOR = 46567.65
SAMPLE_EXPECT = {
    "valor_efetivo_brada": 6985.15,
    "liquido_brada": 6146.93,
    "comissao_ivan": 491.75,
    "comissao_jaque": 245.88,
}
TOL = 0.01

# Planilha oficial Vendas_25_26 reconciliada 08/06 (HubSpot == planilha).
REF_OFICIAL_0806 = 24_138_755.97

# Guard anti-corrida: o sync horario faz clear+write no consolidado; uma
# leitura no meio do clear viria vazia/parcial. Abaixo do piso, nao escreve.
MIN_ROWS_GUARD = 500

VERSAO = "fase1-vendas-pct v0.3"

AVISOS_RECONCILIACAO = [
    "5 colunas sao MANUAIS e PRESERVADAS por deal_id entre runs (Dados para Cobranca, No conta M, No conta C, Comissao externo 3%, Nome do externo): pode editar essas direto aqui que sobrevivem ao proximo run. As demais sao REGENERADAS (clear+write) e nao devem ser editadas (entrada vive no HubSpot, ex.: DATA na Conta Movimentacao = campo data_do_aporte do deal). 'favor preencher' e placeholder do template",
    "Fonte de recurso = traducao aproximada de lei_principal (rotulos do Ivan sao texto livre)",
    "Grao difere: planilha do Ivan = 1 linha por aporte/parcela; gerada = 1 card por numero de projeto (parcelas somadas; ex. Asia 100k+400k = 1 linha 500k). A SOMA bate, o numero de linhas nao",
    "Comissao externo 3% e Nome do externo agora sao MANUAIS (Luciana calcula o 3% do externo/finder; decisao Ivan 12/06). Jaque 4% segue AUTO em todos os deals: Jaque 4% e externo 3% podem coexistir ate a Luciana ajustar",
    "Data do aporte = closedate (decisao 08/06); o Ivan anota ISS como so o ano (fechamento+1): divergencia esperada nas linhas ISS",
    "Linhas sem formula no mestre do Ivan (linha 11+) tem comissao em branco la; aqui todas sao calculadas",
]


# ===================================================
# PARSE / FORMAT
# ===================================================

def parse_brl(s):
    """Display BR ('46567,65' / '40000') -> float. Vazio/invalido -> None."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    if "." in s and "," in s:
        s = s.replace(".", "")   # defesa contra separador de milhar futuro
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_closedate(s):
    """ISO '2025-12-15T00:00:00Z' (com ou sem ms) -> date. Vazio -> None."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def fmt_date_br(d):
    return d.strftime("%d/%m/%Y") if d else ""


def fmt_brl(v):
    """Float -> 'R$ 1.234,56' (so pro relatorio de console)."""
    if v is None:
        return "-"
    s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


# ===================================================
# LEITURA
# ===================================================

def load_consolidado(gc):
    """Le a aba consolidado inteira; valida o contrato de header (hard-fail)."""
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    vals = sh.worksheet(CONSOLIDADO_WS).get_all_values()
    if not vals:
        raise SystemExit("consolidado vazio — sync no meio do clear+write? Tente de novo em alguns minutos.")
    header = vals[0]
    if header != CONSOLIDADO_HEADER:
        faltando = [c for c in CONSOLIDADO_HEADER if c not in header]
        extras = [c for c in header if c not in CONSOLIDADO_HEADER]
        raise SystemExit(
            "Header do consolidado divergiu do contrato (sync.py mudou?).\n"
            f"  faltando: {faltando}\n  extras: {extras}\n"
            f"  ordem atual: {header}\n"
            "Atualize CONSOLIDADO_HEADER conscientemente — leitura e posicional."
        )
    rows = [dict(zip(header, r)) for r in vals[1:] if any(c.strip() for c in r)]

    # Proveniencia: o consolidado e escrito no mesmo run do sync de deals,
    # entao o timestamp proxy e a linha 'ultima_sync_deals' da _meta da fonte.
    fonte_ts = ""
    try:
        for meta_row in sh.worksheet("_meta").get_values("A1:C10"):
            if meta_row and meta_row[0] == "ultima_sync_deals":
                fonte_ts = " ".join(c for c in meta_row[1:] if c)
                break
    except gspread.exceptions.WorksheetNotFound:
        pass

    ids = [r["deal_id"] for r in rows]
    dups = {i for i in ids if ids.count(i) > 1}
    if dups:
        print(f"[aviso] deal_id duplicado no consolidado: {sorted(dups)}")
    return rows, fonte_ts


def split_vendas(rows):
    """Filtra o stream Vendas% won com valor. Excluidas saem com motivo."""
    incluidas, excluidas = [], []
    for r in rows:
        if r["fluxo_comissao"] != "Vendas%":
            continue
        valor = parse_brl(r["valor_bruto"])
        if r["won_ganho"] != "1":
            if (valor or 0) > 0 and r["closedate"].strip():
                excluidas.append((r, "won_0_com_valor_e_closedate"))
            continue
        if not valor or valor <= 0:
            excluidas.append((r, "valor_zero_ou_invalido"))
            continue
        incluidas.append(r)
    return incluidas, excluidas


# ===================================================
# MONTAGEM
# ===================================================

def map_lei(lei):
    return LEI_FONTE_MAP.get(lei, lei)


def build_record(r):
    """1 dict do consolidado -> registro com a linha de 25 celulas + closedate.
    Layout: TARGET (A-R, 18) + CONTATO (S-U, 3) + TECH (V-Y, 4)."""
    d = parse_closedate(r["closedate"])
    def money(col):
        return parse_brl(r[col]) or 0.0
    # K: data_do_aporte do Deal no HubSpot ("Conta Movimentacao", dominio do
    # financeiro). Time do Ivan preenche no deal -> entra aqui no proximo run.
    da = parse_closedate(r["data_aporte"])
    conta_mov = fmt_date_br(da) if da else r["data_aporte"]
    out = [
        r["cliente"],                       # A Cliente
        map_lei(r["lei_principal"]),        # B Fonte de recurso (aproximacao)
        r["proponente"],                    # C Proponente
        "favor preencher",                  # D Dados para Cobranca (MANUAL, preservado)
        r["nome_projeto"],                  # E Projeto
        r["numero_projeto"],                # F Numero do projeto
        "",                                 # G No conta M (MANUAL, preservado + seed one-shot)
        "",                                 # H No conta C (MANUAL, preservado + seed one-shot)
        money("valor_bruto"),               # I Valor
        fmt_date_br(d),                     # J Data do aporte (= closedate)
        conta_mov,                          # K DATA na Conta Movimentacao (<- data_do_aporte do HubSpot)
        r["interno_externo"],               # L Interno ou externo?
        money("valor_efetivo_brada"),       # M Comissao BRADA
        money("liquido_brada"),             # N Liquido Brada
        money("comissao_ivan"),             # O Comissao Ivan 8%
        money("comissao_jaque"),            # P Comissao Jaque 4% (AUTO)
        "",                                 # Q Comissao externo 3% (MANUAL, preservado - Luciana calcula; Block 3)
        "",                                 # R Nome do externo (MANUAL, preservado)
        r.get("nome_contato_proponente", ""),  # S Nome contato proponente (Sprint D)
        r.get("email_proponente", ""),         # T Email proponente
        r.get("telefone_proponente", ""),      # U Telefone proponente
        f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{r['deal_id']}",  # V link
        r["deal_id"],                       # W tecnica
        r["comissao_status"],               # X tecnica
        r["closedate_status"],              # Y tecnica
    ]
    assert len(out) == len(TARGET_HEADER) + len(CONTATO_HEADER) + len(TECH_HEADER), \
        f"build_record: out tem {len(out)} celulas, esperado {len(TARGET_HEADER) + len(CONTATO_HEADER) + len(TECH_HEADER)}"
    return {"src": r, "date": d, "out": out}


# ===================================================
# PRESERVACAO DE COLUNAS MANUAIS (Sprint D)
# ===================================================
# A planilha e regenerada (clear+write) a cada run. 5 colunas viram MANUAIS e
# precisam sobreviver entre runs: lidas da aba atual POR NOME de header (a posicao
# do deal_id muda quando inserimos as colunas de contato) e reinjetadas por deal_id.
# Isto revoga a regra "nao editar celulas" da v0.2 SO pra essas 5 colunas.

# nome do header (== TARGET_HEADER) -> indice no `out` da linha nova.
MANUAL_COLS = {
    "Dados para Cobrança": 3,
    "Nº conta M": 6,
    "Nº conta C": 7,
    "Comissão externo 3%": 16,
    "Nome do externo": 17,
}
EXTERNO_COL = "Comissão externo 3%"   # unica currency entre as manuais
PLACEHOLDER_COBRANCA = "favor preencher"


def build_preserved_map(values):
    """values = get_all_values() da aba atual -> {deal_id: {nome_col: valor}}.
    Crash-safe: sheet vazio / header sem deal_id -> {}. Regras:
      - texto: preserva se nao-vazio (e != placeholder pra Dados para Cobranca);
      - externo (currency): parse_brl, preserva SO se abs>0.005 e guarda FLOAT
        (descarta o '0,00' auto historico -> Block 3 vale ja no 1o run).
    Lookup estritamente por NOME (robusto a shift de layout entre v0.2 e v0.3)."""
    if not values:
        return {}
    header = values[0]
    try:
        idx_deal = header.index("deal_id")
    except ValueError:
        return {}
    col_idx = {name: header.index(name) for name in MANUAL_COLS if name in header}
    preserved = {}
    for row in values[1:]:
        if idx_deal >= len(row):
            continue
        did = (row[idx_deal] or "").strip()
        if not did:
            continue
        keep = {}
        for name, i in col_idx.items():
            raw = (row[i] if i < len(row) else "") or ""
            raw = raw.strip()
            if not raw:
                continue
            if name == EXTERNO_COL:
                v = parse_brl(raw)
                if v is None or abs(v) <= 0.005:
                    continue
                keep[name] = v
            else:
                if name == "Dados para Cobrança" and raw == PLACEHOLDER_COBRANCA:
                    continue
                keep[name] = raw
        if keep:
            preserved[did] = keep
    return preserved


def apply_preservation(records, preserved):
    """Reinjeta os valores manuais preservados nas linhas novas, por deal_id.
    Muta rec['out'] in place -> propaga pras abas cumulativa E de ciclo (mesmos dicts)."""
    n = 0
    for rec in records:
        ov = preserved.get(rec["src"]["deal_id"])
        if not ov:
            continue
        for name, val in ov.items():
            rec["out"][MANUAL_COLS[name]] = val
        n += 1
    return n


def read_preserved_manual(gc, report_id):
    """Le a aba 'Controle de Vendas' atual da planilha de saida (read-only) e
    monta o mapa de preservacao. Resiliente: qualquer falha -> {} (1o run, aba
    inexistente, etc.) e o run continua (manuais apenas nao preservados)."""
    try:
        sh = gc.open_by_key(report_id)
        ws = sh.worksheet("Controle de Vendas")
        return build_preserved_map(ws.get_all_values())
    except gspread.exceptions.WorksheetNotFound:
        return {}
    except Exception as e:
        print(f"[aviso] preservacao: nao li a aba atual ({type(e).__name__}); manuais nao preservados neste run.")
        return {}


def sort_records(records):
    """closedate asc, sem-data no fim; sort estavel preserva a ordem da fonte."""
    return sorted(records, key=lambda rec: (rec["date"] is None, rec["date"] or datetime.date.min))


def current_cycle(today=None):
    """Ciclo corrente: janela 21/mes-1 a 20/mes, nomeado pelo mes do FIM."""
    today = today or datetime.date.today()
    y, m = today.year, today.month
    if today.day >= 21:
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return f"{y:04d}-{m:02d}"


def cycle_window(cycle):
    y, m = int(cycle[:4]), int(cycle[5:7])
    end = datetime.date(y, m, 20)
    py, pm = (y - 1, 12) if m == 1 else (y, m - 1)
    start = datetime.date(py, pm, 21)
    return start, end


def cut_cycle(records, cycle):
    start, end = cycle_window(cycle)
    return [rec for rec in records if rec["date"] and start <= rec["date"] <= end]


# ===================================================
# VALIDACOES (dry-run imprime; so a amostra bloqueia --write)
# ===================================================

def validate_sample(rows):
    """Confere a linha-amostra contra o mestre do Ivan. PASS libera --write."""
    cands = [r for r in rows if r["deal_id"] == SAMPLE_DEAL_ID]
    via = f"deal_id {SAMPLE_DEAL_ID}"
    if not cands:
        cands = [r for r in rows
                 if SAMPLE_CLIENTE_CONTAINS in r["cliente"].casefold()
                 and abs((parse_brl(r["valor_bruto"]) or 0) - SAMPLE_VALOR) <= TOL
                 and r["fluxo_comissao"] == "Vendas%"]
        via = "fallback cliente+valor"
        if not cands:
            return {"status": "NOT_FOUND", "via": via, "detalhes": []}
        if len(cands) > 1:
            return {"status": "AMBIGUOUS", "via": via,
                    "detalhes": [r["deal_id"] for r in cands]}
    r = cands[0]
    detalhes, ok = [], True
    for campo, esperado in SAMPLE_EXPECT.items():
        achado = parse_brl(r[campo])
        passou = achado is not None and abs(achado - esperado) <= TOL
        ok = ok and passou
        detalhes.append((campo, esperado, achado, "PASS" if passou else "FAIL"))
    return {"status": "PASS" if ok else "FAIL", "via": via, "detalhes": detalhes}


def validate_total(records, excluidas):
    soma = round(sum(rec["out"][8] for rec in records), 2)
    delta = round(soma - REF_OFICIAL_0806, 2)
    won0 = [(r["deal_id"], r["cliente"], parse_brl(r["valor_bruto"]) or 0,
             r["closedate"][:10], r["stage"])
            for r, motivo in excluidas if motivo == "won_0_com_valor_e_closedate"]
    valor0 = [(r["deal_id"], r["cliente"])
              for r, motivo in excluidas if motivo == "valor_zero_ou_invalido"]
    soma_won0 = round(sum(v for _, _, v, _, _ in won0), 2)
    sigma = [(rec["src"]["deal_id"], rec["src"]["cliente"], rec["out"][8])
             for rec in records if "sigma" in rec["src"]["cliente"].casefold()]
    return {"soma": soma, "delta": delta, "won0": won0, "soma_won0": soma_won0,
            "valor0": valor0, "sigma": sigma}


# ===================================================
# ABAS DERIVADAS
# ===================================================

def build_totais(records, cycle_records, cycle):
    def _ext(rec):
        # Sprint D: externo (idx 16) virou MANUAL -> pode ser "" (vazio) ou float.
        v = rec["out"][16]
        return v if isinstance(v, (int, float)) else (parse_brl(v) or 0.0)
    def somas(recs):
        return {
            "Ivan": round(sum(rec["out"][14] for rec in recs), 2),
            "Jaque": round(sum(rec["out"][15] for rec in recs), 2),
            "Externo": round(sum(_ext(rec) for rec in recs), 2),
            "brada": round(sum(rec["out"][12] for rec in recs), 2),
            "valor": round(sum(rec["out"][8] for rec in recs), 2),
        }
    acum, cic = somas(records), somas(cycle_records)
    header = ["Pessoa", "Vendas% acumulado", f"Vendas% ciclo {cycle}"]
    rows = [
        ["Ivan", acum["Ivan"], cic["Ivan"]],
        ["Jaque", acum["Jaque"], cic["Jaque"]],
    ]
    rows += [[p, 0.0, 0.0] for p in PESSOAS_FASE2]
    rows += [
        ["Externo", acum["Externo"], cic["Externo"]],
        ["", "", ""],
        ["Comissão BRADA total", acum["brada"], cic["brada"]],
        ["Valor bruto total", acum["valor"], cic["valor"]],
        ["", "", ""],
        ["Fase 1: apenas stream Vendas%. Carina/Daniele/Rafaela/Ricardo "
         "zerados até a Fase 2 (MATCH-fixo/Elaboração/Reuniões). "
         "Externo 3% agora é manual (Luciana); Jaque 4% segue automático.", "", ""],
    ]
    return header, rows


def build_meta(args_ns, cycle, counts, sample, total, fonte_ts):
    start, end = cycle_window(cycle)
    sample_str = sample["status"] + (f" (via {sample['via']})" if sample["status"] != "PASS" else "")
    rows = [
        ["gerado_em", datetime.datetime.now().strftime("%d/%m/%Y %H:%M")],
        ["script", f"sheets_reporting_financeiro_mensal.py {VERSAO}"],
        ["fonte", f"{SOURCE_SPREADSHEET_ID}/{CONSOLIDADO_WS}"],
        ["fonte_atualizada_em", fonte_ts or "(timestamp nao encontrado na _meta da fonte)"],
        ["filtro", "fluxo_comissao == Vendas% AND won_ganho == 1 AND valor_bruto > 0"],
        ["ciclo", cycle],
        ["janela_ciclo", f"{fmt_date_br(start)} a {fmt_date_br(end)}"],
        ["hipotese_corte", "closedate entre 21/mes-1 e 20/mes — abas do Ivan agrupam por ciclo de FOLHA (validar com Ivan)"],
        ["linhas_lidas", counts["lidas"]],
        ["linhas_vendas_pct", counts["vendas_pct"]],
        ["linhas_escritas", counts["escritas"]],
        ["linhas_ciclo", counts["ciclo"]],
        ["excluidas_won_0", len(total["won0"])],
        ["excluidas_valor_zero", len(total["valor0"])],
        ["pendente_classificacao", counts["pendente"]],
        ["sem_closedate", counts["sem_closedate"]],
        ["sem_proponente", counts["sem_proponente"]],
        ["sem_numero_projeto", counts["sem_numero"]],
        ["soma_valor", total["soma"]],
        ["referencia_oficial_0806", REF_OFICIAL_0806],
        ["delta", total["delta"]],
        ["soma_won_0_excluidas", total["soma_won0"]],
        ["validacao_amostra", sample_str],
    ]
    rows += [[f"aviso_{i}", a] for i, a in enumerate(AVISOS_RECONCILIACAO, 1)]
    return ["chave", "valor"], rows


def compute_counts(rows, records, cycle_records):
    return {
        "lidas": len(rows),
        "vendas_pct": sum(1 for r in rows if r["fluxo_comissao"] == "Vendas%"),
        "escritas": len(records),
        "ciclo": len(cycle_records),
        "pendente": sum(1 for rec in records if rec["src"]["comissao_status"] == "pendente_classificacao"),
        "sem_closedate": sum(1 for rec in records if rec["date"] is None),
        "sem_proponente": sum(1 for rec in records if not rec["src"]["proponente"].strip()),
        "sem_numero": sum(1 for rec in records if not rec["src"]["numero_projeto"].strip()),
    }


# ===================================================
# RELATORIO (dry-run E write imprimem o mesmo)
# ===================================================

def print_report(counts, records, cycle_records, cycle, sample, total, fonte_ts, max_preview):
    w = print
    w("=" * 100)
    w(f"REPORTING FINANCEIRO — Fase 1 Vendas% ({VERSAO})")
    w(f"fonte: {SOURCE_SPREADSHEET_ID}/{CONSOLIDADO_WS} | atualizada: {fonte_ts or '?'}")
    w("=" * 100)
    w(f"linhas lidas: {counts['lidas']} | Vendas%: {counts['vendas_pct']} | "
      f"escritas (won, valor>0): {counts['escritas']} | ciclo {cycle}: {counts['ciclo']}")
    w(f"pendente_classificacao: {counts['pendente']} | sem closedate: {counts['sem_closedate']} | "
      f"sem proponente: {counts['sem_proponente']} | sem numero projeto: {counts['sem_numero']}")

    w(f"\n--- Controle de Vendas (primeiras {min(max_preview, len(records))} de {len(records)}) ---")
    w(f"{'Cliente':40} | {'Valor':>14} | {'Data':>10} | {'I/E':7} | {'C.BRADA':>11} | "
      f"{'Ivan':>9} | {'Jaque':>9} | status")
    for rec in records[:max_preview]:
        o = rec["out"]
        w(f"{o[0][:40]:40} | {o[8]:>14,.2f} | {o[9]:>10} | {o[11][:7]:7} | {o[12]:>11,.2f} | "
          f"{o[14]:>9,.2f} | {o[15]:>9,.2f} | {o[23]}")  # o[23] = comissao_status (deslocou +3 com CONTATO)

    w(f"\n--- Validacao da amostra (Casa do Alemao {fmt_brl(SAMPLE_VALOR)}, linha 2 do mestre do Ivan) ---")
    w(f"status: {sample['status']} (via {sample['via']})")
    for campo, esperado, achado, st in sample.get("detalhes", []):
        if isinstance(esperado, float):
            w(f"  {campo:22} esperado {esperado:>10,.2f} | achado {achado if achado is None else format(achado, '>10,.2f')} | {st}")

    w("\n--- Reconciliacao com a planilha oficial (Vendas_25_26, 08/06) ---")
    w(f"soma Valor gerada: {fmt_brl(total['soma'])} | oficial: {fmt_brl(REF_OFICIAL_0806)} | "
      f"delta: {fmt_brl(total['delta'])}")
    if total["won0"]:
        w(f"excluidas won=0 com valor e closedate (gap de STAGE no HubSpot, somam {fmt_brl(total['soma_won0'])}):")
        for did, cli, v, cd, stage in total["won0"]:
            w(f"  {did} | {cli[:45]:45} | {v:>12,.2f} | {cd} | {stage}")
    if total["valor0"]:
        w(f"excluidas valor 0/invalido (ruido): {[(d, c[:30]) for d, c in total['valor0']]}")
    if total["sigma"]:
        w("atencao (pendencia @ivan existente — venda real ou remover?):")
        for did, cli, v in total["sigma"]:
            w(f"  {did} | {cli[:45]} | {v:,.2f}")

    w("\n--- Divergencias ESPERADAS na reconciliacao (avisos gravados na _meta) ---")
    for i, a in enumerate(AVISOS_RECONCILIACAO, 1):
        w(f"  {i}. {a}")
    w("=" * 100)


# ===================================================
# ESCRITA (replica local do padrao write_to_sheets do sync.py;
# o de la e amarrado a Brada_Dashboard_Deals e a _meta dela)
# ===================================================

CURRENCY_COL_LETTERS = ["I", "M", "N", "O", "P", "Q"]
NUMBER_FORMAT = {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}


def write_tab(sh, name, header, rows):
    try:
        ws = sh.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=max(100, len(rows) + 20), cols=len(header) + 2)
    ws.clear()
    safe_rows = [[("" if c is None else c) for c in r] for r in rows]
    ws.update(values=[header] + safe_rows, range_name="A1")
    return ws


def apply_currency_formats(ws, n_rows, col_letters):
    if n_rows < 1:
        return
    end = n_rows + 1  # +1 do header
    ws.batch_format([{"range": f"{c}2:{c}{end}", "format": NUMBER_FORMAT} for c in col_letters])


def write_report(gc, report_id, records, cycle_records, cycle, totais, meta):
    sh = gc.open_by_key(report_id)
    full_header = TARGET_HEADER + CONTATO_HEADER + TECH_HEADER

    ws = write_tab(sh, "Controle de Vendas", full_header, [rec["out"] for rec in records])
    apply_currency_formats(ws, len(records), CURRENCY_COL_LETTERS)
    print(f"[write] Controle de Vendas: {len(records)} linhas")

    ws = write_tab(sh, f"{cycle}_Vendas", full_header, [rec["out"] for rec in cycle_records])
    apply_currency_formats(ws, len(cycle_records), CURRENCY_COL_LETTERS)
    print(f"[write] {cycle}_Vendas: {len(cycle_records)} linhas")

    th, trows = totais
    ws = write_tab(sh, "Totais", th, trows)
    apply_currency_formats(ws, len(trows), ["B", "C"])
    print(f"[write] Totais: {len(trows)} linhas")

    mh, mrows = meta
    write_tab(sh, "_meta", mh, mrows)
    print(f"[write] _meta: {len(mrows)} linhas")

    # Polish: remove a aba default vazia da planilha nova
    for default_name in ("Página1", "Sheet1"):
        try:
            ws0 = sh.worksheet(default_name)
            if not any(any(c.strip() for c in row) for row in ws0.get_all_values()):
                sh.del_worksheet(ws0)
                print(f"[write] aba default '{default_name}' (vazia) removida")
        except gspread.exceptions.WorksheetNotFound:
            pass


# ===================================================
# MAIN
# ===================================================

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser(description="Planilha financeira paralela (Fase 1: Vendas%)")
    ap.add_argument("--write", action="store_true",
                    help="escreve na planilha paralela (default: dry-run)")
    ap.add_argument("--sheet-id", default=None,
                    help="ID da planilha paralela (default: env REPORT_FINANCEIRO_SHEET_ID)")
    ap.add_argument("--cycle", default=None,
                    help="ciclo YYYY-MM (janela 21/mes-1 a 20/mes; default: ciclo corrente)")
    ap.add_argument("--max-preview", type=int, default=20)
    args = ap.parse_args()

    cycle = args.cycle or current_cycle()
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", cycle):
        raise SystemExit(f"--cycle invalido: {cycle!r} (esperado YYYY-MM)")

    report_id = args.sheet_id or REPORT_SHEET_ID
    if report_id == SOURCE_SPREADSHEET_ID:
        raise SystemExit("Guard rail: a planilha de saida NAO pode ser a Brada_Dashboard_Deals (fonte).")

    gc = get_sheets_client()
    rows, fonte_ts = load_consolidado(gc)
    incluidas, excluidas = split_vendas(rows)
    records = sort_records([build_record(r) for r in incluidas])
    # Sprint D: preserva as 5 colunas manuais por deal_id (le a aba atual ANTES do
    # clear+write). Roda tambem em dry-run, pro preview refletir o que sera escrito.
    preserved = read_preserved_manual(gc, report_id)
    n_preserved = apply_preservation(records, preserved)
    print(f"[preservacao] {n_preserved} deal(s) com colunas manuais reinjetadas "
          f"(de {len(preserved)} preservaveis no sheet atual)")
    cycle_records = cut_cycle(records, cycle)

    counts = compute_counts(rows, records, cycle_records)
    sample = validate_sample(rows)
    total = validate_total(records, excluidas)
    totais = build_totais(records, cycle_records, cycle)
    meta = build_meta(args, cycle, counts, sample, total, fonte_ts)

    print_report(counts, records, cycle_records, cycle, sample, total, fonte_ts, args.max_preview)

    if not args.write:
        print("\n[dry-run] nenhuma escrita feita. Use --write para gravar na planilha paralela.")
        return

    # Gates de escrita (comissao = folha)
    if len(rows) < MIN_ROWS_GUARD:
        raise SystemExit(f"Abortado: consolidado com {len(rows)} linhas (< {MIN_ROWS_GUARD}) — "
                         "possivel leitura no meio do clear+write do sync horario. Nao escrevi nada.")
    if sample["status"] != "PASS":
        print(f"\nAbortado: validacao da amostra = {sample['status']} — o calculo upstream "
              "(build_consolidado_layer) pode ter mudado. Nao escrevi nada.")
        sys.exit(2)

    write_report(gc, report_id, records, cycle_records, cycle, totais, meta)
    print(f"\nOK: planilha paralela atualizada ({report_id}). "
          f"Controle de Vendas={len(records)} | {cycle}_Vendas={len(cycle_records)}")


if __name__ == "__main__":
    main()
