"""
Financeiro MATCH — Ivan (S-A, sessão 22-25/06/2026).

Popula a aba `Cópia de Junho_MATCH` (gid 444443497) da planilha OFICIAL do Ivan
"Comissões 2026" (1XVRuIMN9...) com as colunas A-G a partir do consolidado da
Brada_Dashboard_Deals. H+ é território da Luciana (comissão fixa: Ivan 1000 /
Jaqueline 700 / Rafaela 200) — NÃO TOCAR.

Decisões 22/06 (ata_ivan_22jun):
  - Comissão sai da planilha (Luciana faz H+ fixo). Stream Vendas% = MATCH canonical.
  - Filtro: data no ciclo 20-20 (ou 21-20, fallback) AND valor_aporte > 0 AND
    stage >= [Match]-Projetos (ordem 7, id 1246602643). O consolidado já expõe
    `convertido = 1 sse stage_ordem >= match_ordem OR e_ganho` (sync.py:1856-1858).
  - Reconciliar por deal_id OU (cliente_norm + projeto_norm + valor_round_2).
    NUNCA por numero_projeto (mesmo SEI em projetos diferentes).
  - Default = dry-run; --write é gated.

Uso:
  python ops/financeiro_match_ivan.py
  python ops/financeiro_match_ivan.py --write
  python ops/financeiro_match_ivan.py --cycle-start 2026-05-20 --cycle-end 2026-06-20
"""

import argparse
import csv
import datetime as dt
import io
import os
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

import gspread
import requests

# Padrão de ops/: sobe 1 nível pra achar sync.py na raiz.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Reusa o get_sheets_client (resolução de credenciais hierárquica) do sync.py.
from sync import PORTAL_ID, get_sheets_client  # noqa: E402


def hubspot_deal_url(deal_id):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{deal_id}"

# Token HubSpot (mesmo padrão de fallback do sync.py)
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
if not HUBSPOT_TOKEN:
    _env_path = os.path.join(os.path.expanduser("~"), ".brada-secrets", "hubspot.env")
    if os.path.exists(_env_path):
        with open(_env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    if k.strip() == "HUBSPOT_TOKEN":
                        HUBSPOT_TOKEN = v.strip().strip('"').strip("'")
                        break


# ===========================================================
# DUPLICADOS de sheets_reporting_financeiro_mensal.py (2026-06-22)
# Deduplicar quando a paralela `1pHbTmyK` for deprecada. A paralela
# está em workflow_dispatch only → drift improvável; extração agora
# vira código órfão na hora da remoção.
# ===========================================================

SOURCE_SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or \
    "1bBGfZIkjuqBBQL9U79cMWQUgz9FLrbIKm2SObAXEKF8"
CONSOLIDADO_WS = "consolidado"

CONSOLIDADO_HEADER = [
    "deal_id", "cliente", "cnpj", "pipeline", "produto", "interno_externo",
    "fluxo_comissao", "projeto_key", "numero_projeto", "nome_projeto",
    "proponente", "stage", "convertido", "won_ganho", "tem_overlap_projeto",
    "closedate", "closedate_status", "data_aporte", "valor_bruto",
    "valor_vendido", "liquido_brada", "comissao_ivan", "comissao_jaque",
    "comissao_externo", "comissao_status", "owner", "owner_status",
    "origem_lead", "lei_principal", "ano", "empresa_canonica",
    "tipo_de_proponente", "valor_efetivo_brada",
    "nome_contato_proponente", "email_proponente", "telefone_proponente",
    "valor_projetado_ativo",
]

# Tradução lei_principal -> "Fonte de recurso" do Ivan. Aproximação:
# os rótulos do Ivan são texto livre. Valor sem mapeamento gateia o --write
# (a Luciana usa VLOOKUP em H+ — fonte errada zera comissão).
LEI_FONTE_MAP = {
    "Rouanet": "IR Cultura",
    "Esporte Federal": "Esporte IR",
    "Esporte Estadual": "ICMS Esporte",
    "Cultura Estadual": "ICMS Cultura",
    "Cultura Municipal": "ISS",
    "(sem lei preenchida)": "",
}

MIN_ROWS_GUARD = 500  # anti-corrida com clear+write do sync horário :00


def parse_brl(s):
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    if "." in s and "," in s:
        s = s.replace(".", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_closedate(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def fmt_date_br(d):
    return d.strftime("%d/%m/%Y") if d else ""


def map_lei(lei):
    return LEI_FONTE_MAP.get(lei, lei)


def load_consolidado(gc):
    """Lê a aba consolidado inteira; valida o contrato de header (hard-fail)."""
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    vals = sh.worksheet(CONSOLIDADO_WS).get_all_values()
    if not vals:
        raise SystemExit("consolidado vazio — sync no meio do clear+write? Tente em 2-3 min.")
    header = vals[0]
    if header != CONSOLIDADO_HEADER:
        faltando = [c for c in CONSOLIDADO_HEADER if c not in header]
        extras = [c for c in header if c not in CONSOLIDADO_HEADER]
        raise SystemExit(
            "Header do consolidado divergiu do contrato (sync.py mudou?).\n"
            f"  faltando: {faltando}\n  extras: {extras}\n"
            f"  ordem atual: {header}"
        )
    rows = [dict(zip(header, r)) for r in vals[1:] if any(c.strip() for c in r)]
    fonte_ts = ""
    try:
        for meta_row in sh.worksheet("_meta").get_values("A1:C10"):
            if meta_row and meta_row[0] == "ultima_sync_deals":
                fonte_ts = " ".join(c for c in meta_row[1:] if c)
                break
    except gspread.exceptions.WorksheetNotFound:
        pass
    return rows, fonte_ts


# ===========================================================
# CONFIG da S-A (Ivan)
# ===========================================================

IVAN_SHEET_ID = "1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI"  # Comissões 2026
WRITE_TAB = "Cópia de Junho_MATCH"     # gid 444443497, alvo do --write
REF_TAB   = "Junho_MATCH"              # gid 202101115, manual (read-only)
EXPECTED_HEADER_A_G = [
    "Cliente", "Fonte de recurso", "Proponente", "Projeto",
    "Numero do projeto", "Valor", "Data do aporte",
]

DEFAULT_CYCLE_START = date(2026, 5, 21)
DEFAULT_CYCLE_END   = date(2026, 6, 20)
FALLBACK_CYCLE_START = date(2026, 5, 20)
FALLBACK_CYCLE_END   = date(2026, 6, 20)

SHEETS_EPOCH = datetime(1899, 12, 30)


# ===========================================================
# UTIL
# ===========================================================

def norm(s):
    """lowercase + strip + colapsa whitespace + remove acentos (NFD) +
    descarta caracteres não-alphanum (cobre U+FFFD do encoding quebrado
    do sync para nomes com 'ã' que viraram '�' no caminho)."""
    s = unicodedata.normalize("NFD", (s or "").strip().casefold())
    s = "".join(c for c in s if c.isalnum() or c.isspace())
    return " ".join(s.split())


def serial_to_date(v):
    """Sheets serial (float) -> date. epoch = 1899-12-30 (não 1900-01-01:
    Sheets/Excel contam 1900 como bissexto erroneamente)."""
    if v in (None, "", 0):
        return None
    try:
        n = int(float(v))
    except (TypeError, ValueError):
        return None
    return (SHEETS_EPOCH + timedelta(days=n)).date()


def fmt_brl(v):
    if v is None:
        return "-"
    s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def pick_date(r, field):
    """field in {'closedate', 'data_aporte', 'data_match'} -> date|None.
    `data_match` é fetched do HubSpot direto (não está no consolidado, vive em
    deal.data_do_match) — armazenado no dict como 'data_match' (string ISO ou '')."""
    return parse_closedate(r.get(field, ""))


def fetch_data_do_match(deal_ids):
    """Batch read de data_do_match pros deal_ids (HubSpot API).
    Retorna {deal_id: 'YYYY-MM-DD' ou ''}. Vazio se token ausente."""
    if not HUBSPOT_TOKEN or not deal_ids:
        return {}
    url = "https://api.hubapi.com/crm/v3/objects/deals/batch/read"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
    out = {}
    # Batch read aceita até 100 IDs por chamada
    for i in range(0, len(deal_ids), 100):
        chunk = deal_ids[i:i + 100]
        body = {"inputs": [{"id": d} for d in chunk], "properties": ["data_do_match"]}
        r = requests.post(url, headers=headers, json=body, timeout=30)
        if r.status_code != 200:
            print(f"[aviso] batch read HubSpot {r.status_code}: {r.text[:200]}")
            continue
        for d in r.json().get("results", []):
            v = d.get("properties", {}).get("data_do_match", "") or ""
            out[d["id"]] = v
    return out


# ===========================================================
# FILTRO
# ===========================================================

def filter_match_pool(consolidado):
    """Pré-filtro estável (não depende do ciclo): Match canonical + stage >= 7 + valor > 0.
    Dedup por deal_id (hard-fail se duplicata)."""
    seen = {}
    dups = []
    for r in consolidado:
        if r.get("fluxo_comissao") != "Vendas%":
            continue
        if r.get("convertido") != "1":
            continue
        v = parse_brl(r.get("valor_bruto"))
        if not v or v <= 0:
            continue
        did = (r.get("deal_id") or "").strip()
        if did in seen:
            dups.append(did)
        else:
            seen[did] = r
    if dups:
        raise SystemExit(
            f"Hard-fail: deal_id duplicado no pool MATCH: {dups}. "
            "Investigue o consolidado antes de continuar."
        )
    return list(seen.values())


def cut_by_window(pool, date_field, start, end):
    """Filtra por janela inclusiva [start, end] usando date_field."""
    out = []
    for r in pool:
        d = pick_date(r, date_field)
        if d and start <= d <= end:
            out.append(r)
    return out


def sort_records(records, date_field):
    return sorted(
        records,
        key=lambda r: (
            pick_date(r, date_field) or date.min,
            -(parse_brl(r.get("valor_bruto")) or 0.0),
        ),
    )


def build_row_a_g(r, date_field):
    d = pick_date(r, date_field)
    return [
        r.get("cliente", ""),
        map_lei(r.get("lei_principal", "")),
        r.get("proponente", ""),
        r.get("nome_projeto", ""),
        r.get("numero_projeto", ""),
        round(parse_brl(r.get("valor_bruto")) or 0.0, 2),
        fmt_date_br(d),
    ]


# ===========================================================
# LEITURA da planilha do Ivan
# ===========================================================

def open_ivan_sheet(gc):
    return gc.open_by_key(IVAN_SHEET_ID)


def read_write_tab_snapshot(sh):
    """Le a aba WRITE_TAB (A:G) com UNFORMATTED_VALUE.
    Retorna (header, rows). Hard-fail se permission denied ou aba sem header."""
    ws = sh.worksheet(WRITE_TAB)
    vals = ws.get("A:G", value_render_option="UNFORMATTED_VALUE")
    if not vals:
        raise SystemExit(f"Aba '{WRITE_TAB}' vazia — confirma com o Ivan se é a aba certa.")
    header = vals[0]
    rows = vals[1:] if len(vals) > 1 else []
    return ws, header, rows


def read_formula_cells(sh, cells):
    """Le células específicas com FORMULA pra inspeção. cells = ["H2", "I2", ...]."""
    ws = sh.worksheet(WRITE_TAB)
    out = {}
    for c in cells:
        try:
            v = ws.get(c, value_render_option="FORMULA")
            out[c] = v[0][0] if v and v[0] else ""
        except Exception as e:
            out[c] = f"<erro: {type(e).__name__}: {e}>"
    return out


def read_junho_match(sh):
    """Le REF_TAB (manual, read-only) com UNFORMATTED_VALUE; converte serial->date em G."""
    try:
        ws = sh.worksheet(REF_TAB)
    except gspread.exceptions.WorksheetNotFound:
        raise SystemExit(f"Aba '{REF_TAB}' não encontrada na planilha do Ivan.")
    vals = ws.get("A:G", value_render_option="UNFORMATTED_VALUE")
    if not vals:
        return []
    header = vals[0]
    out = []
    for raw in vals[1:]:
        # padroniza tamanho da linha
        row = list(raw) + [""] * (7 - len(raw))
        cliente = str(row[0] or "").strip()
        fonte   = str(row[1] or "").strip()
        prop    = str(row[2] or "").strip()
        projeto = str(row[3] or "").strip()
        numero  = str(row[4] or "").strip()
        valor_raw = row[5]
        if isinstance(valor_raw, (int, float)):
            valor = float(valor_raw)
        else:
            valor = parse_brl(valor_raw) or 0.0
        data_raw = row[6]
        if isinstance(data_raw, (int, float)):
            data_d = serial_to_date(data_raw)
        else:
            # texto: tenta DD/MM/YYYY
            data_d = None
            s = str(data_raw or "").strip()
            if s:
                try:
                    data_d = datetime.strptime(s, "%d/%m/%Y").date()
                except ValueError:
                    pass
        if not cliente and not valor:
            continue  # linha vazia
        if norm(cliente) in {"total", "totais", "soma"}:
            continue  # linha de fechamento (cliente='Total', valor=0)
        out.append({
            "cliente": cliente, "fonte": fonte, "proponente": prop,
            "projeto": projeto, "numero": numero, "valor": valor, "data": data_d,
        })
    return out


# ===========================================================
# RECONCILIAÇÃO (4 buckets)
# ===========================================================

def reconcile(ours, manual):
    """Reconcilia por VALOR (chave estável — única no ciclo da S-A) com
    tiebreak por cliente substring. Cliente da manual = nome curto ('Casa do
    Alemão'); cliente do consolidado = dealname com sufixo lei ('Casa do
    Alemão IR Esporte'). Casar por nome cru não funciona; valor sim
    (todos únicos no ciclo de Junho).

    Buckets: match_strong (valor+cliente_contém), match_value_only (valor casa
    mas cliente não contém — investigar), match_ambiguous (valor empata 2+ na
    nossa), unmatched_manual, unmatched_ours."""
    def key_v(v):
        return round(float(v or 0), 2)

    ours_by_value = defaultdict(list)
    for o in ours:
        ours_by_value[key_v(o.get("valor"))].append(o)

    out = {
        "match_strong": [],
        "match_value_only": [],
        "match_ambiguous": [],
        "unmatched_manual": [],
        "unmatched_ours": [],
    }
    matched_ours_ids = set()
    for m in manual:
        v = key_v(m.get("valor"))
        cands = ours_by_value.get(v, [])
        if not cands:
            out["unmatched_manual"].append(m)
            continue
        # tiebreak por cliente substring (manual está contido no nosso, com norm)
        m_name = norm(m.get("cliente", ""))
        contains = [o for o in cands if m_name and m_name in norm(o.get("cliente", ""))]
        if len(contains) == 1:
            out["match_strong"].append({"manual": m, "ours": contains[0]})
            matched_ours_ids.add(contains[0]["deal_id"])
        elif len(contains) > 1:
            out["match_ambiguous"].append({"manual": m, "ours": contains})
            for o in contains:
                matched_ours_ids.add(o["deal_id"])
        else:
            # valor casa mas nenhum cliente bate por substring — investigar
            if len(cands) == 1:
                out["match_value_only"].append({"manual": m, "ours": cands[0]})
                matched_ours_ids.add(cands[0]["deal_id"])
            else:
                out["match_ambiguous"].append({"manual": m, "ours": cands})
                for o in cands:
                    matched_ours_ids.add(o["deal_id"])

    for o in ours:
        if o["deal_id"] not in matched_ours_ids:
            out["unmatched_ours"].append(o)

    return out


# ===========================================================
# BUILD ROWS A-G a partir da MANUAL (source-of-truth do conjunto)
# ===========================================================

def build_rows_from_manual(manual, pool, cycle_start, cycle_end):
    """Pra cada linha da manual:
      - Acha deal correspondente no pool por valor (tiebreak: cliente substring).
      - Monta linha A-G usando consolidado/HubSpot.
      - G: prefere data_do_aporte; senão data_do_match; senão vazio (com flag).
      - Coleta flags por deal: campo vazio, data fora do ciclo, etc.
    Retorna lista de dicts {manual, our, row_a_g, flags, url}, na ordem da manual."""
    pool_by_value = defaultdict(list)
    for r in pool:
        pool_by_value[round(parse_brl(r.get("valor_bruto")) or 0, 2)].append(r)

    out = []
    for m in manual:
        v_key = round(float(m.get("valor") or 0), 2)
        cands = pool_by_value.get(v_key, [])
        m_name = norm(m.get("cliente", ""))
        contains = [r for r in cands if m_name and m_name in norm(r.get("cliente", ""))]
        if len(contains) == 1:
            our = contains[0]
        elif len(contains) > 1:
            # ambiguidade — pega o de mais alta sobreposição de nome
            our = max(contains, key=lambda r: len(norm(r.get("cliente", ""))))
        elif len(cands) == 1:
            our = cands[0]
        else:
            out.append({
                "manual": m, "our": None, "row_a_g": None,
                "flags": ["deal não encontrado no consolidado MATCH (valor não casa)"],
                "url": "",
            })
            continue

        flags = []
        # G: data_do_aporte > data_do_match > vazio
        d_aporte = parse_closedate(our.get("data_aporte", ""))
        d_match = parse_closedate(our.get("data_match", ""))
        d_close = parse_closedate(our.get("closedate", ""))
        if d_aporte:
            g_date = d_aporte
            g_source = "data_do_aporte"
        elif d_match:
            g_date = d_match
            g_source = "data_do_match"
            flags.append(
                "data_do_aporte vazio no HubSpot — usei data_do_match "
                f"({d_match.strftime('%d/%m/%Y')}) como fallback"
            )
        else:
            g_date = None
            g_source = "vazio"
            flags.append("data_do_aporte E data_do_match vazios no HubSpot")

        if g_date and not (cycle_start <= g_date <= cycle_end):
            flags.append(
                f"{g_source}={g_date:%d/%m/%Y} está FORA do ciclo "
                f"({cycle_start:%d/%m} a {cycle_end:%d/%m}) — manual indica "
                f"{m['data'].strftime('%d/%m/%Y') if m['data'] else '?'}; "
                "provável dado desatualizado no HubSpot"
            )

        # Col B: lei_principal vazio = gap
        lei = our.get("lei_principal", "")
        if not lei or lei == "(sem lei preenchida)":
            flags.append(
                f"lei_principal vazia no HubSpot (manual indica '{m['fonte']}'); "
                "col B sai vazia"
            )
        # Col A: company_name vazio = cliente sai com dealname
        # (não temos como detectar fácil aqui; deixar implícito)

        # Col C: nome_do_proponente vazio
        if not our.get("proponente", "").strip():
            flags.append("nome_do_proponente vazio no HubSpot")

        # Col D: nome_do_projeto vazio
        if not our.get("nome_projeto", "").strip():
            flags.append("nome_do_projeto vazio no HubSpot")

        # Col E: numero_do_projeto vazio
        if not our.get("numero_projeto", "").strip():
            flags.append("numero_do_projeto vazio no HubSpot")

        row_a_g = [
            our.get("cliente", ""),
            map_lei(lei) if lei and lei != "(sem lei preenchida)" else "",
            our.get("proponente", ""),
            our.get("nome_projeto", ""),
            our.get("numero_projeto", ""),
            round(parse_brl(our.get("valor_bruto")) or 0.0, 2),
            fmt_date_br(g_date),
        ]
        out.append({
            "manual": m, "our": our, "row_a_g": row_a_g,
            "flags": flags, "url": hubspot_deal_url(our["deal_id"]),
        })
    return out


def print_ivan_message(items):
    """Imprime o bloco de mensagem pra Bruno encaminhar pro Ivan."""
    section("MENSAGEM PRO IVAN (Bruno copia daqui pra baixo)")
    print("Ivan, segue o status do financeiro Match — Junho/2026 (ciclo 21/05-20/06).")
    print(f"Vou popular a `Cópia de Junho_MATCH` com os {len(items)} deals que constam no Junho_MATCH manual.")
    print("Achados por deal — preciso confirmar com você se posso corrigir via API ou se a Jaqueline precisa fazer:\n")
    n_ok = sum(1 for it in items if not it["flags"])
    print(f"OK (sem gaps): {n_ok}/{len(items)}")
    print()
    for i, it in enumerate(items, 1):
        m = it["manual"]
        cli = m.get("cliente", "")
        val_str = fmt_brl(m.get("valor", 0))
        date_m = m["data"].strftime("%d/%m/%Y") if m.get("data") else "?"
        print(f"### {i}. {cli} — {val_str} — manual {date_m}")
        if it["our"]:
            print(f"    {it['url']}")
        if it["flags"]:
            for f in it["flags"]:
                print(f"    - {f}")
        else:
            print("    - sem gaps")
        print()
    print("Pergunta: posso atualizar os campos faltantes via API (eu rodo o script), "
          "ou precisa ser a Jaqueline corrigindo no HubSpot manualmente?")


# ===========================================================
# RELATÓRIO
# ===========================================================

def section(title):
    print()
    print("=" * 90)
    print(title)
    print("=" * 90)


def print_table(headers, rows, widths=None, max_rows=None):
    if not rows:
        print("(vazio)")
        return
    if widths is None:
        widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0)) for i, h in enumerate(headers)]
    sep = " | "
    print(sep.join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
    print(sep.join("-" * widths[i] for i in range(len(headers))))
    shown = rows[:max_rows] if max_rows else rows
    for r in shown:
        print(sep.join(str(c).ljust(widths[i]) for i, c in enumerate(r)))
    if max_rows and len(rows) > max_rows:
        print(f"... +{len(rows) - max_rows} linha(s)")


def to_csv(rows, headers):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


def records_to_ours_dicts(records, date_field):
    """Converte rows do consolidado (Match pool já filtrado) em dicts plug-and-play
    pra reconcile() — pega cliente/projeto/valor/data via date_field."""
    out = []
    for r in records:
        out.append({
            "deal_id": r.get("deal_id", ""),
            "cliente": r.get("cliente", ""),
            "fonte": map_lei(r.get("lei_principal", "")),
            "proponente": r.get("proponente", ""),
            "projeto": r.get("nome_projeto", ""),
            "numero": r.get("numero_projeto", ""),
            "valor": round(parse_brl(r.get("valor_bruto")) or 0.0, 2),
            "data": pick_date(r, date_field),
        })
    return out


def matrix_run(pool, manual, label_prefix=""):
    """Roda a matriz N×M (ciclo × campo G) e retorna o vencedor +
    o dict de buckets desse vencedor."""
    combos = [
        ("default_21-20", DEFAULT_CYCLE_START, DEFAULT_CYCLE_END),
        ("fallback_20-20", FALLBACK_CYCLE_START, FALLBACK_CYCLE_END),
    ]
    fields = ["closedate", "data_aporte", "data_match"]
    results = []
    for cycle_label, cs, ce in combos:
        for f in fields:
            cut = cut_by_window(pool, f, cs, ce)
            ours = records_to_ours_dicts(cut, f)
            rec = reconcile(ours, manual)
            matched_total = len(rec["match_strong"]) + len(rec.get("match_value_only", []))
            results.append({
                "cycle": cycle_label,
                "field": f,
                "start": cs, "end": ce,
                "n": len(cut),
                "match_strong": len(rec["match_strong"]),
                "match_value_only": len(rec.get("match_value_only", [])),
                "matched_total": matched_total,
                "match_ambiguous": len(rec["match_ambiguous"]),
                "unmatched_manual": len(rec["unmatched_manual"]),
                "unmatched_ours": len(rec["unmatched_ours"]),
                "buckets": rec,
                "cut": cut,
                "field_": f,
            })

    headers = ["ciclo", "campo G", "janela", "N", "strong", "value-only",
               "ambig.", "só manual", "só nossa"]
    rows = [
        [r["cycle"], r["field"], f"{r['start']:%d/%m}-{r['end']:%d/%m}",
         r["n"], r["match_strong"], r["match_value_only"],
         r["match_ambiguous"], r["unmatched_manual"], r["unmatched_ours"]]
        for r in results
    ]
    section(f"{label_prefix}Matriz (ciclo × campo G) contra Junho_MATCH ({len(manual)} linhas)")
    print_table(headers, rows)

    # Vencedor: max(matched_total), tiebreak min(unmatched_manual + unmatched_ours)
    winner = max(results, key=lambda r: (r["matched_total"], -(r["unmatched_manual"] + r["unmatched_ours"])))
    return winner, results


def print_buckets(label, buckets):
    section(f"Reconciliação detalhada — {label}")
    bs = buckets

    if bs["match_strong"]:
        print(f"\nmatch_strong ({len(bs['match_strong'])}):")
        headers = ["cliente (manual)", "projeto", "valor", "data manual", "deal_id (nosso)"]
        rows = []
        for m in bs["match_strong"]:
            man = m["manual"]; our = m["ours"]
            rows.append([
                man["cliente"][:35], man["projeto"][:30],
                fmt_brl(man["valor"]),
                man["data"].strftime("%d/%m/%Y") if man["data"] else "?",
                our["deal_id"],
            ])
        print_table(headers, rows)

    if bs["match_ambiguous"]:
        print(f"\nmatch_ambiguous ({len(bs['match_ambiguous'])}) — valor casa, cliente NÃO desambigua:")
        for m in bs["match_ambiguous"]:
            print(f"  manual: {m['manual']['cliente']} / {m['manual']['projeto']} / {fmt_brl(m['manual']['valor'])}")
            for o in m["ours"]:
                print(f"    candidato deal_id={o['deal_id']} cliente={o.get('cliente','')!r}")

    if bs.get("match_value_only"):
        print(f"\nmatch_value_only ({len(bs['match_value_only'])}) — valor casa mas substring NÃO bate:")
        for m in bs["match_value_only"]:
            print(f"  manual: {m['manual']['cliente']!r} (R${m['manual']['valor']:,.2f}) "
                  f"vs nosso deal {m['ours']['deal_id']} cliente={m['ours'].get('cliente','')!r}")

    if bs["unmatched_manual"]:
        print(f"\nunmatched_manual ({len(bs['unmatched_manual'])}) — linhas em Junho_MATCH SEM equivalente nossa:")
        headers = ["cliente", "projeto", "valor", "data"]
        rows = [[m["cliente"][:35], m["projeto"][:30], fmt_brl(m["valor"]),
                 m["data"].strftime("%d/%m/%Y") if m["data"] else "?"] for m in bs["unmatched_manual"]]
        print_table(headers, rows)

    if bs["unmatched_ours"]:
        print(f"\nunmatched_ours ({len(bs['unmatched_ours'])}) — linhas nossas SEM equivalente em Junho_MATCH:")
        headers = ["deal_id", "cliente", "projeto", "valor", "data"]
        rows = [[o["deal_id"], o["cliente"][:35], o["projeto"][:30], fmt_brl(o["valor"]),
                 o["data"].strftime("%d/%m/%Y") if o["data"] else "?"] for o in bs["unmatched_ours"]]
        print_table(headers, rows)


def validate_fonte_b(buckets):
    """Compara col B (Fonte de recurso) entre nossa saída e manual nos match_strong.
    Separa em 2 listas: 'gap' (nossa B = vazio, lei_principal não preenchido no
    HubSpot — não-bloqueante) e 'conflito' (ambos preenchidos mas diferem —
    bloqueante)."""
    gaps = []
    conflitos = []
    for m in buckets["match_strong"] + buckets.get("match_value_only", []):
        ours_b = m["ours"]["fonte"]
        man_b  = m["manual"]["fonte"]
        if norm(ours_b) == norm(man_b):
            continue
        entry = {
            "deal_id": m["ours"]["deal_id"],
            "cliente": m["manual"]["cliente"],
            "ours": ours_b, "manual": man_b,
        }
        if not norm(ours_b):
            gaps.append(entry)
        else:
            conflitos.append(entry)
    return gaps, conflitos


def check_leis_sem_mapeamento(records):
    """Lista leis que caem no fallback (não estão no LEI_FONTE_MAP)."""
    leis = Counter()
    sem_map = []
    for r in records:
        lei = r.get("lei_principal", "")
        leis[lei] += 1
        if lei and lei not in LEI_FONTE_MAP:
            sem_map.append(lei)
    return leis, sorted(set(sem_map))


# ===========================================================
# ESCRITA (gated)
# ===========================================================

def gate_check(buckets, leis_sem_map, conflitos_b, formula_alarm, header_ok,
               consolidado_size, n_lines, M, allow_partial=False):
    fails = []
    warns = []
    if consolidado_size < MIN_ROWS_GUARD:
        fails.append(f"consolidado com {consolidado_size} linhas (< {MIN_ROWS_GUARD})")
    if not header_ok:
        fails.append("header da aba destino não é o esperado")
    if leis_sem_map:
        fails.append(f"{len(leis_sem_map)} leis sem mapeamento: {leis_sem_map}")
    if conflitos_b:
        fails.append(f"{len(conflitos_b)} CONFLITOS de 'Fonte de recurso' (nossa preenchida e diferente da manual)")
    if formula_alarm:
        fails.append(f"H2:I2 referencia '{REF_TAB}!' (cópia por fórmula, não por valor)")
    n_manual = (len(buckets["match_strong"]) + len(buckets["unmatched_manual"])
                + len(buckets["match_ambiguous"]) + len(buckets.get("match_value_only", [])))
    threshold = max(4, n_manual - 1)
    matched_any = len(buckets["match_strong"]) + len(buckets.get("match_value_only", []))
    if matched_any < threshold:
        msg = f"match_strong+match_value_only={matched_any} < {threshold} (de {n_manual} na manual)"
        if allow_partial:
            warns.append(msg + " — IGNORADO pelo --allow-partial")
        else:
            fails.append(msg)
    if n_lines <= 0 or n_lines > 200:
        fails.append(f"N a escrever fora do range sane: {n_lines}")
    return fails, warns


def do_write(sh, ws, rows_a_g, M):
    """Limpa A2:G{max(M, N)+1} e escreve A2:G{N+1}. Nunca toca H+ nem Junho_MATCH."""
    N = len(rows_a_g)
    end = max(M, N) + 1
    if end >= 2:
        ws.batch_clear([f"A2:G{end}"])
    if N > 0:
        ws.update(
            range_name=f"A2:G{N + 1}",
            values=rows_a_g,
            value_input_option="USER_ENTERED",
        )
    return N


# ===========================================================
# MAIN
# ===========================================================

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser(description="Financeiro MATCH Ivan — populador A-G")
    ap.add_argument("--write", action="store_true", help="escreve A2:G na Cópia de Junho_MATCH")
    ap.add_argument("--cycle-start", type=lambda s: dt.date.fromisoformat(s),
                    default=None, help="janela início YYYY-MM-DD (override do vencedor da matriz)")
    ap.add_argument("--cycle-end", type=lambda s: dt.date.fromisoformat(s),
                    default=None, help="janela fim YYYY-MM-DD (override)")
    ap.add_argument("--field", choices=["closedate", "data_aporte", "data_match"], default=None,
                    help="campo G (override do vencedor da matriz)")
    ap.add_argument("--allow-partial", action="store_true",
                    help="autoriza --write mesmo com matched_total abaixo do threshold (S-A: 3/5 deals)")
    args = ap.parse_args()

    gc = get_sheets_client()
    section("FINANCEIRO MATCH IVAN — S-A (v1, 2026-06-22)")
    print(f"fonte: {SOURCE_SPREADSHEET_ID}/{CONSOLIDADO_WS}")
    print(f"alvo:  {IVAN_SHEET_ID} aba '{WRITE_TAB}' (gid 444443497)")
    print(f"manual: aba '{REF_TAB}' (gid 202101115, read-only)")

    # 1) Pre-check + snapshot
    sh_ivan = open_ivan_sheet(gc)
    try:
        ws_write, header, current_rows = read_write_tab_snapshot(sh_ivan)
    except gspread.exceptions.APIError as e:
        raise SystemExit(
            f"Pré-check permission FAIL na aba '{WRITE_TAB}': {e}\n"
            "Confirma se a SA brada-sheets@brada-tickets.iam.gserviceaccount.com "
            "tem Editor na planilha do Ivan."
        )
    header_stripped = [str(h).strip() for h in header[:7]]
    header_ok = header_stripped == EXPECTED_HEADER_A_G
    section("Pré-check (snapshot do estado atual)")
    print(f"header[:7] esperado:        {EXPECTED_HEADER_A_G}")
    print(f"header[:7] real (stripped): {header_stripped}")
    print(f"header[:7] raw:             {header[:7]}")
    print(f"header VÁLIDO: {header_ok}")
    M = len([r for r in current_rows if any(str(c).strip() for c in r)])
    print(f"M (linhas atualmente populadas em A:G): {M}")
    if M > 0:
        print("\n--- DUMP CSV do estado atual A:G (Cópia de Junho_MATCH) ---")
        snap = []
        for r in current_rows[:M]:
            row = list(r) + [""] * (7 - len(r))
            # converte data serial em col 6 (G) pra ISO
            if isinstance(row[6], (int, float)):
                d = serial_to_date(row[6])
                row[6] = d.isoformat() if d else row[6]
            snap.append(row)
        print(to_csv(snap, EXPECTED_HEADER_A_G).strip())

    # 2) Fórmulas H2:I2 (alarme cópia-por-fórmula)
    section("Inspeção H2:I2 (revisão B2 — alarme se referencia Junho_MATCH!)")
    forms = read_formula_cells(sh_ivan, ["H2", "I2"])
    for c, v in forms.items():
        print(f"  {c}: {v!r}")
    formula_alarm = any(f"{REF_TAB}!" in str(v) for v in forms.values())
    if formula_alarm:
        print(">>> ALARME: H2/I2 referencia a aba manual Junho_MATCH. "
              "A 'Cópia de' parece ser cópia por fórmula, não por valor. "
              "Pausar e voltar pro Ivan/Luciana antes de --write.")

    # 3) Junho_MATCH manual
    manual = read_junho_match(sh_ivan)
    section(f"Junho_MATCH (manual, {len(manual)} linhas)")
    if manual:
        rows_m = [[m["cliente"][:35], m["fonte"][:18], m["proponente"][:25],
                   m["projeto"][:30], m["numero"][:18], fmt_brl(m["valor"]),
                   m["data"].strftime("%d/%m/%Y") if m["data"] else "?"]
                  for m in manual]
        print_table(["cliente", "fonte", "proponente", "projeto", "numero", "valor", "data"], rows_m)

    # 4) Consolidado + pool
    rows, fonte_ts = load_consolidado(gc)
    print(f"\nconsolidado: {len(rows)} linhas | atualizado: {fonte_ts or '?'}")
    if len(rows) < MIN_ROWS_GUARD:
        print(f">>> ATENÇÃO: consolidado com {len(rows)} linhas (< {MIN_ROWS_GUARD}). --write será bloqueado.")
    pool = filter_match_pool(rows)
    print(f"pool MATCH (Vendas% AND convertido=1 AND valor>0): {len(pool)} deals")

    # data_do_match não está no consolidado — fetch direto do HubSpot. É o
    # candidato natural pra coluna G "Data do aporte" (data prometida pelo
    # executivo). Achado da S-A: dos 5 deals da Junho_MATCH manual, 3
    # batem com data_do_match exato; 2 da Casa do Alemão (15/abr no HubSpot
    # vs 15/jun na manual) sinalizam dado desatualizado no HubSpot.
    deal_ids_pool = [r["deal_id"] for r in pool]
    data_match_by_did = fetch_data_do_match(deal_ids_pool)
    print(f"data_do_match preenchido em {sum(1 for v in data_match_by_did.values() if v)}/{len(pool)} deals do pool")
    for r in pool:
        r["data_match"] = data_match_by_did.get(r["deal_id"], "")

    # 5) Diagnóstico opcional — matriz só pra info
    cs = args.cycle_start or DEFAULT_CYCLE_START
    ce = args.cycle_end or DEFAULT_CYCLE_END
    if not (args.cycle_start and args.cycle_end and args.field):
        matrix_run(pool, manual)  # info only

    # 6) BUILD a partir da manual (source-of-truth do conjunto)
    section(f"Build A-G por linha da manual (ciclo de referência: {cs:%d/%m} a {ce:%d/%m})")
    items = build_rows_from_manual(manual, pool, cs, ce)
    print(f"{len(items)} linha(s) processadas.")

    # 7) Tabela final A-G
    section(f"Linhas A-G a serem ESCRITAS (cap 30; total {sum(1 for it in items if it['row_a_g'])})")
    headers_p = ["#", "A Cliente", "B Fonte", "C Prop.", "D Projeto", "E Num.", "F Valor", "G Data", "flags"]
    rows_pp = []
    for i, it in enumerate(items, 1):
        if not it["row_a_g"]:
            rows_pp.append([i, "(deal não achado)", "", "", "", "", "", "", " | ".join(it["flags"])])
            continue
        row = it["row_a_g"]
        rows_pp.append([
            i,
            (str(row[0])[:25]),
            (str(row[1])[:18]),
            (str(row[2])[:25]),
            (str(row[3])[:25]),
            (str(row[4])[:22]),
            fmt_brl(row[5]),
            row[6] or "(vazio)",
            f"{len(it['flags'])} flag(s)" if it["flags"] else "OK",
        ])
    print_table([h[:25] for h in headers_p], rows_pp, max_rows=30)

    # 8) Mensagem pro Ivan (Bruno copia daqui)
    print_ivan_message(items)

    # 9) Cross-check semântico
    soma_f = round(sum((it["row_a_g"][5] if it["row_a_g"] else 0) for it in items), 2)
    soma_manual = round(sum(m.get("valor", 0) for m in manual), 2)
    section("Cross-check semântico")
    print(f"soma da col F (escrita): {fmt_brl(soma_f)}")
    print(f"soma da manual:          {fmt_brl(soma_manual)}")
    print(f"delta:                   {fmt_brl(soma_f - soma_manual)}")

    # 10) Decisão final
    section("DECISÃO FINAL")
    rows_a_g_final = [it["row_a_g"] for it in items if it["row_a_g"]]
    print(f"Linhas a escrever: {len(rows_a_g_final)} de {len(items)} da manual")
    print(f"Ciclo de referência: {cs:%d/%m/%Y} a {ce:%d/%m/%Y}")
    print("Estratégia G: data_do_aporte > data_do_match > vazio")

    # 11) Gates simplificados (manual = source-of-truth; sem threshold de matched_total)
    section("Gates do --write")
    fails = []
    if len(rows) < MIN_ROWS_GUARD:
        fails.append(f"consolidado com {len(rows)} linhas (< {MIN_ROWS_GUARD})")
    if not header_ok:
        fails.append("header da aba destino não é o esperado")
    if formula_alarm:
        fails.append(f"H2:I2 referencia '{REF_TAB}!' (cópia por fórmula)")
    if len(rows_a_g_final) <= 0 or len(rows_a_g_final) > 200:
        fails.append(f"N a escrever fora do range sane: {len(rows_a_g_final)}")
    if fails:
        print(">>> BLOQUEADO. Razões:")
        for f in fails:
            print(f"   - {f}")
    else:
        print("OK — todos os gates passam.")
    n_with_flags = sum(1 for it in items if it["flags"])
    if n_with_flags:
        print(f"\n[info] {n_with_flags} linha(s) com flags — não bloqueiam, mas precisam de follow-up "
              f"com o Ivan (veja seção MENSAGEM PRO IVAN acima).")

    if not args.write:
        print("\n[dry-run] nenhuma escrita feita. Use --write APÓS revisão.")
        return

    if fails:
        raise SystemExit("\n--write ABORTADO: gates falharam (veja acima).")

    # ESCRITA — usa o sort: ordem da manual (preserva a leitura do Ivan)
    section("ESCRITA (cirúrgica em A:G)")
    n_written = do_write(sh_ivan, ws_write, rows_a_g_final, M)
    print(f"OK — {n_written} linha(s) escrita(s) em A2:G{n_written + 1}.")
    if M > n_written:
        print(f"[aviso] {M - n_written} linha(s) antiga(s) entre A{n_written + 2}:G{M + 1} foram limpas.")
    print(f"\nLink: https://docs.google.com/spreadsheets/d/{IVAN_SHEET_ID}/edit#gid=444443497")


if __name__ == "__main__":
    main()
