# -*- coding: utf-8 -*-
"""Nucleo compartilhado das automacoes financeiras MATCH.

Funcoes puras: selecao, ciclo 21-20, normalizacao, completude e reconciliacao.
Nenhuma funcao deste modulo le nem escreve em HubSpot ou Google Sheets — e o que
mantem a suite de testes offline. Leitura de HubSpot vive em hubspot_financeiro.
"""
from __future__ import annotations

import datetime as dt
import re
import unicodedata
from collections import defaultdict

from sheets_reporting_financeiro_mensal import parse_brl, parse_closedate, cycle_window

PORTAL_ID = "50771078"
FINANCE_FIELDS = (
    "numero_contrato_financeiro",
    "documento_cobranca",
    "condicoes_pagamento_financeiro",
    "numero_parcelas_financeiro",
)


def norm(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(c for c in text if not unicodedata.combining(c)).lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def digits(value):
    return re.sub(r"\D", "", str(value or ""))


def money(value):
    parsed = parse_brl(value)
    return round(parsed, 2) if parsed is not None else None


def integer_at_least_one(value):
    try:
        number = float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None
    return int(number) if number >= 1 and number.is_integer() else None


def document_label(value):
    key = norm(value)
    return {"recibo": "Recibo", "nota fiscal": "Nota Fiscal", "nota_fiscal": "Nota Fiscal"}.get(key, str(value or "").strip())


def interno_externo(row):
    return "Externo" if norm(row.get("tipo_de_proponente")) == "externo" else "Interno"


def deal_link(row):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/deal/{str(row.get('deal_id', '')).strip()}"


def is_match_won(row):
    return (
        norm(row.get("pipeline")) == "incentivador"
        and norm(row.get("produto")) == "match"
        and norm(row.get("stage")) == "ganho incentivador"
        and str(row.get("won_ganho", "")) == "1"
        and (money(row.get("valor_bruto")) or 0) > 0
    )


def select_match_won(rows):
    return [dict(row) for row in rows if is_match_won(row)]


def select_cycle(rows, cycle, all_pending=False):
    start, end = cycle_window(cycle)
    selected = []
    for row in rows:
        date = parse_closedate(row.get("closedate", ""))
        if all_pending or (date and start <= date <= end):
            copy = dict(row)
            copy["_date"] = date
            selected.append(copy)
    return selected


# Nem toda lacuna pesa igual. As de IDENTIDADE e VALOR sao bloqueantes: sem
# cliente, projeto, valor ou data a linha nao identifica cobranca nenhuma e
# escreve-la seria pior que nao escrever. As demais (contato e os 4 campos
# financeiros criados em 06/08) sao lacunas de PREENCHIMENTO: o negocio existe,
# alguem ainda nao digitou. Como a escrita e upsert por deal_id, a celula se
# preenche sozinha no run seguinte, sem retrabalho.
#
# A distincao existe porque as 4 properties financeiras estao hoje com ZERO
# preenchimento. Tratar tudo como bloqueante deixaria a aba da Bia vazia por
# tempo indeterminado, que foi exatamente o que travou o rollout de 06/08.
CAMPOS_BLOQUEANTES = (
    "cliente/empresa associada", "lei_principal", "numero_do_projeto",
    "nome_do_projeto", "nome_do_proponente", "empresa_associada/cnpj",
    "closedate", "valor",
)


def blocking_gaps(row):
    """So as lacunas que impedem a linha de existir. Ver CAMPOS_BLOQUEANTES."""
    return [g for g in completeness_gaps(row) if g in CAMPOS_BLOQUEANTES]


def completeness_gaps(row):
    """TODAS as lacunas, bloqueantes e de preenchimento. Use blocking_gaps para
    decidir escrita; use esta para o relatorio de completude e as pendencias."""
    gaps = []
    required_text = {
        "cliente/empresa associada": row.get("cliente"),
        "lei_principal": row.get("lei_principal"),
        "numero_do_projeto": row.get("numero_projeto"),
        "nome_do_projeto": row.get("nome_projeto"),
        "nome_do_proponente": row.get("proponente"),
        "nome_contato_proponente": row.get("nome_contato_proponente"),
        "email_proponente": row.get("email_proponente"),
        "telefone_proponente": row.get("telefone_proponente"),
        "numero_contrato_financeiro": row.get("numero_contrato_financeiro"),
        "documento_cobranca": row.get("documento_cobranca"),
        "condicoes_pagamento_financeiro": row.get("condicoes_pagamento_financeiro"),
    }
    for name, value in required_text.items():
        if not str(value or "").strip() or (name == "lei_principal" and norm(value) == "sem lei preenchida"):
            gaps.append(name)
    if not str(row.get("cnpj") or "").strip():
        gaps.append("empresa_associada/cnpj")
    if not parse_closedate(row.get("closedate", "")):
        gaps.append("closedate")
    if not money(row.get("valor_bruto")) or money(row.get("valor_bruto")) <= 0:
        gaps.append("valor")
    if integer_at_least_one(row.get("numero_parcelas_financeiro")) is None:
        gaps.append("numero_parcelas_financeiro(inteiro>=1)")
    if norm(row.get("documento_cobranca")) not in {"recibo", "nota fiscal", "nota_fiscal"}:
        if "documento_cobranca" not in gaps:
            gaps.append("documento_cobranca(Recibo/Nota Fiscal)")
    return gaps


def source_age_minutes(source_timestamp, now=None):
    """Aceita ISO ou 'YYYY-MM-DD HH:MM:SS [BRT/UTC]' e retorna idade em minutos."""
    raw = str(source_timestamp or "").strip()
    if not raw:
        return None
    now = now or dt.datetime.now(dt.timezone.utc)
    parsed = None
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        match = re.search(r"(20\d\d-\d\d-\d\d)[ T](\d\d:\d\d(?::\d\d)?)", raw)
        if match:
            parsed = dt.datetime.fromisoformat(f"{match.group(1)}T{match.group(2)}")
            tz = dt.timezone.utc if "UTC" in raw.upper() else dt.timezone(dt.timedelta(hours=-3))
            parsed = parsed.replace(tzinfo=tz)
    if parsed is None:
        match_br = re.search(r"(\d\d)/(\d\d)/(20\d\d)\s+(\d\d:\d\d(?::\d\d)?)", raw)
        if match_br:
            fmt = "%d/%m/%Y %H:%M" if len(match_br.group(4)) == 5 else "%d/%m/%Y %H:%M:%S"
            parsed = dt.datetime.strptime(
                "/".join(match_br.group(i) for i in (1, 2, 3)) + " " + match_br.group(4), fmt
            ).replace(tzinfo=dt.timezone(dt.timedelta(hours=-3)))
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone(dt.timedelta(hours=-3)))
    return (now - parsed.astimezone(dt.timezone.utc)).total_seconds() / 60


def assert_fresh_source(source_timestamp, max_minutes=90, now=None):
    age = source_age_minutes(source_timestamp, now=now)
    if age is None:
        raise SystemExit("[abort] ultima_sync_deals ausente ou ilegivel; nada escrito.")
    if age < -5 or age > max_minutes:
        raise SystemExit(f"[abort] consolidado stale: ultima_sync_deals ha {age:.1f} min (limite {max_minutes}); nada escrito.")
    return age


def _row_numbers(record, number_key):
    raw = record.get(number_key, "")
    values = {digits(raw)} if digits(raw) else set()
    for key in ("projeto_key", "nome_projeto"):
        d = digits(record.get(key, ""))
        if d:
            values.add(d)
    return values


def reconcile(existing, deals, schema):
    """Reconciliacao conservadora e deterministica.

    existing: [{row_number, cells, deal_id}]
    schema: indices cliente, projeto, numero, valor, data e tech.
    Retorna matches, ambiguous e unmatched sem reutilizar deal.
    """
    available = {str(d.get("deal_id", "")): d for d in deals if str(d.get("deal_id", ""))}
    by_id = dict(available)
    matches, ambiguous = [], []
    ambiguous_ids = set()

    def candidate_set(record, level):
        value = money(record["cells"][schema["valor"]])
        number = digits(record["cells"][schema["numero"]])
        client = norm(record["cells"][schema["cliente"]])
        project = norm(record["cells"][schema["projeto"]])
        date = parse_closedate(str(record["cells"][schema["data"]]))
        pool = list(available.values())
        if level == 1:
            did = str(record.get("deal_id", "")).strip()
            return [by_id[did]] if did in available else []
        if level == 2 and number and value is not None:
            return [d for d in pool if digits(d.get("numero_projeto")) == number and money(d.get("valor_bruto")) == value]
        if level == 3 and number and value is not None:
            return [d for d in pool if number in _row_numbers(d, "numero_projeto") and money(d.get("valor_bruto")) == value]
        if level == 4 and value is not None and (client or project):
            return [d for d in pool if money(d.get("valor_bruto")) == value and
                    ((client and norm(d.get("cliente")) == client) or (project and norm(d.get("nome_projeto")) == project))]
        if level == 5 and date:
            prior = candidate_set(record, 4)
            return [d for d in prior if parse_closedate(d.get("closedate", "")) == date]
        return []

    for record in existing:
        chosen, used_level, last_multi = None, None, []
        for level in range(1, 6):
            candidates = candidate_set(record, level)
            if len(candidates) == 1:
                chosen, used_level = candidates[0], level
                break
            if len(candidates) > 1:
                last_multi = candidates
        if chosen:
            did = str(chosen["deal_id"])
            available.pop(did, None)
            matches.append({"row": record, "deal": chosen, "level": used_level})
        elif last_multi:
            ambiguous.append({"row": record, "candidates": last_multi})
            ambiguous_ids.update(str(d["deal_id"]) for d in last_multi)
    unmatched = [d for did, d in available.items() if did not in ambiguous_ids]
    return matches, ambiguous, unmatched


def text_id(value):
    if value in (None, ""):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def sheet_date(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return (dt.date(1899, 12, 30) + dt.timedelta(days=int(value))).isoformat()
    raw = str(value).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(raw[:10], fmt).date().isoformat()
        except ValueError:
            pass
    parsed = parse_closedate(raw)
    return parsed.isoformat() if parsed else raw


def changed_cells(old, new, auto_indices, normalizers=None):
    changes = []
    normalizers = normalizers or {}
    width = max(len(old), len(new))
    old = list(old) + [""] * (width - len(old))
    new = list(new) + [""] * (width - len(new))
    for idx in auto_indices:
        # Null HubSpot values never erase an existing cell.
        if new[idx] in (None, "") and old[idx] not in (None, ""):
            continue
        normalize = normalizers.get(idx, lambda value: value)
        if normalize(old[idx]) != normalize(new[idx]):
            changes.append((idx, old[idx], new[idx]))
    return changes
