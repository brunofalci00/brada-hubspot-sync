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


# Nem toda lacuna pesa igual, e o criterio nao e "quao importante e o campo":
# e se a AUSENCIA dele quebra a reconciliacao. As linhas legadas nao tem
# deal_id, entao o unico jeito de reencontra-las no run seguinte e por
# cliente + projeto + numero + valor + data (ver SCHEMA de cada automacao).
# Faltando uma dessas, o proximo run nao reconhece a linha e a DUPLICA. Por isso
# essas travam a escrita: escrever seria pior que nao escrever.
#
# Todo o resto e lacuna de PREENCHIMENTO: o negocio existe, alguem ainda nao
# digitou. Como a escrita e upsert por deal_id, a celula se preenche sozinha no
# run seguinte, sem retrabalho e sem duplicar nada. Entram aqui o CNPJ, a lei, o
# contato do proponente e as 4 properties financeiras.
#
# A distincao importa porque as 4 financeiras estao hoje com ZERO preenchimento
# e o CNPJ falta em 19 dos 57 MATCH ganhos. Tratar tudo como bloqueante deixaria
# a aba da Bia vazia por tempo indeterminado — foi exatamente o que travou o
# rollout de 06/08.
CAMPOS_BLOQUEANTES = (
    "cliente/empresa associada", "numero_do_projeto", "nome_do_projeto",
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
    """Idade em minutos do carimbo `ultima_sync_deals`.

    Formatos aceitos: ISO com offset; 'YYYY-MM-DD HH:MM:SS [BRT/UTC]' com marca
    explicita; e 'DD/MM/YYYY HH:MM', que e o que o sync.py grava de fato.

    Sobre o fuso do formato brasileiro: `sync.py` usa
    `datetime.datetime.now().strftime("%d/%m/%Y %H:%M")`, hora LOCAL do runner e
    sem marca de fuso. O sync roda no GitHub Actions, cujo runner esta em UTC —
    entao esse carimbo e UTC, nao BRT.

    Isso foi medido em 19/08/2026: o carimbo dizia 12:50 com a hora local em
    10:39, e o guard calculou idade de -131 min e abortou uma escrita com fonte
    de 49 minutos. O erro era de exatamente 3 horas, e no sentido PERIGOSO: como
    a idade saia 180 min menor que a real, uma fonte parada ha ate 4h30 passava
    como "menos de 90 minutos".

    Ler o formato brasileiro como UTC acerta o caso real (CI). Se alguem rodar o
    sync na maquina, em BRT, a idade sai 180 min MAIOR que a real e o guard
    aborta — falha para o lado seguro, que e o que se quer num guard de folha.
    """
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
            # UTC: e o fuso do runner que grava esse carimbo. Ver docstring.
            tz_br = dt.timezone.utc
            if "BRT" in raw.upper() or "-03" in raw:
                tz_br = dt.timezone(dt.timedelta(hours=-3))
            parsed = dt.datetime.strptime(
                "/".join(match_br.group(i) for i in (1, 2, 3)) + " " + match_br.group(4), fmt
            ).replace(tzinfo=tz_br)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone(dt.timedelta(hours=-3)))
    return (now - parsed.astimezone(dt.timezone.utc)).total_seconds() / 60


def assert_fresh_source(source_timestamp, max_minutes=90, now=None):
    age = source_age_minutes(source_timestamp, now=now)
    if age is None:
        raise SystemExit("[abort] ultima_sync_deals ausente ou ilegivel; nada escrito.")
    if age < -5:
        raise SystemExit(
            f"[abort] ultima_sync_deals esta {abs(age):.1f} min no FUTURO. Isso nao e fonte "
            "velha, e fuso ou relogio errado — e escrever assim gravaria dado de origem "
            "desconhecida em folha. Confira o carimbo na aba _meta da Brada_Dashboard_Deals."
        )
    if age > max_minutes:
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


# Palavras que nao ajudam a identificar empresa: preposicao e sufixo societario.
# "Acelera Indie Plus LTDA" e "ACELERA INDIE PLUS TREINAMENTOS LTDA" sao a mesma
# coisa; o que decide sao os tokens de conteudo.
_RUIDO_RAZAO_SOCIAL = {
    "de", "da", "do", "das", "dos", "e", "em", "a", "o", "as", "os",
    "ltda", "me", "epp", "eireli", "sa", "s", "cia", "the",
}


def mesma_entidade(a, b):
    """Os dois textos nomeiam a MESMA empresa, so escrita de jeito diferente?

    Existe para nao "corrigir" o que um humano escreveu melhor. O Ivan digita
    "EGP BRASIL", "Encaminhando", "PLUG AND PLUS"; o cadastro tem
    "ESCRITORIO DE GERENCIAMENTO DE PROJETOS DO BRASIL - EGP",
    "Associacao Encaminhando", "PLUG AND PLUS EDUCACAO LTDA". Sobrescrever so deixa
    a planilha mais feia, e em "Escola de Danca Missao Intensidade" -> "Missao
    Intensidade" chega a PERDER informacao.

    O caso que importa e outro: o campo trazer o nome do PROJETO, que e outra
    entidade ("Gauchos GAMES" onde o proponente e a Epopeia). Ai vale sobrescrever.

    Criterio: um contido no outro, ou todos os tokens de conteudo do mais curto
    presentes no mais longo. Substring sozinho nao basta, porque "Acelera Indie
    Plus LTDA" nao e substring de "ACELERA INDIE PLUS TREINAMENTOS LTDA".
    """
    x, y = norm(a), norm(b)
    if not x or not y:
        return False
    if x in y or y in x:
        return True
    tx = {t for t in x.split() if len(t) > 2 and t not in _RUIDO_RAZAO_SOCIAL}
    ty = {t for t in y.split() if len(t) > 2 and t not in _RUIDO_RAZAO_SOCIAL}
    if not tx or not ty:
        return False
    menor, maior = (tx, ty) if len(tx) <= len(ty) else (ty, tx)
    return menor <= maior


def numeric_render_repairs(old, new, text_indices):
    """Celulas de coluna TEXTUAL que o Sheets guardou como NUMERO.

    CNPJ, numero de contrato e numero de projeto sao identificadores, nao
    quantidades. Escritos com USER_ENTERED, o Sheets os interpreta como numero e
    o zero a esquerda evapora: 08316498000108 virou 8316498000108 e
    01137526000180 virou 1137526000180 na carga de 19/08. CNPJ com 13 digitos
    nao serve para emitir nota.

    Devolve so os casos em que os digitos SIGNIFICATIVOS batem, ou seja, dano de
    renderizacao. Se o conteudo for diferente de verdade, nao entra aqui: isso e
    divergencia de dado e vai para divergent_cells, sem sobrescrever nada.
    """
    saida = []
    width = max(len(old), len(new))
    old = list(old) + [""] * (width - len(old))
    new = list(new) + [""] * (width - len(new))
    for idx in text_indices:
        atual, alvo = old[idx], new[idx]
        if str(atual).strip() == "" or str(alvo).strip() == "":
            continue
        if isinstance(atual, str):
            continue                       # ja esta como texto: nada a reparar
        canonico = str(alvo).strip()       # o HubSpot devolve varios com espaco no fim
        if str(atual) == canonico:
            continue                       # guardado como numero, mas sem perda
        a, b = digits(atual).lstrip("0"), digits(canonico).lstrip("0")
        if a and a == b:
            saida.append((idx, atual, canonico))
    return saida


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


def changed_cells(old, new, auto_indices, normalizers=None, only_fill_blanks=False):
    """Celulas a mudar numa linha que ja existe.

    only_fill_blanks=True: escreve APENAS onde a celula esta vazia. Serve para
    aba que alguem mantem a mao. Medido em 19/08 na aba da Bia: sem isso, a
    automacao trocaria "Craque do Amanha Santa Isabel SP" e "Craques do Amanha"
    (duas linhas distintas, digitadas por ela) pelo mesmo "FIM DE ANO FELIZ - 5a
    EDICAO" do HubSpot, e "ICMS Esporte Petropolis" viraria "ICMS Esporte". O
    dado dela era mais especifico que o do CRM. Divergencia assim vira relatorio
    (ver divergent_cells), nao sobrescrita.
    """
    changes = []
    normalizers = normalizers or {}
    width = max(len(old), len(new))
    old = list(old) + [""] * (width - len(old))
    new = list(new) + [""] * (width - len(new))
    for idx in auto_indices:
        # Null HubSpot values never erase an existing cell.
        if new[idx] in (None, "") and old[idx] not in (None, ""):
            continue
        if only_fill_blanks and str(old[idx]).strip() != "":
            continue
        normalize = normalizers.get(idx, lambda value: value)
        if normalize(old[idx]) != normalize(new[idx]):
            changes.append((idx, old[idx], new[idx]))
    return changes


def divergent_cells(old, new, auto_indices, normalizers=None):
    """Celulas preenchidas dos DOIS lados com valores diferentes.

    Com only_fill_blanks essas nao sao tocadas, mas tambem nao podem sumir: cada
    uma e uma pergunta real de dado (qual dos dois esta certo, o CRM ou a
    planilha). Sai no relatorio como [DIVERGE].
    """
    saida = []
    normalizers = normalizers or {}
    width = max(len(old), len(new))
    old = list(old) + [""] * (width - len(old))
    new = list(new) + [""] * (width - len(new))
    for idx in auto_indices:
        if str(old[idx]).strip() == "" or str(new[idx]).strip() == "":
            continue
        normalize = normalizers.get(idx, lambda value: value)
        if normalize(old[idx]) != normalize(new[idx]):
            saida.append((idx, old[idx], new[idx]))
    return saida
