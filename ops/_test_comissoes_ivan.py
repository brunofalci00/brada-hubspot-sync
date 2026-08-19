# -*- coding: utf-8 -*-
"""Testes puros das automacoes Controle de Vendas + BIA."""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import financeiro_match_common as common
import sheets_comissoes_ivan as vendas
import sheets_cobrancas_bia as bia
import sheets_reporting_financeiro_mensal as reporting
import sync


def deal(deal_id="D1", **overrides):
    base = {
        "deal_id": deal_id, "cliente": "Empresa", "cnpj": "1", "pipeline": "Incentivador",
        "produto": "Match", "stage": "Ganho - Incentivador", "won_ganho": "1",
        "lei_principal": "Rouanet", "numero_projeto": "2400704", "nome_projeto": "Projeto",
        "proponente": "Proponente", "valor_bruto": "1500,50", "closedate": "2026-08-10T00:00:00Z",
        "tipo_de_proponente": "Externo", "nome_contato_proponente": "Contato",
        "email_proponente": "c@x.com", "telefone_proponente": "119",
        "numero_contrato_financeiro": "CT-1", "documento_cobranca": "nota_fiscal",
        "condicoes_pagamento_financeiro": "30/60", "numero_parcelas_financeiro": "2",
    }
    base.update(overrides)
    return base


def record(row, values, deal_id=""):
    return {"row_number": row, "cells": values, "deal_id": deal_id}


def test_schema_append_only():
    assert reporting.CONSOLIDADO_HEADER[-4:] == list(common.FINANCE_FIELDS)
    for field in common.FINANCE_FIELDS:
        assert field in sync.DEAL_PROPERTIES


def test_scope_and_cycle():
    rows = [deal("A"), deal("B", produto="CRIAPE"), deal("C", stage="Proponente"), deal("D", won_ganho="0")]
    assert [d["deal_id"] for d in common.select_match_won(rows)] == ["A"]
    assert [d["deal_id"] for d in common.select_cycle(rows[:1], "2026-08")] == ["A"]
    assert common.select_cycle([deal("X", closedate="2026-07-20")], "2026-08") == []
    assert common.select_cycle([deal("X", closedate="2026-07-21")], "2026-08")


def test_completeness_and_installments():
    assert common.completeness_gaps(deal()) == []
    gaps = common.completeness_gaps(deal(documento_cobranca="", numero_parcelas_financeiro="1.5"))
    assert "documento_cobranca" in gaps
    assert "numero_parcelas_financeiro(inteiro>=1)" in gaps
    assert common.integer_at_least_one("1") == 1
    assert common.integer_at_least_one("0") is None
    assert "empresa_associada/cnpj" in common.completeness_gaps(deal(cnpj=""))
    assert common.sheet_date(46188) == "2026-06-15"
    assert common.text_id(123.0) == "123"


def test_reconcile_order_and_ambiguity():
    deals = [deal("D1", numero_projeto="111", valor_bruto="100"), deal("D2", numero_projeto="222", valor_bruto="200")]
    cells = [""] * 32
    cells[7], cells[10] = "222", 200
    matches, ambiguous, unmatched = common.reconcile([record(2, cells)], deals, vendas.SCHEMA)
    assert matches[0]["deal"]["deal_id"] == "D2" and matches[0]["level"] == 2
    assert not ambiguous and [d["deal_id"] for d in unmatched] == ["D1"]

    twins = [deal("A", numero_projeto="333", valor_bruto="50"), deal("B", numero_projeto="333", valor_bruto="50")]
    cells[7], cells[10] = "333", 50
    matches, ambiguous, _ = common.reconcile([record(3, cells)], twins, vendas.SCHEMA)
    assert not matches and ambiguous


def test_null_never_erases():
    old, new = ["mantem"], [""]
    assert common.changed_cells(old, new, [0]) == []
    assert common.changed_cells(old, ["novo"], [0]) == [(0, "mantem", "novo")]


def test_vendas_layout_formula_and_manuals():
    row = vendas.build_row(deal(), row_number=43)
    assert len(row) == 32 and row[0] == "Empresa" and row[10] == 1500.5
    assert row[14] == "30/60" and row[15] == "Externo" and row[31] == "D1"
    assert row[16].startswith('=IF(P43="Externo";') and row[19].endswith(';R43*4%;0)')
    for idx in [8, 9, 12, 13, 20, 21, 22, 23, 24, 25]:
        assert row[idx] == "", f"manual idx {idx} alterado"


def test_bia_layout_and_manuals():
    row = bia.build_row(deal())
    assert len(row) == 29 and row[2] == "CT-1" and row[3] == "Nota Fiscal"
    assert row[4] == "30/60" and row[11] == 2 and row[28] == "D1"
    for idx in [10, 13, 17, 18]:
        assert row[idx] == "", f"manual idx {idx} alterado"


def test_freshness():
    now = dt.datetime(2026, 8, 6, 18, 0, tzinfo=dt.timezone.utc)
    assert 59 < common.assert_fresh_source("2026-08-06 14:00:00 BRT", now=now) < 61
    assert 59 < common.assert_fresh_source("06/08/2026 14:00 831", now=now) < 61
    try:
        common.assert_fresh_source("2026-08-06 12:00:00 BRT", now=now)
    except SystemExit as exc:
        assert "stale" in str(exc)
    else:
        raise AssertionError("fonte stale deveria abortar")


if __name__ == "__main__":
    tests = [(name, fn) for name, fn in sorted(globals().items()) if name.startswith("test_")]
    for name, fn in tests:
        fn()
        print("PASS", name)
    print(f"OK — {len(tests)} testes")
