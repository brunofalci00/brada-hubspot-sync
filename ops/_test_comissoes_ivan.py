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


def test_contrato_consolidado_aceita_os_dois_formatos():
    """Producao tem 37 colunas; com o deploy do sync.py novo passa a ter 41.
    Os dois valem. Qualquer outra coisa tem que abortar."""
    obrig = reporting.CONSOLIDADO_HEADER
    fin = reporting.CONSOLIDADO_HEADER_FINANCEIRO
    assert fin == list(common.FINANCE_FIELDS)
    assert not set(obrig) & set(fin), "financeiras nao podem estar no bloco obrigatorio"
    assert reporting.validar_header_consolidado(list(obrig)) == []
    assert reporting.validar_header_consolidado(list(obrig) + fin) == fin
    for ruim, porque in [
        (list(obrig)[:-1], "faltando a ultima obrigatoria"),
        (list(obrig) + fin[:2], "cauda financeira pela metade"),
        (list(obrig) + ["coluna_nova"], "coluna desconhecida no fim"),
        (list(reversed(obrig)), "ordem trocada"),
    ]:
        try:
            reporting.validar_header_consolidado(ruim)
        except SystemExit:
            pass
        else:
            raise AssertionError("deveria abortar: " + porque)


def test_lacuna_bloqueante_x_preenchivel():
    """As 4 properties financeiras estao com zero preenchimento no HubSpot. Se
    elas travassem a escrita, a aba da Bia ficaria vazia por tempo indeterminado
    — foi o que segurou o rollout de 06/08. Identidade e valor seguem travando."""
    sem_financeiras = deal(numero_contrato_financeiro="", documento_cobranca="",
                           condicoes_pagamento_financeiro="", numero_parcelas_financeiro="")
    assert common.blocking_gaps(sem_financeiras) == []
    assert common.completeness_gaps(sem_financeiras), "as lacunas ainda tem que ser reportadas"
    # CNPJ, lei e contato NAO travam: faltam em muitos deals e nao participam da
    # reconciliacao. Entram como lacuna reportada, e a celula se preenche no run
    # seguinte quando alguem digitar no HubSpot.
    assert common.blocking_gaps(deal(cnpj="")) == []
    assert "empresa_associada/cnpj" in common.completeness_gaps(deal(cnpj=""))
    assert common.blocking_gaps(deal(lei_principal="")) == []
    # As chaves de reconciliacao travam: sem elas o run seguinte duplicaria a linha.
    assert "valor" in common.blocking_gaps(deal(valor_bruto="0"))
    assert "closedate" in common.blocking_gaps(deal(closedate=""))
    assert "numero_do_projeto" in common.blocking_gaps(deal(numero_projeto=""))
    assert "nome_do_projeto" in common.blocking_gaps(deal(nome_projeto=""))
    assert "cliente/empresa associada" in common.blocking_gaps(deal(cliente=""))
    assert common.blocking_gaps(deal()) == []
    # o conjunto bloqueante e exatamente o das chaves de reconciliacao
    assert set(common.CAMPOS_BLOQUEANTES) == {
        "cliente/empresa associada", "numero_do_projeto", "nome_do_projeto",
        "closedate", "valor"}


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


def test_only_fill_blanks_preserva_o_que_foi_digitado():
    """Aba mantida a mao: automacao preenche vazio, nunca corrige o humano.
    Caso real da aba da Bia em 19/08 — o nome que ela digitou era mais
    especifico que o nome do projeto no HubSpot."""
    planilha = ["Craques do Amanhã", "", "(21) 98836-8628"]
    hubspot = ["FIM DE ANO FELIZ", "CT-9", "+5521988368628"]
    idx = [0, 1, 2]
    assert common.changed_cells(planilha, hubspot, idx, only_fill_blanks=True) == [(1, "", "CT-9")]
    # sem a flag, sobrescreveria os tres
    assert len(common.changed_cells(planilha, hubspot, idx)) == 3
    # o que nao foi sobrescrito nao some: vira relatorio
    div = common.divergent_cells(planilha, hubspot, idx)
    assert [d[0] for d in div] == [0, 2]
    # celula vazia de um dos lados nao e divergencia, e lacuna
    assert common.divergent_cells(["", "x"], ["a", ""], [0, 1]) == []


def test_vendas_layout_formula_and_manuals():
    row = vendas.build_row(deal(), row_number=43)
    assert len(row) == 32 and row[0] == "Empresa" and row[10] == 1500.5
    assert row[14] == "30/60" and row[15] == "Externo" and row[31] == "D1"
    assert row[16].startswith('=IF(P43="Externo";') and row[19].endswith(';R43*4%;0)')
    for idx in [8, 9, 12, 13, 20, 21, 22, 23, 24, 25]:
        assert row[idx] == "", f"manual idx {idx} alterado"


def test_bia_layout_and_manuals():
    """Layout real de 21 colunas conferido na aba viva em 19/08: o time inseriu
    CNPJ e Segmento Cultural no inicio, deslocando tudo duas casas."""
    assert bia.HEADER[:3] == ["Razão Social", "CNPJ", "Segmento Cultural"]
    assert len(bia.HEADER) == 21
    row = bia.build_row(deal())
    assert len(row) == 29
    assert row[0] == "Empresa" and row[1] == "1"          # A Razao Social, B CNPJ
    assert row[4] == "CT-1" and row[5] == "Nota Fiscal"   # E Contrato, F RECIBO/NOTA
    assert row[6] == "30/60" and row[8] == "Externo"      # G CONDICOES, I Interno/Externo
    assert row[11] == 1500.5 and row[13] == 2            # L Valor, N PARCELAS
    assert row[16] == "Contato" and row[18] == "c@x.com"  # Q contato, S e-mail
    assert row[28] == "D1"                                # AC deal_id
    for idx in bia.MANUAIS_DA_BIA:
        assert row[idx] == "", f"coluna da Bia (idx {idx}) foi sobrescrita"


def test_reparo_de_identificador_virado_numero():
    """CNPJ, contrato e numero de projeto sao identificadores. Guardados como
    numero pelo Sheets, perdem o zero a esquerda — aconteceu de verdade na carga
    de 19/08 com 08316498000108 e 01137526000180."""
    planilha = [8316498000108, "X", 26989067000194]
    hubspot = ["08316498000108", "Y", "26989067000194"]
    rep = common.numeric_render_repairs(planilha, hubspot, [0, 2])
    assert rep == [(0, 8316498000108, "08316498000108")]
    # conteudo realmente diferente NAO e reparo, e divergencia
    assert common.numeric_render_repairs([123], ["456"], [0]) == []
    # celula vazia de um lado nao e reparo, e lacuna
    assert common.numeric_render_repairs(["", 1], ["1", ""], [0, 1]) == []
    # numero sem perda de zero nao precisa de reparo, mesmo com espaco sobrando
    # no valor do HubSpot (varios numeros de projeto vem assim)
    assert common.numeric_render_repairs([2317455], ["2317455    "], [0]) == []
    # celula que ja e texto nunca e reparada
    assert common.numeric_render_repairs(["08316498000108"], ["8316498000108"], [0]) == []


def test_freshness():
    now = dt.datetime(2026, 8, 6, 18, 0, tzinfo=dt.timezone.utc)
    # marca explicita de fuso manda
    assert 59 < common.assert_fresh_source("2026-08-06 14:00:00 BRT", now=now) < 61
    # 'DD/MM/YYYY HH:MM' e o que o sync.py grava, com a hora do runner (UTC)
    assert 59 < common.assert_fresh_source("06/08/2026 17:00 831", now=now) < 61
    # o mesmo carimbo lido como BRT daria 60 min e passaria; como UTC da 240 e aborta
    try:
        common.assert_fresh_source("06/08/2026 14:00 831", now=now)
    except SystemExit as exc:
        assert "stale" in str(exc)
    else:
        raise AssertionError("240 min deveria abortar (era o bug de 3h)")
    try:
        common.assert_fresh_source("2026-08-06 12:00:00 BRT", now=now)
    except SystemExit as exc:
        assert "stale" in str(exc)
    else:
        raise AssertionError("fonte stale deveria abortar")
    # carimbo no futuro nao e "stale": e fuso ou relogio errado, e tem mensagem propria
    try:
        common.assert_fresh_source("06/08/2026 21:00 831", now=now)
    except SystemExit as exc:
        assert "FUTURO" in str(exc)
    else:
        raise AssertionError("carimbo no futuro deveria abortar")


if __name__ == "__main__":
    tests = [(name, fn) for name, fn in sorted(globals().items()) if name.startswith("test_")]
    for name, fn in tests:
        fn()
        print("PASS", name)
    print(f"OK — {len(tests)} testes")
