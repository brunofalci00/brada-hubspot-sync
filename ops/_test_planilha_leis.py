# -*- coding: utf-8 -*-
"""Testes puros do roteamento e da montagem de linha da planilha CLIENTES/MATCH/COMISSAO.

O que se protege aqui e uma coisa so: **nunca escrever na aba errada**. Cada aba tem processo
fiscal proprio, entao a linha no lugar errado entra no rito de cobranca errado.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import planilha_leis as pl


def deal(**props):
    base = {"dealname": "Empresa X", "valor_do_aporte": "40000", "percentual_brada": "10",
            "closedate": "2026-08-17T20:11:46.652Z", "tipo_de_proponente": "Externo",
            "nome_do_projeto": "PROJETO", "numero_do_projeto": "SLI-1", "_empresa_associada": "EMPRESA X"}
    base.update(props)
    return {"properties": base}


# ------------------------------------------------------------------ roteamento

def test_nome_do_negocio_decide_as_seis_abas():
    """Casos reais colhidos do CRM em 20/08."""
    assert pl.enquadramento_do_dealname("Galiotto - ICMS RIO - Esporte ") == pl.ICMS_RIO
    assert pl.enquadramento_do_dealname("MATIFIC BRASIL LTDA - ISS SP- Promac ") == pl.ISS_SP
    assert pl.enquadramento_do_dealname("Diffucap ICMS Esporte") == ""      # ICMS sem estado
    assert pl.enquadramento_do_dealname("Asia - Lei do Bem") == pl.LEI_DO_BEM
    assert pl.enquadramento_do_dealname("Quality - ISS RJ") == pl.ISS_RIO
    assert pl.enquadramento_do_dealname("CASA DO ALEMAO IR CULTURA") == ""  # IR nao vem por aqui
    assert pl.enquadramento_do_dealname("") == ""


def test_o_campo_manual_vence_o_nome():
    """Quando o picklist existir, ele encerra a discussao: e escolha explicita de quem sabe."""
    d = deal(dealname="Fulano - ICMS RIO - Esporte")
    aba, conf, _m = pl.rotear_aba(d["properties"], enquadramento_manual="ISS SP")
    assert (aba, conf) == (pl.ISS_SP, "ALTA")


def test_o_nome_vence_a_property_de_imposto():
    """O deal da MATIFIC tem linha_de_imposto_categoria=IR e se chama 'ISS SP'. A Jaqueline
    seguiu o nome, e estava certa: a property e que esta errada nesse card."""
    d = deal(dealname="MATIFIC - ISS SP- Promac", linha_de_imposto_categoria="IR")
    aba, conf, _m = pl.rotear_aba(d["properties"])
    assert (aba, conf) == (pl.ISS_SP, "ALTA")


def test_ir_decide_sozinho_porque_nao_se_divide_por_estado():
    d = deal(dealname="Empresa sem pista", linha_de_imposto_categoria="IR")
    aba, conf, _m = pl.rotear_aba(d["properties"])
    assert (aba, conf) == (pl.IR, "ALTA")


def test_iss_e_icms_sem_estado_vao_para_revisao_e_nao_para_o_chute():
    """Escolher entre RIO e SP no chute manda o negocio para o processo fiscal errado."""
    for imposto, opcoes in (("ICMS", "ICMS RIO"), ("ISS", "ISS RIO")):
        d = deal(dealname="Empresa sem pista", linha_de_imposto_categoria=imposto)
        aba, conf, motivo = pl.rotear_aba(d["properties"])
        assert aba == "" and conf == "MEDIA"
        assert opcoes in motivo and "falta o estado" in motivo


def test_sem_sinal_nenhum_e_orfa():
    aba, conf, _m = pl.rotear_aba(deal(dealname="Empresa X")["properties"])
    assert (aba, conf) == ("", "ORFA")


def test_enquadramento_manual_invalido_nao_vira_aba():
    """Digitar qualquer coisa no campo nao pode criar destino."""
    aba, conf, motivo = pl.rotear_aba(deal()["properties"], enquadramento_manual="ISS BAHIA")
    assert aba == "" and conf == "MEDIA" and "nao e uma das 6 abas" in motivo


# ------------------------------------------------------------------ linha

def test_as_duas_tecnicas_ficam_no_inicio_de_toda_aba():
    """Coluna no comeco viaja junto com qualquer ordenacao que comece em A.

    Em 20/08 a coluna do link ficou parada numa ordenacao alfabetica e 31 de 31 links
    passaram a apontar para o cliente errado. E o que este teste impede de voltar.
    """
    assert (pl.COL_DEAL_ID, pl.COL_LINK, pl.OFFSET) == (0, 1, 2)
    for aba in pl.ABAS:
        assert pl.LAYOUT[aba]["cols"]["patrocinador"] == 0   # dentro do bloco do financeiro
        assert pl.pos(aba, "patrocinador") == 2              # depois das duas tecnicas


def test_lei_do_bem_nao_tem_projeto_nem_proponente():
    """A coluna nao existe la; escrever nela invadiria a coluna do vizinho."""
    cols = pl.LAYOUT[pl.LEI_DO_BEM]["cols"]
    for ausente in ("projeto", "numero", "proponente", "lei"):
        assert ausente not in cols


def test_linha_cai_nas_posicoes_da_aba():
    d = deal(dealname="X - ICMS RIO", nome_do_projeto="CRAQUE DO AMANHA",
             numero_do_projeto="SEI-1438", nome_do_proponente="CENTRO DE ESTUDO",
             lei_principal="Esporte Estadual", _empresa_associada="DIFFUCAP")
    linha = pl.build_row(d, pl.ICMS_RIO, deal_id="64058925378")
    P = lambda k: linha[pl.pos(pl.ICMS_RIO, k)]
    assert P("patrocinador") == "DIFFUCAP"
    assert P("projeto") == "CRAQUE DO AMANHA"
    assert P("numero") == "SEI-1438"
    assert P("proponente") == "CENTRO DE ESTUDO"
    assert P("obs") == "EXTERNO"
    assert P("match") == "17/08/2026"
    # a chave e o link, que sao o que sobrevive a uma reordenacao
    assert linha[pl.COL_DEAL_ID] == "64058925378"
    assert linha[pl.COL_LINK].endswith("/record/0-3/64058925378")


def test_nao_escreve_em_coluna_de_cobranca():
    """Tudo que nao e mapeado fica vazio: e cobranca ou rito fiscal, e e do financeiro."""
    linha = pl.build_row(deal(dealname="X - ICMS RIO"), pl.ICMS_RIO, deal_id="1")
    mapeadas = {pl.pos(pl.ICMS_RIO, k) for k in pl.LAYOUT[pl.ICMS_RIO]["cols"]}
    mapeadas |= {pl.COL_DEAL_ID, pl.COL_LINK}
    for i, celula in enumerate(linha):
        if i not in mapeadas:
            assert celula == "", f"coluna {i} deveria ficar vazia, veio {celula!r}"


def test_comissao_e_percentual_sobre_o_aporte():
    assert pl.valor_da_comissao("40000", "10") == 4000.0
    assert pl.valor_da_comissao("180000", "10") == 18000.0
    # o caso real do ISS RIO: a planilha mostra R$ 3.531,94. Float exige tolerancia.
    assert abs(pl.valor_da_comissao("70638.82", "5") - 3531.941) < 0.001
    assert pl.valor_da_comissao("", "10") is None
    assert pl.valor_da_comissao("40000", None) is None


def test_valor_do_match_nao_se_confunde_com_a_comissao():
    """Sao duas receitas: R$ 700 fixo do patrocinador, e a % do proponente."""
    linha = pl.build_row(deal(dealname="X - ICMS RIO"), pl.ICMS_RIO, valor_match="700")
    assert linha[pl.pos(pl.ICMS_RIO, "valor_match")] == "700"
    assert linha[pl.pos(pl.ICMS_RIO, "valor_comissao")] == "4.000,00"


def test_numeros_e_datas_em_pt_br():
    assert pl._dinheiro("1234.5") == "1.234,50"
    assert pl._dinheiro("450000") == "450.000,00"
    assert pl._dinheiro("nao e numero") == ""
    assert pl._data_br("2026-08-17T20:11:46.652Z") == "17/08/2026"
    assert pl._data_br("") == ""


def test_interno_externo():
    assert pl.interno_externo("Externo") == "EXTERNO"
    assert pl.interno_externo("EGP") == "INTERNO"
    assert pl.interno_externo("") == "INTERNO"


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_")]
    for n, f in tests:
        f()
        print("PASS", n)
    print(f"OK — {len(tests)} testes")
