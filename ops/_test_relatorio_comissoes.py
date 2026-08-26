# -*- coding: utf-8 -*-
"""Testes puros do gerador do relatorio de Vendas e Comissoes.

O PDF vai ao financeiro e alimenta apuracao de comissao, entao errar aqui custa dinheiro. Sem
rede: tudo aqui e funcao pura sobre listas de string, no mesmo padrao das outras suites.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gerar_relatorio_comissoes_ivan as rel


def test_cycle_labels_deriva_tudo_do_ano_mes():
    lab = rel.cycle_labels("2026-08")
    assert lab["match_tab"] == "Agosto_MATCH"
    assert lab["elab_tab"] == "Agosto_Elaboração de Projetos"
    assert lab["basename"] == "Relatorio_Vendas_Comissoes_Brada_ciclo_20jul_20ago_2026"


def test_rotulo_do_ciclo_comeca_no_21():
    """A janela do fecho e 21 do mes anterior ate 20 deste.

    Rotular "20/06 a 20/07" e depois "20/07 a 20/08" faz o dia 20/07 aparecer em dois
    relatorios seguidos, o que num documento de apuracao le como periodo sobreposto.
    """
    assert rel.cycle_labels("2026-08")["ciclo"] == "Ciclo de 21/07/2026 a 20/08/2026"
    assert rel.cycle_labels("2026-08")["periodo"] == "21/07 a 20/08"
    # o NOME DO ARQUIVO fica com 20 nos dois lados, pra nao quebrar a serie ja entregue
    assert "20jul_20ago" in rel.cycle_labels("2026-08")["basename"]


def test_cycle_labels_vira_o_ano_em_janeiro():
    lab = rel.cycle_labels("2026-01")
    assert lab["ciclo"] == "Ciclo de 21/12/2025 a 20/01/2026"
    assert lab["basename"].endswith("20dez_20jan_2026")


def test_validar_header_aborta_em_coluna_deslocada():
    """Sem esta trava o relatorio le por posicao e reporta a coluna errada, calado."""
    deslocado = list(rel.ELAB_HEADERS)
    deslocado.insert(2, "Coluna Nova")
    try:
        rel.validar_header(deslocado, rel.ELAB_HEADERS, rel.ELAB_BLANK, "aba")
    except SystemExit as erro:
        assert "Tipo venda" in str(erro)
        return
    raise AssertionError("deveria ter abortado")


def test_validar_header_tolera_espaco_a_direita():
    """A planilha real tem 'Proponente ', 'Valor ', 'Data do aporte '. Literal abortaria."""
    rel.validar_header([h + "  " for h in rel.MATCH_HEADERS],
                       rel.MATCH_HEADERS, rel.MATCH_BLANK, "match")


def test_validar_header_ignora_coluna_de_comissao():
    """As de comissao saem em branco e sao as que mudam de rotulo: o Ivan ja renomeou
    'Ivan' para 'Ivan 5%' em Julho_Elaboracao. Travar onde nao ha leitura e alarme falso."""
    renomeado = list(rel.ELAB_HEADERS)
    renomeado[10], renomeado[11] = "Ivan 5%", "Ricardo "
    rel.validar_header(renomeado, rel.ELAB_HEADERS, rel.ELAB_BLANK, "julho")


def test_validar_header_aborta_em_aba_sem_cabecalho():
    try:
        rel.validar_header([], rel.ELAB_HEADERS, rel.ELAB_BLANK, "vazia")
    except SystemExit:
        return
    raise AssertionError("aba sem cabecalho e aba quebrada, tem que abortar")


def test_valores_em_pt_br():
    assert rel.parse_brl("3.000,00") == 3000.0
    assert rel.parse_brl("R$ 1.234,56") == 1234.56
    assert rel.parse_brl("") == 0.0
    assert rel.parse_brl("nao e numero") == 0.0     # celula suja nao pode derrubar o relatorio
    assert rel.fmt_brl(3000.0) == "3.000,00"
    assert rel.fmt_brl(1234.56) == "1.234,56"


def test_esfera_de_incentivo():
    assert rel.match_esfera("ICMS Esporte") == "ICMS (estadual)"
    assert rel.match_esfera("IR Cultura") == "IR (federal)"
    assert rel.match_esfera("Rouanet") == "Rouanet/Audiovisual (federal)"
    assert rel.match_esfera("PROMAC") == "ISS/PROMAC (municipal)"
    assert rel.match_esfera("Coisa nova") == "Outras fontes"


def test_cobranca_pega_so_externo():
    """A tabela de cobranca existe para o financeiro cobrar; interno nao se cobra."""
    def linha(cliente, ie):
        out = [""] * len(rel.MATCH_HEADERS)
        out[0], out[rel.MATCH_IE_IDX] = cliente, ie
        return out
    saida = rel.cobranca_rows([linha("A", "Externo"), linha("B", "Interno"),
                               linha("C", "externo"), linha("D", "")])
    assert [r[0] for r in saida] == ["A", "C"]
    assert len(saida[0]) == len(rel.COBR_HEADERS)


def test_sumario_conta_a_apurar_pela_condicao_e_nao_pelo_valor():
    """Em julho os captados passaram a vir com Valor 0 em vez de vazio. Contar por celula
    vazia teria dito 'zero a apurar' num ciclo em que 6 dos 7 estavam a apurar."""
    def elab(condicao, valor):
        out = [""] * len(rel.ELAB_HEADERS)
        out[rel.ELAB_COND_IDX], out[rel.ELAB_VALOR_IDX] = condicao, valor
        out[rel.ELAB_LEI_IDX] = "Rouanet"
        return out
    linhas = [elab("10% vr captado", "0,00")] * 6 + [elab("Pix pela plataforma", "3.000,00")]
    resumo = rel.build_summary([], linhas, 0.0, 3000.0, 0, periodo="21/07 a 20/08")
    assert resumo["elab_apurar"] == 6
    assert resumo["elab_fechado_n"] == 1


def test_sumario_sem_match_explica_o_criterio():
    """Ciclo zerado ja foi lido como falha do relatorio. A nota tem que dizer o criterio."""
    resumo = rel.build_summary([], [], 0.0, 0.0, 0, periodo="21/07 a 20/08")
    assert "não houve conversão de vendas de MATCH" in resumo["exec_txt"]
    assert "21/07 a 20/08" in resumo["exec_txt"]
    assert "aporte confirmado" in rel.NOTA_MATCH_VAZIO
    assert "não entra na apuração" in rel.NOTA_MATCH_VAZIO


def test_comissao_sai_em_branco():
    """Contrato com o Ivan desde 06/07: quem apura comissao e o financeiro, nao o PDF."""
    assert rel.MATCH_BLANK == {11, 12, 13, 14, 15}
    assert rel.ELAB_BLANK == {8, 9, 10, 11}
    # e as colunas em branco sao mesmo as de comissao, nao dado do negocio
    for i in rel.MATCH_BLANK:
        assert rel.MATCH_HEADERS[i] in ("Ivan", "Jaqueline", "Carina", "Danielle", "Rafaela")
    for i in rel.ELAB_BLANK:
        assert rel.ELAB_HEADERS[i] in ("OBS", "Líquido pago", "Ivan", "Ricardo")


def test_larguras_e_alinhamentos_batem_com_as_colunas():
    """Desalinhar isso corta coluna no PDF sem erro nenhum."""
    for headers, larguras, aligns in (
            (rel.MATCH_HEADERS, rel.MATCH_WIDTHS, rel.MATCH_ALIGN),
            (rel.ELAB_HEADERS, rel.ELAB_WIDTHS, rel.ELAB_ALIGN),
            (rel.COBR_HEADERS, rel.COBR_WIDTHS, rel.COBR_ALIGN)):
        assert len(larguras) == len(headers)
        assert len(aligns) == len(headers)


def test_navegador_sai_do_env_quando_existe():
    anterior = os.environ.get("CHROME_BIN")
    os.environ["CHROME_BIN"] = __file__          # existe, serve de alvo
    try:
        assert rel.achar_navegador() == __file__
    finally:
        os.environ.pop("CHROME_BIN", None)
        if anterior is not None:
            os.environ["CHROME_BIN"] = anterior


def test_navegador_ignora_env_que_aponta_pra_nada():
    """Env apontando pra caminho inexistente nao pode mascarar o Chrome instalado."""
    anterior = os.environ.get("CHROME_BIN")
    os.environ["CHROME_BIN"] = os.path.join(os.path.dirname(__file__), "nao_existe_mesmo")
    try:
        assert rel.achar_navegador() != os.environ["CHROME_BIN"]
    finally:
        os.environ.pop("CHROME_BIN", None)
        if anterior is not None:
            os.environ["CHROME_BIN"] = anterior


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(name, fn) for name, fn in sorted(globals().items()) if name.startswith("test_")]
    for name, fn in tests:
        fn()
        print("PASS", name)
    print(f"OK — {len(tests)} testes")
