# -*- coding: utf-8 -*-
"""Alimenta a planilha CLIENTES/MATCH/COMISSAO com os negocios que fecharem daqui pra frente.

A planilha tem 6 abas, uma por enquadramento fiscal, porque cada lei tem processo proprio de
repasse e cobranca. A automacao escreve SO os dados que nascem no fechamento da venda; cobranca,
baixa e conciliacao seguem manuais, por decisao do Bruno em 20/08.

O historico NAO e migrado: a Jaqueline sobe a mao o que ja fechou. Este script cuida do que vier.

O nucleo do problema e ROTEAR: em qual das 6 abas a linha entra. O HubSpot nao responde isso
direto, entao ha uma cascata de sinais, do mais forte pro mais fraco, e o que nao decide vai pra
revisao humana em vez de chutar. Escrever na aba errada e pior do que nao escrever.
"""
import re

# --------------------------------------------------------------------------- abas

# Os 6 destinos. O valor e o nome exato da aba no Google Sheets (com o espaco final de
# 'LEI DO BEM ', que existe de verdade e ja quebrou busca antes).
IR = "IR"
ISS_RIO = "ISS RIO"
ISS_SP = "ISS SP"
ICMS_RIO = "ICMS RIO"
ICMS_SP = "ICMS SP"
LEI_DO_BEM = "LEI DO BEM "

ABAS = (IR, ISS_RIO, ISS_SP, ICMS_RIO, ICMS_SP, LEI_DO_BEM)

# Layout de cada aba: linha do cabecalho e a posicao (0-based) de cada campo canonico.
# Sao 6 mapas porque as abas divergem de verdade, e nao so nas colunas especificas da lei:
# o cabecalho esta na linha 2 em IR e ISS RIO e na linha 1 nas outras; 'PROJETO' e a coluna 3
# em IR e a 2 em ISS RIO; e ha um typo ('CONTRAO' em ISS RIO). Enquanto o financeiro nao
# padronizar, o mapa e declarativo aqui — tabela, nao codigo repetido.
LAYOUT = {
    IR: {"linha_cabecalho": 2, "cols": {
        "patrocinador": 0, "lei": 1, "projeto": 2, "obs": 3, "numero": 4, "contrato": 5,
        "match": 6, "valor_match": 7, "status": 8, "t_aporte": 9, "fp": 10,
        "proponente": 19, "nome": 20, "email": 21, "contato": 22,
        "pct": 23, "valor_comissao": 24}},
    ISS_RIO: {"linha_cabecalho": 2, "cols": {
        "patrocinador": 0, "projeto": 1, "obs": 2, "numero": 3, "contrato": 4,
        "match": 5, "valor_match": 7, "status": 8, "t_aporte": 9,
        "pct": 11, "valor_comissao": 12,
        "proponente": 14, "nome": 15, "email": 16, "contato": 17}},
    ISS_SP: {"linha_cabecalho": 1, "cols": {
        "patrocinador": 0, "projeto": 1, "obs": 2, "numero": 3, "contrato": 4,
        "match": 5, "valor_match": 7, "status": 8, "t_aporte": 9, "fp": 11,
        "proponente": 24, "nome": 25, "email": 26, "contato": 27,
        "pct": 28, "valor_comissao": 29}},
    ICMS_RIO: {"linha_cabecalho": 1, "cols": {
        "patrocinador": 0, "lei": 1, "projeto": 2, "obs": 3, "numero": 4, "contrato": 5,
        "match": 6, "valor_match": 8, "status": 9, "t_aporte": 10, "fp": 12,
        "proponente": 21, "nome": 22, "email": 23, "contato": 24,
        "pct": 25, "valor_comissao": 26}},
    ICMS_SP: {"linha_cabecalho": 1, "cols": {
        "patrocinador": 0, "lei": 1, "projeto": 2, "obs": 3, "numero": 4, "contrato": 5,
        "match": 6, "valor_match": 7, "status": 8, "t_aporte": 9, "fp": 10,
        "proponente": 19, "nome": 20, "email": 21, "contato": 22,
        "pct": 23, "valor_comissao": 24}},
    # Lei do Bem nao tem PROJETO, N° nem PROPONENTE: a coluna simplesmente nao existe la.
    LEI_DO_BEM: {"linha_cabecalho": 1, "cols": {
        "patrocinador": 0, "contrato": 1, "match": 3, "valor_match": 4, "status": 5,
        "t_aporte": 6, "fp": 7, "nome": 10, "email": 11, "contato": 12,
        "pct": 13, "valor_comissao": 14}},
}

# As duas colunas tecnicas ficam no INICIO de cada aba, abertas por
# ops/preparar_abas_planilha_leis.py em 20/08:
#
#   A = deal_id       (oculta)  a chave que liga a linha ao negocio
#   B = Link HubSpot  (visivel) o financeiro abre o negocio e ve recibo e anexo
#
# No inicio, e nao no fim, porque coluna no comeco viaja junto com qualquer selecao que
# comece em A. Em 20/08 a coluna de link do Controle de Vendas ficou parada numa ordenacao
# alfabetica e 31 de 31 links passaram a apontar para o cliente errado.
#
# O LAYOUT acima descreve as posicoes DENTRO do bloco do financeiro; o deslocamento fica
# aqui, num lugar so, para que abrir outra coluna tecnica um dia mude uma constante.
COL_DEAL_ID = 0
COL_LINK = 1
OFFSET = 2


def pos(aba, chave):
    """Posicao final da coluna na aba, ja contando as tecnicas do inicio."""
    return LAYOUT[aba]["cols"][chave] + OFFSET


# Cabecalho esperado da PRIMEIRA coluna de cada aba, para a trava. Conferir a planilha inteira
# nao serve: o financeiro mexe nas colunas de cobranca o tempo todo, e travar ali so produz
# alarme falso — foi a licao do gerador do relatorio em 20/08.
ANCORA = "patrocinador"


# --------------------------------------------------------------------------- roteamento

# Como o dealname carrega o enquadramento na pratica. Confirmado em 20/08: o deal da MATIFIC
# se chama "... - ISS SP- Promac" e a Jaqueline o pos na aba ISS SP, embora
# `linha_de_imposto_categoria` diga IR. Quem nomeia o negocio e quem fecha, e acerta mais que
# o campo. Por isso o dealname e sinal mais forte que a property.
_PADROES = (
    (LEI_DO_BEM, r"\bLEI\s*DO\s*BEM\b"),
    (ICMS_RIO, r"\bICMS\b.{0,12}\b(RIO|RJ)\b"),
    (ICMS_SP, r"\bICMS\b.{0,12}\bSP\b"),
    (ISS_RIO, r"\bISS\b.{0,12}\b(RIO|RJ)\b"),
    (ISS_SP, r"\bISS\b.{0,12}\b(SP|PROMAC)\b"),
)

# Sem o estado, ISS e ICMS nao decidem entre duas abas.
_SEM_ESTADO = {"ICMS": (ICMS_RIO, ICMS_SP), "ISS": (ISS_RIO, ISS_SP)}


def _norm(texto):
    return re.sub(r"\s+", " ", str(texto or "")).strip().upper()


def enquadramento_do_dealname(dealname):
    """A aba que o nome do negocio indica, ou "" quando ele nao diz."""
    t = _norm(dealname)
    for aba, padrao in _PADROES:
        if re.search(padrao, t):
            return aba
    return ""


def rotear_aba(props, enquadramento_manual=""):
    """Em qual aba a linha entra. Devolve (aba, confianca, motivo).

    Confianca:
      ALTA    grava
      MEDIA   relatorio, nao grava (sinal existe mas nao fecha o estado)
      ORFA    relatorio, nao grava (nenhum sinal)

    A cascata vai do sinal escrito por quem fecha o negocio para o sinal derivado. Nunca chuta
    entre duas abas: um erro aqui manda o negocio para o processo fiscal errado.
    """
    # 1. O picklist que ainda nao existe. Quando existir, encerra a discussao aqui.
    manual = _norm(enquadramento_manual)
    if manual:
        for aba in ABAS:
            if _norm(aba) == manual:
                return aba, "ALTA", "campo de enquadramento preenchido no card"
        return "", "MEDIA", f"enquadramento {enquadramento_manual!r} nao e uma das 6 abas"

    # 2. O nome do negocio, que na pratica acerta mais que a property.
    pelo_nome = enquadramento_do_dealname(props.get("dealname"))
    if pelo_nome:
        return pelo_nome, "ALTA", "enquadramento no nome do negocio"

    # 3. A property de imposto. Sozinha so decide o IR, que nao se divide por estado.
    imposto = _norm(props.get("linha_de_imposto_categoria"))
    if imposto == "IR":
        return IR, "ALTA", "linha_de_imposto_categoria = IR (nao se divide por estado)"
    if imposto in _SEM_ESTADO:
        a, b = _SEM_ESTADO[imposto]
        return "", "MEDIA", f"imposto {imposto}, mas falta o estado: pode ser {a} ou {b}"

    return "", "ORFA", "sem enquadramento no nome, no campo, nem na lei"


# --------------------------------------------------------------------------- linha

def _dinheiro(v):
    """Numero em pt-BR. Sem simbolo: a planilha formata a coluna."""
    try:
        n = float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return ""
    return f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _data_br(iso):
    s = str(iso or "")[:10]
    if len(s) != 10 or s[4] != "-":
        return ""
    return f"{s[8:10]}/{s[5:7]}/{s[0:4]}"


def interno_externo(tipo_de_proponente):
    """A coluna OBS da planilha guarda INTERNO/EXTERNO, e nao uma observacao."""
    return "EXTERNO" if _norm(tipo_de_proponente) == "EXTERNO" else "INTERNO"


def valor_da_comissao(t_aporte, percentual):
    """% sobre o aporte. E a comissao cobrada do PROPONENTE, depois do evento.

    Nao confundir com o valor do match, que e fixo e vem do patrocinador.
    """
    try:
        return float(str(t_aporte).replace(",", ".")) * float(str(percentual).replace(",", ".")) / 100.0
    except (TypeError, ValueError):
        return None


def deal_link(deal_id, portal="50771078"):
    return f"https://app.hubspot.com/contacts/{portal}/record/0-3/{deal_id}"


def build_row(deal, aba, valor_match="", deal_id=""):
    """Monta a linha da aba, so com as colunas que a automacao sabe preencher.

    Toda posicao nao mapeada fica vazia de proposito: e coluna de cobranca ou de rito fiscal,
    que o financeiro preenche. O script nunca escreve nelas.
    """
    cols = LAYOUT[aba]["cols"]
    largura = max(cols.values()) + 1 + OFFSET
    linha = [""] * largura
    p = deal.get("properties", deal)

    def por(chave, valor):
        if chave in cols and valor not in (None, ""):
            linha[pos(aba, chave)] = valor

    if deal_id:
        linha[COL_DEAL_ID] = str(deal_id)
        linha[COL_LINK] = deal_link(deal_id)

    pct = p.get("percentual_brada")
    aporte = p.get("valor_do_aporte") or p.get("amount")
    comissao = valor_da_comissao(aporte, pct)

    por("patrocinador", p.get("_empresa_associada"))
    por("lei", p.get("lei_principal"))
    por("projeto", p.get("nome_do_projeto"))
    por("obs", interno_externo(p.get("tipo_de_proponente")))
    por("numero", p.get("numero_do_projeto"))
    por("match", _data_br(p.get("closedate")))
    por("valor_match", valor_match)
    por("t_aporte", _dinheiro(aporte))
    por("fp", p.get("numero_parcelas_financeiro"))
    por("proponente", p.get("nome_do_proponente"))
    por("nome", p.get("nome_contato_proponente"))
    por("email", p.get("email_proponente"))
    por("contato", p.get("telefone_proponente"))
    por("pct", pct)
    por("valor_comissao", _dinheiro(comissao) if comissao is not None else "")
    return linha
