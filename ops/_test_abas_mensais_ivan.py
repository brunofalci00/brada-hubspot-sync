# -*- coding: utf-8 -*-
"""Testes da logica pura de sheets_abas_mensais_ivan (sem rede).
Trava o que vira folha: montagem de linha de MATCH/Elaboracao/Ricardo, mapa de
mes e recorte de ciclo. A escrita real (geracao de aba/ocultar/append) e validada
por teste de integracao em sandbox (fora do repo)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sheets_abas_mensais_ivan as m


def test_cycle_to_mes():
    assert m.cycle_to_mes("2026-07") == "Julho"
    assert m.cycle_to_mes("2026-01") == "Janeiro"
    assert m.cycle_to_mes("2026-12") == "Dezembro"


def test_build_match_row():
    r = {"cliente": "Cli", "lei_principal": "Rouanet", "proponente": "Prop",
         "interno_externo": "Interno", "nome_projeto": "Proj", "numero_projeto": "N1",
         "valor_bruto": "1000", "closedate": "2026-06-10T00:00:00Z",
         "nome_contato_proponente": "Contato", "telefone_proponente": "119",
         "email_proponente": "a@b.com", "deal_id": "D1"}
    row = m.build_match_row(r)
    assert len(row) == 17, "A-Q = 17 colunas"
    assert row[0] == "Cli"          # A
    assert row[1] == "IR Cultura"   # B map_lei(Rouanet)
    assert row[2] == "Prop"         # C
    assert row[3] == "Interno"      # D
    assert row[4] == "Proj"         # E
    assert row[5] == "N1"           # F
    assert row[6] == 1000.0         # G valor
    assert row[7] == "10/06/2026"   # H data
    for j in range(8, 13):          # I-M comissoes em branco
        assert row[j] == "", f"comissao idx{j} deve ser vazia"
    assert row[13] == "Contato"     # N
    assert row[14] == "119"         # O
    assert row[15] == "a@b.com"     # P
    assert row[16] == "D1"          # Q deal_id


def test_build_elaboracao_row():
    d = {"id": "E1", "properties": {
        "nome_do_proponente": "PropE", "closedate": "2026-05-03T00:00:00Z",
        "produto": "Prestação de Contas", "condicao_de_pagamento": "Pix pela plataforma",
        "valor_do_aporte": "2000", "lei_principal": "Rouanet"}}
    row = m.build_elaboracao_row(d)
    assert len(row) == 13, "A-M = 13 colunas"
    assert row[0] == "PropE"
    assert row[1] == "03/05/2026"
    assert row[2] == "Prestação de Contas"   # C produto cru (mostra Prestacao!)
    assert row[3] == "Pix pela plataforma"
    assert row[4] == 2000.0
    assert row[5] == "Rouanet"
    for j in range(6, 12):                   # G-L manuais em branco
        assert row[j] == "", f"manual idx{j} deve ser vazia"
    assert row[12] == "E1"                   # M deal_id


def test_build_ricardo_row():
    # layout "Vendas 26_elaboracao" (a partir da col C): Nome, Data fechamento,
    # Condicao, Valor, Lei, Data pagamento, Valor Pago, Link Hubspot, deal_id.
    d = {"id": "R1", "properties": {
        "nome_do_proponente": "PropR", "closedate": "2026-04-01T00:00:00Z",
        "produto": "Elaboração", "condicao_de_pagamento": "Pix pela plataforma",
        "valor_do_aporte": "3000", "lei_principal": "Esporte Federal"}}
    row = m.build_ricardo_row(d)
    assert len(row) == 9, "9 colunas a partir da coluna C"
    assert row[0] == "PropR"                  # Nome
    assert row[1] == "01/04/2026"             # Data do fechamento
    assert row[2] == "Pix pela plataforma"    # Condicao de Pagamento
    assert row[3] == 3000.0                   # Valor
    assert row[4] == "Esporte Federal"        # Lei
    assert row[5] == "01/04/2026"             # Data de pagamento = Data do fechamento (modelo)
    assert row[6] == 3000.0                   # Valor Pago = Valor (modelo)
    assert "hubspot.com" in row[7] and "R1" in row[7]   # Link Hubspot
    assert row[8] == "R1"                                # deal_id


def test_deals_no_ciclo():
    def mk(did, cd):
        return {"id": did, "properties": {"closedate": cd}}
    deals = [mk("A", "2026-06-10T00:00:00Z"),   # dentro 21/05-20/06
             mk("B", "2026-05-01T00:00:00Z"),   # fora (antes)
             mk("C", "2026-07-05T00:00:00Z"),   # fora (depois)
             mk("D", "")]                         # sem data
    ids = {d["id"] for d in m._deals_no_ciclo(deals, "2026-06")}
    assert ids == {"A"}, f"so o do ciclo entra, veio {ids}"


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_")]
    for n, f in tests:
        f()
        print("PASS", n)
    print(f"OK — {len(tests)} testes")
