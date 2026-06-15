"""Testes PUROS (offline, sem CRM/Sheets) das mudancas do Sprint D no
sheets_reporting_financeiro_mensal.py:

  1. build_preserved_map  -> lookup por NOME, regras texto/currency, crash-safe
  2. apply_preservation    -> reinjeta manuais por deal_id, nao toca o resto
  3. build_record          -> out tem 25 celulas == len(full_header), contato no lugar

Roda: python ops/_test_sprint_d.py   (sem env, sem rede)
Importar o modulo dispara `from sync import ...`, mas o import do sync nao faz
chamadas de API (so le env com defaults), entao roda offline.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sheets_reporting_financeiro_mensal as fin  # noqa: E402

OLD_HEADER = fin.TARGET_HEADER + fin.TECH_HEADER                       # v0.2 (deal_id @19)
NEW_HEADER = fin.TARGET_HEADER + fin.CONTATO_HEADER + fin.TECH_HEADER  # v0.3 (deal_id @22)

_pass = 0
_fail = 0


def check(cond, msg):
    global _pass, _fail
    if cond:
        _pass += 1
        print(f"  PASS  {msg}")
    else:
        _fail += 1
        print(f"  FAIL  {msg}")


def _row(header, deal_id, **manuais):
    """Linha (lista do tamanho do header) com manuais nos indices certos."""
    row = [""] * len(header)
    row[header.index("deal_id")] = deal_id
    # manuais sempre nos indices do TARGET (iguais em old/new)
    for name, val in manuais.items():
        col = {
            "cobranca": "Dados para Cobrança",
            "conta_m": "Nº conta M",
            "conta_c": "Nº conta C",
            "externo": "Comissão externo 3%",
            "nome_ext": "Nome do externo",
        }[name]
        row[header.index(col)] = val
    return row


def _fake_consolidado_row(deal_id="D1"):
    """Dict do consolidado com as chaves que build_record le."""
    return {
        "cliente": "Empresa Teste", "lei_principal": "Rouanet", "proponente": "EGP",
        "nome_projeto": "Projeto X", "numero_projeto": "2400704",
        "valor_bruto": "46567,65", "data_aporte": "", "closedate": "2025-12-15T00:00:00Z",
        "interno_externo": "Interno", "valor_efetivo_brada": "6985,15",
        "liquido_brada": "6146,93", "comissao_ivan": "491,75", "comissao_jaque": "245,88",
        "comissao_externo": "0", "deal_id": deal_id,
        "comissao_status": "ok", "closedate_status": "real",
        "nome_contato_proponente": "Fulano", "email_proponente": "f@x.com",
        "telefone_proponente": "+5511999",
    }


def test_preserved_map_layout_invariante():
    print("\n[1] build_preserved_map: identico em layout antigo e novo")
    manuais = dict(cobranca="Cobrar via boleto", conta_m="46109-1", conta_c="46119-9",
                   externo="1.234,56", nome_ext="John")
    old = [OLD_HEADER, _row(OLD_HEADER, "D1", **manuais)]
    new = [NEW_HEADER, _row(NEW_HEADER, "D1", **manuais)]
    m_old = fin.build_preserved_map(old)
    m_new = fin.build_preserved_map(new)
    check(m_old == m_new, f"mapas iguais (old deal_id@{OLD_HEADER.index('deal_id')}, new @{NEW_HEADER.index('deal_id')})")
    check(m_new.get("D1", {}).get("Nº conta M") == "46109-1", "Nº conta M preservado como texto")
    check(m_new.get("D1", {}).get("Dados para Cobrança") == "Cobrar via boleto", "Dados para Cobranca preservado")
    check(m_new.get("D1", {}).get("Comissão externo 3%") == 1234.56, "externo vira FLOAT 1234.56")
    check(isinstance(m_new["D1"]["Comissão externo 3%"], float), "externo guardado como float (nao str)")


def test_preserved_map_regras():
    print("\n[2] build_preserved_map: placeholder e stale-zero descartados")
    rows = [NEW_HEADER,
            _row(NEW_HEADER, "D1", cobranca="favor preencher", externo="0,00"),
            _row(NEW_HEADER, "D2", conta_m="  ", nome_ext="  ")]  # whitespace
    m = fin.build_preserved_map(rows)
    check("D1" not in m, "linha so com placeholder + externo 0,00 -> nao preservada")
    check("D2" not in m, "linha so com whitespace -> nao preservada")


def test_preserved_map_crash_safe():
    print("\n[3] build_preserved_map: crash-safe")
    check(fin.build_preserved_map([]) == {}, "sheet vazio -> {}")
    check(fin.build_preserved_map([["Cliente", "Valor"]]) == {}, "header sem deal_id -> {}")
    no_did = [NEW_HEADER, _row(NEW_HEADER, "", conta_m="X")]
    check(fin.build_preserved_map(no_did) == {}, "linha com deal_id vazio -> ignorada")


def test_apply_preservation():
    print("\n[4] apply_preservation: reinjeta manuais por deal_id")
    records = [fin.build_record(_fake_consolidado_row("D1"))]
    preserved = {"D1": {"Nº conta M": "99-9", "Comissão externo 3%": 1234.56, "Nome do externo": "John"},
                 "D2": {"Nº conta M": "00-0"}}  # D2 nao existe -> ignorado
    n = fin.apply_preservation(records, preserved)
    out = records[0]["out"]
    check(n == 1, "1 deal reinjetado (D2 ignorado)")
    check(out[6] == "99-9", "Nº conta M (idx 6) reinjetado")
    check(out[16] == 1234.56, "externo (idx 16) reinjetado como float")
    check(out[17] == "John", "Nome do externo (idx 17) reinjetado")
    check(out[3] == "favor preencher", "Dados para Cobranca (idx 3) NAO sobrescrito (nao estava no preserved)")


def test_build_record_layout():
    print("\n[5] build_record: 25 celulas, contato no lugar certo")
    rec = fin.build_record(_fake_consolidado_row("D1"))
    out = rec["out"]
    full = fin.TARGET_HEADER + fin.CONTATO_HEADER + fin.TECH_HEADER
    check(len(out) == 25, f"out tem 25 celulas (tem {len(out)})")
    check(len(out) == len(full), "len(out) == len(full_header)")
    check(out[16] == "" and out[17] == "", "externo (16) e nome externo (17) nascem vazios (Block 3)")
    check(out[18] == "Fulano", "S (18) = nome contato proponente")
    check(out[19] == "f@x.com", "T (19) = email proponente")
    check(out[20] == "+5511999", "U (20) = telefone proponente")
    check(out[22] == "D1", "W (22) = deal_id tecnica")
    check(out[23] == "ok", "X (23) = comissao_status (o que print_report passou a usar)")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    test_preserved_map_layout_invariante()
    test_preserved_map_regras()
    test_preserved_map_crash_safe()
    test_apply_preservation()
    test_build_record_layout()
    print(f"\n===== {_pass} PASS / {_fail} FAIL =====")
    sys.exit(1 if _fail else 0)


if __name__ == "__main__":
    main()
