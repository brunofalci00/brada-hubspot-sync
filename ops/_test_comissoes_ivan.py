# -*- coding: utf-8 -*-
"""Testes da logica pura de sheets_comissoes_ivan (sem rede).
Trava o que vira folha: dedup (classify), montagem de linha (build_row),
recorte de ciclo (select_cycle) e completude. A escrita real (read_cv/append/
ocultar coluna) e validada por teste de integracao em sandbox (fora do repo)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sheets_comissoes_ivan as m


def mk(deal_id, cliente="X", valor="1000", lei="Rouanet", num="123", ie="Interno",
       closedate="2026-06-10T00:00:00Z", cnpj="111", proj="P", prop="Prop"):
    return {"deal_id": deal_id, "cliente": cliente, "valor_bruto": valor, "lei_principal": lei,
            "numero_projeto": num, "interno_externo": ie, "nome_projeto": proj, "proponente": prop,
            "closedate": closedate, "cnpj": cnpj}


def test_classify_deal_id_e_valor():
    cv = {"seen_deal_ids": {"D1"}, "legacy": [{"val": 2000.0, "num": "999", "cli": "y"}]}
    cands = [mk("D1", valor="500"), mk("D2", valor="2000", num="999"), mk("D3", valor="333")]
    ja, dup, novos = m.classify(cands, cv)
    assert [r["deal_id"] for r in ja] == ["D1"], "deal_id ja presente -> ja_presente"
    assert [r["deal_id"] for r in dup] == ["D2"], "valor ja na CV -> provavel_dup"
    assert dup[0]["_dup_num_match"] is True, "numero do projeto bate -> sinaliza"
    assert [r["deal_id"] for r in novos] == ["D3"], "valor inedito -> novo"


def test_classify_dup_sem_num_match():
    cv = {"seen_deal_ids": set(), "legacy": [{"val": 700.0, "num": "111", "cli": "a"}]}
    dup = m.classify([mk("Z", valor="700", num="888")], cv)[1]
    assert dup and dup[0]["_dup_num_match"] is False, "valor bate, numero nao -> dup com flag False"


def test_build_row_colunas():
    r = mk("D9", cliente="Cli", valor="1500,50", lei="Rouanet", num="N1", ie="Externo",
           closedate="2026-05-03T00:00:00Z", proj="Proj", prop="Pp")
    row = m.build_row(r)
    assert len(row) == m.N_CV_COLS + 1
    assert row[0] == "Cli"          # A Cliente
    assert row[1] == "IR Cultura"   # B Fonte (map_lei Rouanet)
    assert row[2] == "Pp"           # C Proponente
    assert row[4] == "Proj"         # E Projeto
    assert row[5] == "N1"           # F Numero
    assert row[8] == 1500.5         # I Valor (numero)
    assert row[9] == "03/05/2026"   # J Data (dd/mm/aaaa)
    assert row[11] == "Externo"     # L Interno/Externo
    assert row[18] == "D9"          # S deal_id (tecnica)
    for j in [3, 6, 7, 10, 12, 13, 14, 15, 16, 17]:   # D,G,H,K,M-R manuais
        assert row[j] == "", f"coluna idx {j} deve ficar vazia (manual)"


def test_select_cycle_janela():
    cands = [mk("A", closedate="2026-06-10T00:00:00Z"),   # dentro 21/05-20/06
             mk("B", closedate="2026-05-01T00:00:00Z"),   # fora (antes)
             mk("C", closedate="2026-07-05T00:00:00Z"),   # fora (depois)
             mk("D", closedate="")]                        # sem data
    ids = {r["deal_id"] for r in m.select_cycle(cands, "2026-06")}
    assert ids == {"A"}, f"so o do ciclo entra, veio {ids}"


def test_select_all_pending():
    cands = [mk("A", closedate="2026-06-10T00:00:00Z"), mk("D", closedate="")]
    assert len(m.select_cycle(cands, "2026-06", all_pending=True)) == 2


def test_completude():
    assert m.completude_gaps(mk("X")) == []
    assert "lei_principal" in m.completude_gaps(mk("X", lei=""))
    assert "lei_principal" in m.completude_gaps(mk("X", lei="(sem lei preenchida)"))
    assert "valor" in m.completude_gaps(mk("X", valor="0"))
    assert any("cnpj" in f for f in m.completude_gaps(mk("X", cnpj="")))
    assert "closedate" in m.completude_gaps(mk("X", closedate=""))


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_")]
    for n, f in tests:
        f()
        print("PASS", n)
    print(f"OK — {len(tests)} testes")
