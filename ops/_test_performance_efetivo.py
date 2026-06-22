"""Teste UNIT (offline, sem HubSpot, sem Sheets) da camada efetiva no
write_performance_sheet — coluna efetivo_brl + efetivo_classificado_n.

Mocka gc (metas sintetica) e captura write_to_sheets (nao escreve nada). Roda a
write_performance_sheet REAL com enriched + efetivo_by_deal sinteticos e assere:
  - colunas novas presentes e NO FIM do header (protege binding posicional Looker)
  - efetivo_brl agrega valor_efetivo_brada dos won do ano (deal de outro ano ignorado)
  - efetivo_classificado_n conta so os com efetivo>0
  - pct_meta (bruto) intacto
Ver PROMPT_Meta_Liquida_vs_Bruta + D3/Backlog_Split_Match. 21/06.

Uso: python ops/_test_performance_efetivo.py   (exit 1 se algum check falhar)
"""
import os
import sys
import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# import sync (do diretorio pai), robusto a como o script e invocado
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sync  # noqa: E402

_pass = _fail = 0


def check(cond, msg):
    global _pass, _fail
    if cond:
        _pass += 1
        print(f"  PASS  {msg}")
    else:
        _fail += 1
        print(f"  FAIL  {msg}")


# ---- mocks ----
class _FakeWS:
    def __init__(self, rows):
        self._rows = rows

    def get(self, rng, value_render_option=None):
        return self._rows


class _FakeSH:
    def __init__(self, metas_rows):
        self._metas = metas_rows

    def worksheet(self, name):
        if name == "metas_anuais":
            return _FakeWS(self._metas)
        raise sync.gspread.exceptions.WorksheetNotFound(name)


class _FakeGC:
    def __init__(self, metas_rows):
        self._sh = _FakeSH(metas_rows)

    def open_by_key(self, key):
        return self._sh


def main():
    ano = str(datetime.datetime.now().year)

    # metas_anuais sintetica (bruta, como na producao)
    metas_rows = [
        ["produto", "meta_anual_brl", "ano"],
        ["Match", 4_100_000, ano],
        ["CRIAPE", 0, ano],
    ]

    # enriched sintetico
    enriched = [
        {"deal_id": "1", "produto": "Match", "e_ganho": 1,
         "closedate": f"{ano}-03-01", "valor_vendido": 1_000_000, "valor_projetado_ativo": 0},
        {"deal_id": "2", "produto": "Match", "e_ganho": 1,
         "closedate": f"{ano}-04-01", "valor_vendido": 2_000_000, "valor_projetado_ativo": 0},
        {"deal_id": "3", "produto": "CRIAPE", "e_ganho": 1,
         "closedate": f"{ano}-05-01", "valor_vendido": 1_000_000, "valor_projetado_ativo": 0},
        # deal won de ANO ANTERIOR -> deve ser ignorado no agregado do ano corrente
        {"deal_id": "4", "produto": "Match", "e_ganho": 1,
         "closedate": "2024-01-01", "valor_vendido": 9_999, "valor_projetado_ativo": 0},
    ]

    # efetivo por deal (vindo do consolidado): deal 1 classificado, deal 2 NAO (0),
    # deal 3 CRIAPE = 15% de 1M, deal 4 (ano anterior) nem entra
    efetivo_by_deal = {"1": 100_000.0, "2": 0.0, "3": 150_000.0, "4": 1_500.0}

    # captura o write_to_sheets em vez de escrever
    captured = {}

    def _fake_write(data, header, worksheet_name=None, meta_label=None, meta_range=None):
        captured["data"] = data
        captured["header"] = header
        captured["ws"] = worksheet_name

    orig_write = sync.write_to_sheets
    sync.write_to_sheets = _fake_write
    try:
        sync.write_performance_sheet(enriched, _FakeGC(metas_rows), efetivo_by_deal=efetivo_by_deal)
    finally:
        sync.write_to_sheets = orig_write

    check(bool(captured), "write_performance_sheet chamou write_to_sheets")
    header = captured.get("header", [])
    data = captured.get("data", [])
    by_prod = {r[header.index("produto")]: r for r in data} if header else {}

    # colunas novas presentes e NO FIM (binding Looker)
    check("efetivo_brl" in header and "efetivo_classificado_n" in header,
          "colunas efetivo_brl + efetivo_classificado_n presentes")
    check(header[-2:] == ["efetivo_brl", "efetivo_classificado_n"],
          "colunas efetivas no FIM do header (protege binding posicional Looker)")

    # Match: vendido 3M (deal 2024 ignorado), efetivo 100k (deal 2 = 0), classificado 1/2
    m = by_prod.get("Match", [])
    check(m and m[header.index("vendido_brl")] == 3_000_000.0,
          "Match vendido_brl = 3.000.000 (deal de outro ano ignorado)")
    check(m and m[header.index("efetivo_brl")] == 100_000.0,
          "Match efetivo_brl = 100.000 (so deal 1; deal 2 tem efetivo 0)")
    check(m and m[header.index("efetivo_classificado_n")] == 1,
          "Match efetivo_classificado_n = 1 (de 2 ganhos do ano)")
    check(m and m[header.index("n_ganhos_ano")] == 2,
          "Match n_ganhos_ano = 2 (deal 2024 fora)")

    # CRIAPE: efetivo 150k = 15% de 1M (invariante checado de fato no guard, live)
    c = by_prod.get("CRIAPE", [])
    check(c and c[header.index("efetivo_brl")] == 150_000.0,
          "CRIAPE efetivo_brl = 150.000")

    # pct_meta (bruto) intacto: 3.000.000 / 4.100.000
    check(m and m[header.index("pct_meta")] == round(3_000_000 / 4_100_000, 4),
          "pct_meta bruto intacto (vendido/meta = 0,7317), nao tocado pela camada efetiva")

    # sanidade: efetivo_brl <= vendido_brl em todo produto
    ok_sane = all(
        r[header.index("efetivo_brl")] <= r[header.index("vendido_brl")] for r in data
    )
    check(ok_sane, "efetivo_brl <= vendido_brl em todos os produtos (corte nunca passa o bruto)")

    print(f"\n  {_pass} PASS / {_fail} FAIL")
    sys.exit(1 if _fail else 0)


if __name__ == "__main__":
    main()
