"""Testes PUROS (offline, sem CRM/Sheets) da metrica de funil "passou pela etapa"
(reuniao/diagnostico) adicionada ao sync.py (21/06):

  1. enrich()      -> emite data_entrou_reuniao/diagnostico em AAAA-MM-DD (ou ""),
                      flags *_em_carga nascem 0.
  2. dias_em_carga -> set de dias com >= burst_min entradas (deteccao de carga).

Roda: python ops/_test_funil_entrada.py   (sem env, sem rede)
Importar dispara `from sync import ...`, mas o import do sync nao faz chamadas de
API (so le env com defaults), entao roda offline. enrich() e' seguro offline:
owners/contacts/companies tratados com `or {}`.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sync  # noqa: E402

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


def _fake_stage(stage_id):
    return {stage_id: {"nome": "stage", "ordem": 1, "pipeline_id": "default",
                       "pipeline_nome": "Incentivador", "probability": "", "is_closed": False}}


def _enrich(props, stage_id="1246562476"):
    deal = {"id": "D1", "properties": {"dealstage": stage_id, **props}}
    return sync.enrich(deal, _fake_stage(stage_id), {}, {})


def test_enrich_emite_data():
    print("\n[1] enrich: data_entrou_* em AAAA-MM-DD quando preenchido")
    out = _enrich({
        "hs_v2_date_entered_1246562476": "2026-03-15T10:00:00Z",
        "hs_v2_date_entered_1246562478": "2026-04-20T09:30:00.412Z",
    })
    check(out["data_entrou_reuniao"] == "2026-03-15", f"reuniao -> 2026-03-15 (got {out['data_entrou_reuniao']!r})")
    check(out["data_entrou_diagnostico"] == "2026-04-20", f"diagnostico -> 2026-04-20 (got {out['data_entrou_diagnostico']!r})")


def test_enrich_nulo():
    print("\n[2] enrich: data vazia quando a property e' nula (cobre migrado->Ganho)")
    out = _enrich({})  # sem hs_v2_date_entered_*
    check(out["data_entrou_reuniao"] == "", "reuniao ausente -> ''")
    check(out["data_entrou_diagnostico"] == "", "diagnostico ausente -> ''")


def test_enrich_flags_nascem_zero():
    print("\n[3] enrich: flags *_em_carga nascem 0 (setadas so na 2a passada do main)")
    out = _enrich({"hs_v2_date_entered_1246562476": "2026-01-02T18:11:50Z"})
    check(out["entrou_reuniao_em_carga"] == 0, "entrou_reuniao_em_carga = 0")
    check(out["entrou_diagnostico_em_carga"] == 0, "entrou_diagnostico_em_carga = 0")
    for k in ("data_entrou_reuniao", "data_entrou_diagnostico",
              "entrou_reuniao_em_carga", "entrou_diagnostico_em_carga"):
        check(k in out, f"chave '{k}' presente no dict do enrich")


def test_dias_em_carga():
    print("\n[4] dias_em_carga: rajada (>=20/dia) detectada, organico nao")
    dias = ["2026-01-02"] * 20 + ["2026-03-01", "2026-03-02", "2026-03-01"]
    carga = sync.dias_em_carga(dias)
    check(carga == {"2026-01-02"}, f"so 02/01 (20 entradas) e' carga (got {carga})")
    check("2026-03-01" not in carga, "dia organico (2 entradas) nao e' carga")
    check(sync.dias_em_carga(["2026-01-02"] * 19) == set(), "19 no mesmo dia -> nao (fronteira >=20)")
    check(sync.dias_em_carga([""] * 30) == set(), "strings vazias ignoradas")
    check(sync.dias_em_carga(["x"] * 5, burst_min=5) == {"x"}, "burst_min custom respeitado")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    test_enrich_emite_data()
    test_enrich_nulo()
    test_enrich_flags_nascem_zero()
    test_dias_em_carga()
    print(f"\n===== {_pass} PASS / {_fail} FAIL =====")
    sys.exit(1 if _fail else 0)


if __name__ == "__main__":
    main()
