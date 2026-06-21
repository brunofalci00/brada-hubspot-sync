"""Testes PUROS (offline, sem CRM/Sheets) da camada raw_funil_eventos do sync.py
(Tarefa 6, 21/06): build_funil_eventos_layer + evento_em_carga (Regra A).

  - 1 linha por (deal x etapa em que entrou), filtro cross-pipeline + null-skip.
  - Regra A de carga: dia de rajada (>=20/etapa) OU migrado importado-direto-na-etapa
    (source IMPORT/INTEGRATION e data_entrada == data_criacao); mantem move organico.

Roda: python ops/_test_funil_eventos.py   (sem env, sem rede)
Importa `sync` (sem chamadas de API no import). build_funil_eventos_layer e' puro.
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


def _stages():
    return {
        "1246562476": {"nome": "Reunião Agendada", "ordem": 4, "pipeline_id": "default", "pipeline_nome": "Incentivador"},
        "1246562478": {"nome": "Diagnóstico", "ordem": 5, "pipeline_id": "default", "pipeline_nome": "Incentivador"},
        "1253324968": {"nome": "Ganho - Incentivador", "ordem": 10, "pipeline_id": "default", "pipeline_nome": "Incentivador"},
        "1246571362": {"nome": "Fechado / Ganho", "ordem": 6, "pipeline_id": "839644419", "pipeline_nome": "Proponente"},
    }


# alias curto -> stage_id (r=reuniao, d=diagnostico, g=ganho incentivador, pg=ganho proponente)
_ALIAS = {"r": "1246562476", "d": "1246562478", "g": "1253324968", "pg": "1246571362"}


def _deal(did, source="CRM_UI", **entered):
    props = {"hs_object_source": source}
    for k, v in entered.items():
        if v and len(v) == 10:   # 'AAAA-MM-DD' -> timestamp ISO
            v = v + "T12:00:00Z"
        props[f"hs_v2_date_entered_{_ALIAS[k]}"] = v
    return {"id": did, "properties": props}


def _enr(did, pipeline_id="default", pipeline_nome="Incentivador", data_criacao="2026-01-01",
         produto="Match", executivo_nome="Fulano", tipo_de_proponente="", valor_do_aporte=0):
    return {"deal_id": did, "pipeline_id": pipeline_id, "pipeline_nome": pipeline_nome,
            "data_criacao": data_criacao, "produto": produto, "executivo_nome": executivo_nome,
            "tipo_de_proponente": tipo_de_proponente, "valor_do_aporte": valor_do_aporte}


def _build(deals, enr):
    return sync.build_funil_eventos_layer(deals, enr, _stages())


def test_evento_basico_e_dims():
    print("\n[1] evento por data nao-nula + dims de arraste copiadas")
    ev = _build([_deal("A1", source="CRM_UI", r="2026-03-15")],
                [_enr("A1", data_criacao="2026-03-01", produto="Match",
                      executivo_nome="Ana", tipo_de_proponente="Externo", valor_do_aporte=5000)])
    check(len(ev) == 1, f"1 data -> 1 evento (got {len(ev)})")
    e0 = ev[0]
    check(e0["data_entrada"] == "2026-03-15", f"data_entrada AAAA-MM-DD (got {e0['data_entrada']!r})")
    check(e0["stage_nome"] == "Reunião Agendada", f"stage_nome (got {e0['stage_nome']!r})")
    check(e0["em_carga"] == 0, "CRM_UI, entry!=create, sem rajada -> em_carga 0")
    check(e0["produto"] == "Match" and e0["executivo_nome"] == "Ana"
          and e0["tipo_de_proponente"] == "Externo" and e0["valor_do_aporte"] == 5000
          and e0["pipeline_nome"] == "Incentivador", "dims de arraste copiadas do enriched")
    check(list(e0.keys())[-1] == "em_carga", "em_carga e' a ultima coluna (ordem do header)")


def test_data_nula_sem_evento():
    print("\n[2] data nula -> nenhum evento pra aquela etapa")
    ev = _build([_deal("N1", source="CRM_UI")], [_enr("N1")])
    check(ev == [], f"deal sem hs_v2_date_entered -> 0 eventos (got {len(ev)})")


def test_tres_etapas():
    print("\n[3] deal que passou por 3 etapas do mesmo pipeline -> 3 eventos")
    ev = _build([_deal("T1", r="2026-03-01", d="2026-03-10", g="2026-03-20")],
                [_enr("T1", data_criacao="2026-02-01")])
    check(len(ev) == 3, f"3 datas -> 3 eventos (got {len(ev)})")
    check({e["stage_id"] for e in ev} == {"1246562476", "1246562478", "1253324968"}, "3 etapas distintas")


def test_regra_a_artefato_import():
    print("\n[4] Regra A: migrado importado-direto-na-etapa (IMPORT, entry==create) -> carga")
    ev = _build([_deal("M1", source="IMPORT", r="2026-02-10")],
                [_enr("M1", data_criacao="2026-02-10")])
    check(len(ev) == 1 and ev[0]["em_carga"] == 1, "IMPORT + entry==create -> em_carga 1")


def test_regra_a_organico_mantem():
    print("\n[5] Regra A: migrado movido-depois (IMPORT, entry>create) -> NAO carga")
    ev = _build([_deal("M2", source="IMPORT", r="2026-02-10")],
                [_enr("M2", data_criacao="2026-01-01")])
    check(len(ev) == 1 and ev[0]["em_carga"] == 0, "IMPORT + entry!=create -> em_carga 0 (move organico mantido)")


def test_regra_a_nativo_nao_e_carga():
    print("\n[6] Regra A: nativo importado-direto (CRM_UI, entry==create) -> NAO carga")
    ev = _build([_deal("M3", source="CRM_UI", r="2026-02-10")],
                [_enr("M3", data_criacao="2026-02-10")])
    check(len(ev) == 1 and ev[0]["em_carga"] == 0, "CRM_UI mesmo com entry==create -> em_carga 0")


def test_rajada_por_dia():
    print("\n[7] rajada >=20/dia na etapa -> carga; 19 -> nao (fronteira)")
    deals20 = [_deal(f"B{i}", source="CRM_UI", r="2026-05-01") for i in range(20)]
    enr20 = [_enr(f"B{i}", data_criacao=f"2026-04-{i+1:02d}") for i in range(20)]  # create != entry
    ev20 = _build(deals20, enr20)
    check(len(ev20) == 20 and all(e["em_carga"] == 1 for e in ev20), "20 no mesmo dia/etapa -> todos carga")
    deals19 = [_deal(f"C{i}", source="CRM_UI", r="2026-05-02") for i in range(19)]
    enr19 = [_enr(f"C{i}", data_criacao=f"2026-04-{i+1:02d}") for i in range(19)]
    ev19 = _build(deals19, enr19)
    check(all(e["em_carga"] == 0 for e in ev19), "19 no mesmo dia -> nenhum carga (fronteira >=20)")


def test_carga_e_por_etapa():
    print("\n[8] carga e' POR ETAPA (12 reuniao + 12 diag no mesmo dia -> nenhum carga)")
    deals = ([_deal(f"R{i}", source="CRM_UI", r="2026-06-01") for i in range(12)] +
             [_deal(f"D{i}", source="CRM_UI", d="2026-06-01") for i in range(12)])
    enr = ([_enr(f"R{i}", data_criacao=f"2026-05-{i+1:02d}") for i in range(12)] +
           [_enr(f"D{i}", data_criacao=f"2026-05-{i+1:02d}") for i in range(12)])
    ev = _build(deals, enr)
    check(len(ev) == 24 and all(e["em_carga"] == 0 for e in ev), "12/etapa < 20 -> nenhum carga (nao soma cross-stage)")


def test_cross_pipeline_skip():
    print("\n[9] null-skip cross-pipeline: deal Proponente ignora etapa de outro pipeline")
    deal = _deal("P1", source="CRM_UI", pg="2026-03-10", r="2026-03-09")  # 'r' e' do Incentivador (stray)
    ev = _build([deal], [_enr("P1", pipeline_id="839644419", pipeline_nome="Proponente", produto="CRIAPE")])
    check(len(ev) == 1 and ev[0]["stage_id"] == "1246571362", "so a etapa do proprio pipeline (ignora stray)")


def test_bordas():
    print("\n[10] bordas: vazio e deal sem enriched")
    check(sync.build_funil_eventos_layer([], [], {}) == [], "([],[],{}) -> []")
    ev = _build([_deal("X", r="2026-03-15")], [])  # sem enriched correspondente
    check(ev == [], "deal sem enriched correspondente -> pulado, sem crash")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    test_evento_basico_e_dims()
    test_data_nula_sem_evento()
    test_tres_etapas()
    test_regra_a_artefato_import()
    test_regra_a_organico_mantem()
    test_regra_a_nativo_nao_e_carga()
    test_rajada_por_dia()
    test_carga_e_por_etapa()
    test_cross_pipeline_skip()
    test_bordas()
    print(f"\n===== {_pass} PASS / {_fail} FAIL =====")
    sys.exit(1 if _fail else 0)


if __name__ == "__main__":
    main()
