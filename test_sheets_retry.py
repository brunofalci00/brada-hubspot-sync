"""Retry nas chamadas ao Google Sheets.

Existe por causa de falha real: em 26/08 as 06:51 o `Sync HubSpot -> Sheets` morreu com
`gspread.exceptions.APIError: [503]: The service is currently unavailable`. Nao era erro
nosso — o Google pisca, e a rodada inteira ia junto porque nao havia retentativa.

O mesmo 503 derrubou 6 de 60 rodadas do mutirao no brada-plataforma-sync, no mesmo dia.
"""

import pytest

import sync


class RespostaFake:
    def __init__(self, status):
        self.status_code = status


class ErroAPI(Exception):
    """Imita gspread.exceptions.APIError: o que importa e ter `.response.status_code`."""
    def __init__(self, status):
        super().__init__(f"APIError [{status}]")
        self.response = RespostaFake(status)


def _chamador(erros, resultado="ok"):
    """Devolve (fn, registro): fn levanta os erros da lista, depois entrega o resultado."""
    fila = list(erros)
    registro = {"chamadas": 0}

    def fn(*a, **kw):
        registro["chamadas"] += 1
        if fila:
            raise fila.pop(0)
        return resultado

    return fn, registro


def _sono():
    esperas = []
    return esperas, esperas.append


def test_sucesso_na_primeira_nao_dorme():
    esperas, dormir = _sono()
    fn, reg = _chamador([])
    assert sync.com_retry(fn, dormir=dormir)() == "ok"
    assert reg["chamadas"] == 1
    assert esperas == []


def test_503_e_retentado_e_entrega():
    # O caso exato que derrubou as duas automacoes em 26/08.
    esperas, dormir = _sono()
    fn, reg = _chamador([ErroAPI(503)])
    assert sync.com_retry(fn, dormir=dormir)() == "ok"
    assert reg["chamadas"] == 2


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_toda_a_familia_transitoria_e_retentada(status):
    esperas, dormir = _sono()
    fn, reg = _chamador([ErroAPI(status)])
    assert sync.com_retry(fn, dormir=dormir)() == "ok"
    assert reg["chamadas"] == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_erro_de_permissao_estoura_na_hora(status):
    """Retentar credencial errada so atrasa a descoberta de um problema que nao passa.

    E o ponto que separa este retry de um `except Exception` preguicoso: 403 de planilha
    sem acesso tem que aparecer na primeira, nao 35 segundos depois.
    """
    esperas, dormir = _sono()
    fn, reg = _chamador([ErroAPI(status)])
    with pytest.raises(ErroAPI):
        sync.com_retry(fn, dormir=dormir)()
    assert reg["chamadas"] == 1
    assert esperas == []


def test_erro_de_codigo_nosso_nao_e_mascarado():
    # TypeError nao tem `.response`. Retentar 4 vezes so esconderia o bug.
    esperas, dormir = _sono()
    fn, reg = _chamador([TypeError("bug nosso")])
    with pytest.raises(TypeError):
        sync.com_retry(fn, dormir=dormir)()
    assert reg["chamadas"] == 1


def test_desiste_depois_das_tentativas_e_propaga():
    esperas, dormir = _sono()
    fn, reg = _chamador([ErroAPI(503)] * 9)
    with pytest.raises(ErroAPI):
        sync.com_retry(fn, tentativas=4, dormir=dormir)()
    assert reg["chamadas"] == 4


def test_backoff_dobra():
    # 5, 10, 20 = 35s de espera total antes de desistir. Uma piscada do Google cabe.
    esperas, dormir = _sono()
    fn, _ = _chamador([ErroAPI(503)] * 9)
    with pytest.raises(ErroAPI):
        sync.com_retry(fn, tentativas=4, espera_inicial=5, dormir=dormir)()
    assert esperas == [5, 10, 20]


def test_argumentos_chegam_intactos():
    # O wrapper entra na frente de HTTPClient.request, que recebe method, endpoint e kwargs.
    vistos = {}

    def fn(*a, **kw):
        vistos["args"], vistos["kwargs"] = a, kw
        return "ok"

    sync.com_retry(fn, dormir=lambda _: None)("GET", "/x", params={"a": 1})
    assert vistos["args"] == ("GET", "/x")
    assert vistos["kwargs"] == {"params": {"a": 1}}


def test_envolver_cliente_instala_o_wrapper():
    """Guarda do ponto unico: quem esquecer de envolver quebra este teste.

    Vale para os dois caminhos do repo — o `get_sheets_client()` do sync.py e o cliente
    proprio do check_looker_contract.py.
    """
    class ClienteFake:
        def __init__(self):
            self.http_client = type("H", (), {"request": staticmethod(lambda *a, **k: "ok")})()

    gc = sync.envolver_cliente(ClienteFake())
    assert gc.http_client.request.__name__ == "_wrapper"
    assert gc.http_client.request() == "ok"
