"""Reconciliacao planilha OFICIAL Vendas_25_26 -> HubSpot Incentivador (08/06).

A planilha OFICIAL do Ivan (13timE4...) e a fonte da verdade unica (substitui abas
fragmentadas + classificacao_match_ivan). Grao = 1 card por (empresa canonica, numero);
parcelas com mesmo numero somam. Planilha vence (valor / interno-externo / data).
CRIAPE (pipeline Proponente 839644419) esta FORA: este modulo so escreve em Incentivador
(pipeline 'default') -- asserido no apply (guarda de regressao CRIAPE estrutural).

Match: cn/cnpj vem VAZIOS no HS -> casa pelo NOME DO DEAL (dn_to_cliente). Tiers:
numero(empresa) -> valor(empresa) -> numero GLOBAL (com guarda) -> valor+nome GLOBAL -> card vazio.

closedate (decisao Bruno 08/06): ano = 'Ano de Fechamento'; usa dia/mes reais do aporte
quando o ano coincide, senao 15/dez do Ano de Fechamento.

GOTCHA: HubSpot valor = decimal limpo -> hs_num. Planilha = formato BR -> parse_brl. NUNCA cruzar.

Uso:
    python ops/reconciliacao_oficial.py                  # DRY-RUN: plano + tabela-mestre + battery
    python ops/reconciliacao_oficial.py --sample 3       # amostra por bucket (payloads old->new)
    python ops/reconciliacao_oficial.py --apply-fields --only valor --execute   # PATCH (gated)
    python ops/reconciliacao_oficial.py --create-first --execute                # cria 1 deal e para
    python ops/reconciliacao_oficial.py --create-execute --execute              # cria lote restante
Reusa loaders/normalizadores do resolver + fixer + sync. NAO escreve sem --execute.
"""
import os
import re
import sys
import json
import time
import argparse
from collections import defaultdict, Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import resolver_planilha_multicampo as R  # noqa: E402
import fixer_espelhamento as F            # noqa: E402
sync = R.sync

OFICIAL_SHEET = "13timE4IsrBPR7PIoIdOeBOp_OFup-Wa1LY0RO-tvjrY"
OFICIAL_TAB = "Planilha1"
HEADER_ROW = 2  # 0-based: header na linha 3
DASH_SHEET = "1bBGfZIkjuqBBQL9U79cMWQUgz9FLrbIKm2SObAXEKF8"
OUT_ABA = "reconciliacao_planilha_cards"
INCENT_PIPE = "default"
GANHO_STAGE = "1253324968"   # Ganho - Incentivador
PRODUTO_MATCH = "Match"
CARD_LINK = "https://app.hubspot.com/contacts/50771078/record/0-3/{}"
SNAP_DIR = r"C:\tmp"
SNAP_DEALS = os.path.join(SNAP_DIR, "recon_oficial_deals.json")

TOTAL_ESPERADO = 24138755.97
ANO_TOTAIS = {"2025": 21066925.34, "2026": 3071830.63}
N_LINHAS_ESPERADO = 79

_MES_PT = {"janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5, "junho": 6,
           "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12}

# Sem HOLDS: Bruno autorizou (08/06) replicar a planilha integralmente. Corrida e
# MedWrites/RMed ja resolvidos na conversa Ivan (transcricao). Planilha vence SEMPRE.
HOLDS = {}
HOLD_EMPRESAS = {}
# ALIAS de canonico: vazio. (rmed->medwriters somava cards separados de mesmo numero;
# o cross-naming real e so o card de 1.46M, tratado por repurpose-por-valor no create.)
ALIAS = {}

REAL_PROPS = {"valor_do_aporte", "tipo_de_proponente", "closedate",
              "numero_do_projeto", "nome_do_proponente", "linha_de_imposto_categoria"}
ONLY_MAP = {
    "valor": {"valor_do_aporte"}, "tipo": {"tipo_de_proponente"},
    "closedate": {"closedate", "data_do_aporte"},
    "lei": {"linha_de_imposto_categoria"}, "numero": {"numero_do_projeto"},
    "proponente": {"nome_do_proponente"},
}


# ---------- datas / fonte / nome ----------
def _dez15(ano):
    return f"{int(ano)}-12-15" if str(ano or "").strip().isdigit() else ""


def parse_data_aporte(s, ano, policy="fechamento"):
    """Retorna (iso, origem). policy='fechamento' (decidido): ano sempre = Ano de Fechamento."""
    s = (s or "").strip()
    if not s:
        return _dez15(ano), "vazio->dez15(ano_fech)"
    try:
        dt = datetime.strptime(s, "%m/%d/%Y").date()
        if policy == "aporte":
            return dt.isoformat(), "us_full(aporte)"
        if str(dt.year) == str(ano):
            return dt.isoformat(), "us_full"
        return _dez15(ano), f"ano_difere({dt.year}!={ano})->dez15"
    except ValueError:
        pass
    if re.fullmatch(r"20\d\d", s):  # ano nu (ISS)
        return (f"{s}-12-15", "iss_ano_nu") if policy == "aporte" else (_dez15(ano), "iss_ano_nu->dez15(ano_fech)")
    mes = _MES_PT.get(sync._norm_key(s))  # mes PT por extenso
    if mes and str(ano).isdigit():
        return f"{int(ano)}-{mes:02d}-15", "mes_pt_sem_dia"
    return _dez15(ano), "fallback->dez15(ano_fech)"


def norm_fonte(s):
    return " ".join((s or "").strip().lower().replace("/", " ").split())


def dn_to_cliente(dn):
    """Extrai o cliente-cabeca do nome do deal (cn vem vazio no HS).
    'Nubank - EGP - Move Verao' -> 'Nubank'; 'RMED ... (proj)' -> 'RMED ...'."""
    s = (dn or "").split("(")[0]
    s = s.split(" - ")[0]
    return s.strip()


def ck2(s):
    """Canonico de empresa com ALIAS (colapsa cross-naming conhecido, ex.: rmed->medwriters)."""
    k = F.cnk(s)
    return ALIAS.get(k, k)


def iso_to_epoch_ms(iso):
    """ISO YYYY-MM-DD -> epoch ms meia-noite UTC (formato seguro p/ date/datetime no HubSpot)."""
    if not iso:
        return None
    d = datetime.strptime(str(iso)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return str(int(d.timestamp() * 1000))


def to_iso_date(v):
    """Normaliza valor de data do HS (epoch ms OU ISO) -> 'YYYY-MM-DD' p/ comparacao idempotente."""
    v = str(v or "").strip()
    if not v:
        return ""
    if v.isdigit():
        return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return v[:10]


# ---------- loader planilha oficial ----------
def load_oficial(gc, policy="fechamento"):
    v = gc.open_by_key(OFICIAL_SHEET).worksheet(OFICIAL_TAB).get_all_values()
    hdr = [h.strip() for h in v[HEADER_ROW]]
    ix = {h: i for i, h in enumerate(hdr)}

    def col(r, name):
        i = ix.get(name)
        return r[i].strip() if (i is not None and i < len(r)) else ""

    out = []
    for r in v[HEADER_ROW + 1:]:
        if not any((c or "").strip() for c in r):
            continue
        cli_raw = col(r, "Cliente")
        if not cli_raw or cli_raw.lower() in ("total", "totais"):
            continue
        prop_raw = col(r, "Proponente")
        cli = " / ".join(p.strip() for p in cli_raw.split("\n") if p.strip())
        prop = " / ".join(p.strip() for p in prop_raw.split("\n") if p.strip())
        ano = col(r, "Ano de Fechamento")
        data_iso, data_src = parse_data_aporte(col(r, "Data do aporte"), ano, policy)   # data de fechamento (closedate)
        data_lit, _ = parse_data_aporte(col(r, "Data do aporte"), ano, "aporte")        # data de aporte literal
        out.append(dict(
            cli=cli, fonte_raw=col(r, "Fonte de recurso"), fonte=norm_fonte(col(r, "Fonte de recurso")),
            proponente=prop, projeto=col(r, "Projeto"),
            num_raw=col(r, "Numero do projeto"), num=F.numk(col(r, "Numero do projeto")),
            valor=R.parse_brl(col(r, "Valor")) or 0.0, data_aporte_raw=col(r, "Data do aporte"),
            ano_fechamento=ano, origem=col(r, "Origem"), interno_externo=col(r, "Projeto Interno?"),
            closedate=data_iso, closedate_src=data_src, data_aporte_lit=data_lit,
            multi_cliente=("\n" in cli_raw or "\n" in prop_raw),
        ))
    return out


def group_oficial(rows):
    proj = defaultdict(list)
    for p in rows:
        ck = ck2(p["cli"])
        nk = F.numk(p["num_raw"])
        key = (ck, nk if len(nk) >= 4 else f"noproj:{round(p['valor'])}")
        proj[key].append(p)
    # set de numeros compartilhados por >1 empresa canonica (tangles de projeto)
    num_emps = defaultdict(set)
    for (ck, nk), _ in proj.items():
        if not str(nk).startswith("noproj"):
            num_emps[nk].add(ck)
    shared = {nk for nk, emps in num_emps.items() if len(emps) > 1}

    grupos = []
    for (ck, nk), parc in proj.items():
        real_num = "" if str(nk).startswith("noproj") else nk
        fontes = [x["fonte"] for x in parc if x["fonte"]]
        ies = [x["interno_externo"] for x in parc if x["interno_externo"]]
        props = [x["proponente"] for x in parc if x["proponente"]]
        closes = [x["closedate"] for x in parc if x["closedate"]]
        grupos.append(dict(
            emp_canon=ck, nome_disp=Counter(x["cli"] for x in parc).most_common(1)[0][0],
            num=real_num, num_raw=next((x["num_raw"] for x in parc if x["num_raw"]), ""),
            valor=round(sum(x["valor"] for x in parc), 2), n_parcelas=len(parc), parcelas=parc,
            fonte=fontes[0] if fontes else "", fonte_raw=next((x["fonte_raw"] for x in parc if x["fonte_raw"]), ""),
            proponente=props[0] if props else "", interno_externo=ies[0] if ies else "",
            ano_fechamento=next((x["ano_fechamento"] for x in parc if x["ano_fechamento"]), ""),
            closedate=Counter(closes).most_common(1)[0][0] if closes else "",
            closedate_src=next((x["closedate_src"] for x in parc), ""),
            data_aporte_lit=next((x["data_aporte_lit"] for x in parc if x.get("data_aporte_lit")), ""),
            projeto=next((x["projeto"] for x in parc if x.get("projeto")), ""),
            inconsist_fonte=len(set(fontes)) > 1, inconsist_ie=len(set(ies)) > 1,
            inconsist_prop=len(set(props)) > 1, tangle_num=real_num in shared,
            multi_cliente=any(x.get("multi_cliente") for x in parc),
        ))
    return grupos


# ---------- HubSpot ----------
def load_deals_cached(refresh=False, max_age_h=6.0):
    if not refresh and os.path.exists(SNAP_DEALS):
        age_h = (time.time() - os.path.getmtime(SNAP_DEALS)) / 3600.0
        if age_h < max_age_h:
            with open(SNAP_DEALS, encoding="utf-8") as f:
                snap = json.load(f)
            print(f"  [cache] deals de {SNAP_DEALS} (idade {age_h:.1f}h, {len(snap['pool'])} Incentivador)")
            return snap["by_id"], snap["pool"]
    by_id, pool = F.retry(R.load_deals)
    try:
        os.makedirs(SNAP_DIR, exist_ok=True)
        with open(SNAP_DEALS, "w", encoding="utf-8") as f:
            json.dump({"by_id": by_id, "pool": pool}, f, ensure_ascii=False)
        print(f"  [cache] snapshot salvo {SNAP_DEALS} (serve de backup pre-apply)")
    except Exception as e:
        print(f"  [cache] falhou salvar: {e}")
    return by_id, pool


def build_hs_index(pool):
    hs_by, by_num = defaultdict(list), defaultdict(list)
    for d in pool:
        rec = dict(d, _used=False, _cli=dn_to_cliente(d["dn"]), _tipo="", _data_aporte="")
        rec["_ck"] = ck2(rec["_cli"])
        hs_by[rec["_ck"]].append(rec)
        k = F.numk(d.get("num_raw"))
        if len(k) >= 5:
            by_num[k].append(rec)
    return hs_by, by_num


def fetch_extra_props(ids, props):
    out = {}
    ids = list(ids)
    for i in range(0, len(ids), 100):
        body = {"properties": props, "inputs": [{"id": x} for x in ids[i:i + 100]]}
        r = sync.req("POST", "/crm/v3/objects/deals/batch/read", json=body)
        if r.status_code in (200, 207):
            for d in r.json().get("results", []):
                out[d["id"]] = d.get("properties", {})
    return out


def match_grupo(g, hs_by, by_num, all_cards):
    ck, rn = g["emp_canon"], g["num"]
    cards = hs_by.get(ck, [])
    # 1) numero na empresa
    cand = [c for c in cards if not c["_used"] and rn and F.numk(c["num_raw"]) == rn]
    sig = "numero(empresa)"
    # 2) valor na empresa
    if not cand:
        cand = [c for c in cards if not c["_used"] and c["valor"] and F.vmatch(c["valor"], g["valor"])]
        sig = "valor(empresa)"
    # 3) numero GLOBAL com guarda: SO com valor corroborando (numeros compartilhados
    #    como WAC737/244057 geram match espurio se aceitar 'unico' sem valor).
    if not cand and len(rn) >= 5:
        glob = [c for c in by_num.get(rn, []) if not c["_used"]]
        val_ok = [c for c in glob if c["valor"] and F.vmatch(c["valor"], g["valor"])]
        if val_ok:
            cand, sig = val_ok, "numero(GLOBAL+valor)"
    # 4) valor exato + nome fuzzy GLOBAL
    if not cand:
        glob = [c for c in all_cards if not c["_used"] and c["valor"]
                and abs(c["valor"] - g["valor"]) < 0.01 and R.name_sig(g["nome_disp"], c["_cli"])]
        if glob:
            cand, sig = [glob[0]], "valor+nome(GLOBAL)"
    # 5) card vazio na empresa (cards vazios sao intercambiaveis -> preenche um por grupo)
    if not cand:
        empty = [c for c in cards if not c["_used"] and not c["valor"]]
        if empty:
            cand, sig = [empty[0]], "card_vazio(empresa)"
    if cand:
        c = cand[0]
        c["_used"] = True
        return dict(card=c, sig=sig, modo="match")
    return dict(card=None, sig="", modo="criar")


# ---------- decisao por campo ----------
def decide_acoes(g, mres):
    card, modo = mres.get("card"), mres["modo"]
    if modo == "criar":
        return "CRIAR", [{"campo": "(novo deal)", "old": "", "new": f"{g['nome_disp']} R${g['valor']:.0f}",
                          "reason": "sem card no HS -> criar (estagio Ganho + todos os campos)"}], "media"
    if modo == "tangle":
        return "TANGLE", [{"campo": "(alocacao)", "old": "", "new": "",
                           "reason": f"{mres.get('n_vazios', 0)} cards vazios na empresa, ambiguo"}], "baixa"

    did = card["id"]
    held = HOLDS.get(did, set()) | ({"*"} if g["emp_canon"] in HOLD_EMPRESAS else set())

    def h(campo):
        return "*" in held or campo in held

    acoes, bucket = [], "OK"

    def bump(b):  # eleva bucket sem rebaixar
        nonlocal bucket
        order = {"OK": 0, "PREENCHER-CAMPO": 1, "CORRIGIR-VALOR": 2, "FLAG-DIVERGENCIA": 3}
        if order[b] > order[bucket]:
            bucket = b

    # valor_do_aporte
    alvo, hsv = g["valor"], (card["valor"] or 0)
    if h("valor_do_aporte"):
        acoes.append({"campo": "valor_do_aporte", "old": hsv, "new": alvo,
                      "reason": "HOLD: " + (HOLD_EMPRESAS.get(g["emp_canon"]) or "conflito de valor (Ivan)")})
        bump("FLAG-DIVERGENCIA")
    elif not hsv:
        acoes.append({"campo": "valor_do_aporte", "old": 0, "new": alvo, "reason": "HS vazio"}); bump("PREENCHER-CAMPO")
    elif abs(hsv - alvo) < 0.01:
        pass
    elif F.vmatch(hsv, alvo):
        acoes.append({"campo": "valor_do_aporte", "old": hsv, "new": alvo, "reason": "planilha vence (vmatch)"}); bump("CORRIGIR-VALOR")
    else:
        acoes.append({"campo": "valor_do_aporte", "old": hsv, "new": alvo, "reason": "planilha vence (diff grande)"}); bump("CORRIGIR-VALOR")

    # tipo_de_proponente
    if not h("tipo_de_proponente"):
        ie = (g["interno_externo"] or "").strip().lower()
        cur = card.get("_tipo", "")
        if ie == "externo":
            tipo_new = "Externo"
        elif ie == "interno":
            tipo_new = sync._map_proponente_interno(g["proponente"])
        else:
            tipo_new = None
        if ie == "interno" and tipo_new is None:
            acoes.append({"campo": "tipo_de_proponente", "old": cur, "new": "(definir-interno)",
                          "reason": f"INTERNO mas proponente='{g['proponente']}' nao mapeia entidade (col invertida); tipo nao setado"})
        elif tipo_new and cur != tipo_new:
            acoes.append({"campo": "tipo_de_proponente", "old": cur, "new": tipo_new,
                          "reason": "coluna 'Projeto Interno?'"}); bump("PREENCHER-CAMPO" if not cur else "CORRIGIR-VALOR")

    # closedate (politica Ano de Fechamento)
    if not h("closedate") and g["closedate"]:
        hs_close = card.get("close", "")  # YYYY-MM
        if not hs_close:
            acoes.append({"campo": "closedate", "old": "", "new": g["closedate"], "reason": g["closedate_src"]}); bump("PREENCHER-CAMPO")
        elif hs_close[:7] != g["closedate"][:7]:
            acoes.append({"campo": "closedate", "old": hs_close, "new": g["closedate"],
                          "reason": "planilha vence (" + g["closedate_src"] + ")"}); bump("CORRIGIR-VALOR")

    # data de aporte == data de fechamento (closedate) == data de conversao do lead (decisao Bruno).
    # NAO escrever data_do_aporte (campo "Conta Movimentacao" = financeiro/Bia, conceito distinto).

    # lei + categoria
    if not h("valor_lei"):
        if g["fonte"]:
            cat, leif, leil, flag = F.fonte_to_lei(g["fonte"])
            if (card.get("cat") or "") != cat:
                acoes.append({"campo": "linha_de_imposto_categoria", "old": card.get("cat", ""), "new": cat,
                              "reason": leil + (" TEMA?" if flag else "")}); bump("PREENCHER-CAMPO" if not card.get("cat") else "CORRIGIR-VALOR")
            cur_lei = R.hs_num((card.get("_leis") or {}).get(leif))
            if abs(cur_lei - alvo) >= 0.01:
                acoes.append({"campo": leif, "old": cur_lei or "", "new": alvo, "reason": leil}); bump("PREENCHER-CAMPO" if not cur_lei else "CORRIGIR-VALOR")
        elif card.get("cat"):
            pass  # HS ja tem categoria -> corrobora, nao mexe (G5)
        else:
            acoes.append({"campo": "valor_lei_*", "old": "", "new": "?",
                          "reason": "SEM-FONTE (planilha+HS sem lei) -> pedir fonte ao Ivan (nao inventar)"}); bump("PREENCHER-CAMPO")

    # numero_do_projeto (fill-if-empty)
    if not h("numero_do_projeto") and g["num"] and not card.get("num"):
        acoes.append({"campo": "numero_do_projeto", "old": "", "new": g["num_raw"], "reason": "fill-if-empty (nivel-projeto)"}); bump("PREENCHER-CAMPO")

    # nome_do_proponente (fill-if-empty; nunca sobrescreve free-text)
    if not h("nome_do_proponente") and g["proponente"] and not card.get("prop"):
        acoes.append({"campo": "nome_do_proponente", "old": "", "new": g["proponente"], "reason": "fill-if-empty"}); bump("PREENCHER-CAMPO")

    # inconsistencias intra-grupo viram flag (nao bloqueiam)
    if g["inconsist_ie"] or g["inconsist_fonte"]:
        acoes.append({"campo": "(grupo)", "old": "", "new": "",
                      "reason": "parcelas inconsistentes: " + ("interno/externo " if g["inconsist_ie"] else "") + ("fonte" if g["inconsist_fonte"] else "")}); bump("FLAG-DIVERGENCIA")

    return bucket, acoes, mres.get("sig", "")


# ---------- apply (gated) ----------
def acoes_to_props(acoes, only=None):
    allow = set()
    for tok in (only or []):
        allow |= ONLY_MAP.get(tok, set())
    props = {}
    for a in acoes:
        campo, new = a["campo"], a["new"]
        is_lei = campo.startswith("valor_lei_") and campo != "valor_lei_*"
        if campo not in REAL_PROPS and not is_lei:
            continue
        if new in ("", "?", None) or str(new).startswith("("):
            continue
        if only:
            if not ((campo in allow) or (is_lei and "lei" in only) or (campo == "linha_de_imposto_categoria" and "lei" in only)):
                continue
        if campo == "valor_do_aporte" or is_lei:
            props[campo] = R._fmt_val(float(new))
        elif campo in ("closedate", "data_do_aporte"):
            ep = iso_to_epoch_ms(new)
            if ep:
                props[campo] = ep
        else:
            props[campo] = new
    return props


def apply_fields(final, by_id, only, execute):
    mode = "EXECUTE (PATCH real)" if execute else "DRY-RUN"
    print(f"\n=== APPLY-FIELDS ({mode}){' only=' + ','.join(only) if only else ''} ===")
    log, patches, skip = [], 0, 0
    for row in final:
        card = row["m"].get("card")
        if not card:
            continue
        # GUARDA DE REGRESSAO CRIAPE: so escreve em Incentivador.
        if card.get("pipe") != INCENT_PIPE:
            print(f"  [SKIP] {card['id']} pipe={card.get('pipe')} != {INCENT_PIPE} (nao tocar CRIAPE)")
            skip += 1
            continue
        props = acoes_to_props(row["acoes"], only)
        if not props:
            continue
        antes = {"valor_do_aporte": card.get("valor"), "closedate": card.get("close"),
                 "tipo_de_proponente": card.get("_tipo"), "data_do_aporte": card.get("_data_aporte"),
                 "numero_do_projeto": card.get("num_raw"), "nome_do_proponente": card.get("prop"),
                 "linha_de_imposto_categoria": card.get("cat")}
        entry = {"deal_id": card["id"], "cliente": row["g"]["nome_disp"], "bucket": row["bucket"],
                 "antes": antes, "props": props}
        log.append(entry)
        if execute:
            rr = sync.req("PATCH", f"/crm/v3/objects/deals/{card['id']}", json={"properties": props})
            if rr.status_code == 200:
                patches += 1
            else:
                print(f"  [erro] {card['id']}: {rr.status_code} {rr.text[:140]}")
            time.sleep(0.05)
        else:
            patches += 1
    for e in log[:30]:
        print(f"  deal {e['deal_id']} ({e['cliente'][:24]}) [{e['bucket']}]: {e['props']}")
    print(f"\n  {'PATCHED' if execute else 'WOULD-PATCH'}={patches} | skip_pipe={skip} | total={len(log)}")
    _write_log("apply_espelhamento_log.jsonl", log)


def _write_log(name, log):
    path = os.path.join(SNAP_DIR, name)
    try:
        os.makedirs(SNAP_DIR, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for e in log:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        print(f"  log: {path}")
    except Exception as e:
        print(f"  [log] falhou: {e}")


# ---------- criar / excluir (gated) ----------
def canonical_company_ids(matched_cards):
    """canonico -> company_id (reusa a company ja associada a um card existente do mesmo canonico)."""
    ids = [c["id"] for c in matched_cards]
    d2c = sync.fetch_associated_companies(ids) if ids else {}
    ck_to_cid = {}
    for c in matched_cards:
        cid = d2c.get(c["id"])
        if cid and c["_ck"] not in ck_to_cid:
            ck_to_cid[c["_ck"]] = cid
    return ck_to_cid


def build_deal_payload(g):
    props = {
        "dealname": (f"{g['nome_disp']} - {g.get('projeto') or g['num_raw']}").strip(" -")[:200],
        "pipeline": INCENT_PIPE, "dealstage": GANHO_STAGE,
        "valor_do_aporte": R._fmt_val(g["valor"]), "amount": R._fmt_val(g["valor"]),
    }
    if g["closedate"]:
        props["closedate"] = iso_to_epoch_ms(g["closedate"])
    if g["num"]:
        props["numero_do_projeto"] = g["num_raw"]
    if g["proponente"]:
        props["nome_do_proponente"] = g["proponente"]
    ie = (g["interno_externo"] or "").lower()
    if ie == "externo":
        props["tipo_de_proponente"] = "Externo"
    elif ie == "interno":
        ent = sync._map_proponente_interno(g["proponente"])
        if ent:
            props["tipo_de_proponente"] = ent
    if g["fonte"]:
        cat, leif, leil, flag = F.fonte_to_lei(g["fonte"])
        props["linha_de_imposto_categoria"] = cat
        props[leif] = R._fmt_val(g["valor"])
    return props


def create_missing(final, ck_to_cid, reverse, execute, only_first=False):
    criar = sorted([r for r in final if r["m"]["modo"] == "criar"], key=lambda r: -r["g"]["valor"])
    # REPURPOSE: CRIAR cujo valor bate EXATO um card reverse (cross-name; ex.: MedWriters 1.46M <-> RMED Point 1.46M).
    # Reaproveita o card existente (preserva deal_id/historico) em vez de duplicar+excluir.
    rev_avail = [c for c in reverse if c.get("pipe") == INCENT_PIPE]
    repurpose, rest = [], []
    for r in criar:
        g = r["g"]
        m = next((c for c in rev_avail if not c.get("_consumed") and abs((c["valor"] or 0) - g["valor"]) < 0.01), None)
        if m:
            m["_consumed"] = True
            repurpose.append((g, m))
        else:
            rest.append(r)
    if only_first:
        rest, repurpose = rest[:1], []
    mode = "EXECUTE (real)" if execute else "DRY-RUN"
    print(f"\n=== CREATE/REPURPOSE ({mode}){' [first only]' if only_first else ''} ===")
    log, created, repurp = [], 0, 0
    for g, card in repurpose:
        payload = build_deal_payload(g)
        payload.pop("pipeline", None); payload.pop("dealstage", None)  # nao mexe estagio do card existente
        entry = {"acao": "repurpose", "deal_id": card["id"], "cliente": g["nome_disp"], "valor": g["valor"],
                 "era": card.get("dn"), "props": payload}
        if execute:
            rr = sync.req("PATCH", f"/crm/v3/objects/deals/{card['id']}", json={"properties": payload})
            if rr.status_code == 200:
                repurp += 1
                print(f"  [repurpose] {card['id']} '{(card.get('dn') or '')[:28]}' -> {g['nome_disp'][:22]} R${g['valor']:.0f}")
            else:
                print(f"  [erro repurpose] {card['id']}: {rr.status_code} {rr.text[:140]}")
            time.sleep(0.1)
        else:
            print(f"  WOULD-REPURPOSE {card['id']} '{(card.get('dn') or '')[:28]}' -> {g['nome_disp'][:22]} R${g['valor']:.0f}")
        log.append(entry)
    for r in rest:
        g = r["g"]
        payload = build_deal_payload(g)
        cid = ck_to_cid.get(g["emp_canon"])
        body = {"properties": payload}
        if cid:
            body["associations"] = [{"to": {"id": cid},
                                     "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 5}]}]
        entry = {"acao": "create", "cliente": g["nome_disp"], "numero": g["num_raw"], "valor": g["valor"],
                 "company_id": cid, "props": payload}
        if execute:
            rr = sync.req("POST", "/crm/v3/objects/deals", json=body)
            if rr.status_code in (200, 201):
                created += 1
                entry["new_deal_id"] = rr.json().get("id")
                print(f"  [criado] {entry['new_deal_id']} {g['nome_disp'][:22]} R${g['valor']:.0f} company={cid}")
            else:
                print(f"  [erro create] {g['nome_disp'][:22]}: {rr.status_code} {rr.text[:160]}")
            time.sleep(0.1)
        else:
            print(f"  WOULD-CREATE {g['nome_disp'][:22]} num={g['num_raw'][:16]} R${g['valor']:.0f} company={cid} stage=Ganho")
            print(f"        props={payload}")
        log.append(entry)
    sem_co = sum(1 for e in log if e["acao"] == "create" and not e.get("company_id"))
    print(f"  {'DONE' if execute else 'PLAN'}: repurpose={repurp if execute else len(repurpose)} | "
          f"create={created if execute else len(rest)} | create sem company={sem_co}")
    _write_log("create_espelhamento_log.jsonl", log)
    return log


def delete_orphans(reverse, execute):
    cands = [c for c in reverse if c.get("pipe") == INCENT_PIPE and not c.get("_consumed")]
    mode = "EXECUTE (DELETE real)" if execute else "DRY-RUN"
    print(f"\n=== DELETE orfaos/dups 2025-26 ({mode}) {len(cands)} cards ===")
    log = []
    for c in cands:
        entry = {"deal_id": c["id"], "dn": c.get("dn"), "valor": c["valor"], "close": c.get("close")}
        if execute:
            rr = sync.req("DELETE", f"/crm/v3/objects/deals/{c['id']}")
            print(f"  [del] {c['id']} {repr((c.get('dn') or '')[:36])} R${c['valor']:.0f} -> {rr.status_code}")
            time.sleep(0.1)
        else:
            print(f"  WOULD-DELETE {c['id']} {repr((c.get('dn') or '')[:36])} R${c['valor']:.0f} close={c.get('close')}")
        log.append(entry)
    _write_log("delete_espelhamento_log.jsonl", log)
    return log


# ---------- tabela-mestre ----------
def write_master(gc, final, reverse):
    instr = ("INSTRUCAO: reconciliacao planilha OFICIAL Vendas_25_26 (08/06) x HubSpot Incentivador. "
             "Planilha vence. Confira deal_id/acao; cole correcao em escolha_bruno. "
             "CRIAR-CANDIDATO = pode ja existir no HS (gap de nome) -> confirmar antes de criar. "
             "FLAG/TANGLE = olho humano. REVERSE = card-com-valor 2025/26 sem linha na planilha.")
    header = ["cliente", "emp_canonica", "numero", "deal_id", "link", "interno_externo",
              "tipo_proponente_novo", "proponente_planilha", "closedate", "valor_planilha",
              "valor_hs", "lei", "categoria", "n_parcelas", "acao", "confianca", "status",
              "acoes_por_campo", "escolha_bruno"]
    rows = []
    for row in sorted(final, key=lambda x: (-x["g"]["valor"])):
        g, m = row["g"], row["m"]
        card = m.get("card") or {}
        ie = (g["interno_externo"] or "").lower()
        tipo = "Externo" if ie == "externo" else (sync._map_proponente_interno(g["proponente"]) or "?(interno)")
        cat = leil = ""
        if g["fonte"]:
            cat, _, leil, _ = F.fonte_to_lei(g["fonte"])
        status = []
        if g["tangle_num"]:
            status.append("numero-compartilhado")
        if g["inconsist_ie"] or g["inconsist_fonte"]:
            status.append("parcelas-inconsistentes")
        if card.get("id") in HOLDS or g["emp_canon"] in HOLD_EMPRESAS:
            status.append("HOLD")
        if g.get("multi_cliente"):
            status.append("multi-cliente-celula")
        if any(a["campo"] == "valor_lei_*" for a in row["acoes"]):
            status.append("lei-SEM-FONTE")
        acoes_str = "; ".join(f"{a['campo']}:{a['old']}->{a['new']}" for a in row["acoes"] if a["campo"] not in ("(grupo)",))[:480]
        rows.append([
            g["nome_disp"], g["emp_canon"], g["num_raw"], card.get("id", "CRIAR"),
            CARD_LINK.format(card["id"]) if card.get("id") else "", g["interno_externo"],
            tipo, g["proponente"], g["closedate"], f"{g['valor']:.2f}",
            f"{(card.get('valor') or 0):.2f}" if card else "", leil, cat, g["n_parcelas"],
            row["bucket"], row["conf"], " | ".join(status), acoes_str, "",
        ])
    rows.append([""] * len(header))
    rows.append(["=== REVERSE: card-com-valor 2025/26 SEM linha na planilha (esperado ~0) ==="] + [""] * (len(header) - 1))
    for c in sorted(reverse, key=lambda x: -x["valor"]):
        rows.append(["(reverse)", c["_ck"], c.get("num_raw", ""), c["id"], CARD_LINK.format(c["id"]),
                     "", "", c.get("prop", ""), c.get("close", ""), "", f"{c['valor']:.2f}", "", c.get("cat", ""),
                     "", "REVISAR", "", "ruido/venda-fora-planilha?", (c.get("dn") or "")[:60], ""])
    out = [[instr] + [""] * (len(header) - 1), header] + rows
    sh = gc.open_by_key(DASH_SHEET)
    try:
        ws = sh.worksheet(OUT_ABA)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=OUT_ABA, rows=len(out) + 10, cols=len(header))
    ws.update(values=out, range_name="A1")
    print(f"  OK aba '{OUT_ABA}' escrita ({len(out)} linhas).")


# ---------- battery ----------
def verify_battery(rows, grupos, final, reverse):
    print("\n=== BATTERY ===")
    tot = round(sum(r["valor"] for r in rows), 2)
    by_ano = defaultdict(float)
    for r in rows:
        by_ano[r["ano_fechamento"]] += r["valor"]
    ok_tot = abs(tot - TOTAL_ESPERADO) <= 1.0 and len(rows) == N_LINHAS_ESPERADO
    print(f"  [{'PASS' if ok_tot else 'FAIL'}] loader: {len(rows)} linhas (esp {N_LINHAS_ESPERADO}), "
          f"Sigma R$ {tot:,.2f} (esp {TOTAL_ESPERADO:,.2f})")
    for ano, alvo in ANO_TOTAIS.items():
        v = round(by_ano.get(ano, 0), 2)
        print(f"        ano {ano}: R$ {v:,.2f} (esp {alvo:,.2f}) {'OK' if abs(v - alvo) <= 1.0 else 'DIFERE'}")
    print(f"  [INFO] grupos apos cnk: {len(grupos)}")
    matched = [r for r in final if r["m"].get("card")]
    sigma_hs = sum((r["m"]["card"].get("valor") or 0) for r in matched)
    print(f"  [INFO] Sigma HS dos casados hoje: R$ {sigma_hs:,.2f}  (alvo pos-apply = {TOTAL_ESPERADO:,.2f} menos HOLD)")
    print(f"  [{'PASS' if len(reverse) <= 1 else 'WARN'}] reverse (card-valor 2025/26 sem planilha): {len(reverse)}")
    for c in reverse:
        print(f"        REVERSE {c['id']} {repr((c.get('dn') or '')[:40])} R$ {c['valor']:,.0f} {c.get('close')}")
    print("  [INFO] guarda CRIAPE: este modulo so escreve em pipeline 'default' (Proponente intacto).")


# ---------- orquestracao ----------
def run(policy="fechamento", sample=0, write_tab=True, refresh=False,
        apply=False, only=None, execute=False, create=False, create_first=False, delete=False):
    gc = sync.get_sheets_client()
    print("Carregando planilha oficial...")
    rows = load_oficial(gc, policy)
    grupos = group_oficial(rows)
    print("Carregando deals HubSpot...")
    by_id, pool = load_deals_cached(refresh=refresh)
    hs_by, by_num = build_hs_index(pool)
    all_cards = [c for lst in hs_by.values() for c in lst]

    resultados = [(g, match_grupo(g, hs_by, by_num, all_cards)) for g in sorted(grupos, key=lambda x: -x["valor"])]
    matched_ids = [m["card"]["id"] for _, m in resultados if m.get("card")]
    LEI_PROPS = ["valor_lei_rouanet", "valor_lei_da_cultura", "valor_lei_da_cultura_municipal",
                 "valor_lei_do_esporte", "valor_lei_do_esporte_estadual", "valor_lei_da_crianca_e_do_adolescente"]
    extra = fetch_extra_props(matched_ids, ["tipo_de_proponente", "data_do_aporte"] + LEI_PROPS)
    for _, m in resultados:
        if m.get("card"):
            p = extra.get(m["card"]["id"]) or {}
            m["card"]["_tipo"] = p.get("tipo_de_proponente", "") or ""
            m["card"]["_data_aporte"] = to_iso_date(p.get("data_do_aporte"))
            m["card"]["_leis"] = {k: p.get(k) for k in LEI_PROPS}

    final = []
    for g, m in resultados:
        bucket, acoes, conf = decide_acoes(g, m)
        final.append(dict(g=g, m=m, bucket=bucket, acoes=acoes, conf=conf))
    reverse = [c for c in all_cards if c["valor"] and not c["_used"] and (c.get("close") or "")[:4] in ("2025", "2026")]

    tally = Counter(r["bucket"] for r in final)
    print("\n=== PLANO POR BUCKET ===")
    for b, n in tally.most_common():
        print(f"  {n:3d}  {b}")

    if sample:
        print(f"\n=== AMOSTRA (--sample {sample} por bucket) ===")
        for b in ["CORRIGIR-VALOR", "PREENCHER-CAMPO", "FLAG-DIVERGENCIA", "CRIAR", "TANGLE", "OK"]:
            ex = [r for r in final if r["bucket"] == b][:sample]
            for r in ex:
                g, card = r["g"], r["m"].get("card") or {}
                print(f"  [{b}] {g['nome_disp'][:26]} num={g['num_raw'][:18]} deal={card.get('id','CRIAR')} "
                      f"R${g['valor']:.0f} conf={r['conf']}")
                for a in r["acoes"]:
                    print(f"        - {a['campo']}: {a['old']} -> {a['new']}  ({a['reason']})")

    verify_battery(rows, grupos, final, reverse)

    if write_tab:
        print("\nEscrevendo tabela-mestre...")
        write_master(gc, final, reverse)

    if apply:
        apply_fields(final, by_id, only, execute)
    if create or create_first:
        matched_cards = [m["card"] for _, m in resultados if m.get("card")]
        ck_to_cid = canonical_company_ids(matched_cards)
        create_missing(final, ck_to_cid, reverse, execute, only_first=create_first)
    if delete:
        delete_orphans(reverse, execute)

    return final, reverse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", choices=["fechamento", "aporte"], default="fechamento")
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--no-write-tab", action="store_true")
    ap.add_argument("--refresh", action="store_true", help="ignora cache de deals")
    ap.add_argument("--apply-fields", action="store_true")
    ap.add_argument("--only", default="", help="csv: valor,tipo,closedate,lei,numero,proponente")
    ap.add_argument("--create", action="store_true", help="cria os cards faltantes (lote)")
    ap.add_argument("--create-first", action="store_true", help="cria so 1 card (maior valor) p/ inspecao")
    ap.add_argument("--delete", action="store_true", help="exclui orfaos/dups 2025-26 (reverse)")
    ap.add_argument("--execute", action="store_true", help="obrigatorio p/ qualquer escrita no CRM")
    args = ap.parse_args()
    only = [t.strip() for t in args.only.split(",") if t.strip()] if args.only else None
    if (args.apply_fields or args.create or args.create_first or args.delete) and not args.execute:
        print(">> DRY-RUN (sem --execute): nada sera escrito no CRM.")
    run(policy=args.policy, sample=args.sample, write_tab=not args.no_write_tab,
        refresh=args.refresh, apply=args.apply_fields, only=only, execute=args.execute,
        create=args.create, create_first=args.create_first, delete=args.delete)


if __name__ == "__main__":
    main()
