"""Resolver multi-campo planilha Ivan <-> HubSpot (Sprint 1.5).

Substitui o matching só-por-nome (mapa_nomes) e o reconciliador antigo
(reconciliacao_planilha_cards.py, só "Controle de Vendas", só nome+valor).

Correlaciona CADA linha das abas de deal do Ivan com o deal real do HubSpot
usando MÚLTIPLOS sinais, com 2 fontes de match:
  1. BRIDGE curado: aba `classificacao_match_ivan` (50 deals Match ganhos com
     deal_id + valor já curados) -> dá o deal_id direto (alta confiança).
  2. FALLBACK multi-sinal: para linhas fora dos 50, scoring restrito ao pipeline
     Incentivador (valor ±2% = identificador; numero_do_projeto = nível-projeto,
     só corrobora; nome token-overlap; categoria IR/ICMS/ISS; data = desempate).

Resolve o VALOR por deal (planilha = fonte da verdade financeira; HS Match quase
sempre vazio) e flag de CONFLITO quando planilha != HS/amount/classificacao (>2%).
Cobre os 2 sentidos (forward: cada linha; reverse: cada deal curado sem linha).

Saída: 1 aba `reconciliacao_planilha_cards` (sem proliferar). Opcional: corrige
`mapa_nomes` (col M) nos ALTA e faz backfill GATED no CRM (fill-if-empty).

Uso:
    python resolver_planilha_multicampo.py                 # dry-run (só imprime)
    python resolver_planilha_multicampo.py --write-tab     # escreve aba + corrige mapa_nomes
    python resolver_planilha_multicampo.py --backfill       # dry-run dos PATCHes (amostra)
    python resolver_planilha_multicampo.py --backfill-execute  # PATCH real (após sign-off)
"""
import os
import re
import sys
from collections import defaultdict
from difflib import SequenceMatcher

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ENV_PATH = r"C:\Users\bruno\.brada-secrets\hubspot.env"
with open(ENV_PATH, encoding="utf-8-sig") as fh:
    for line in fh:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sync  # noqa: E402

IVAN_SHEET = "1FbvQqb84RXzZAeIqvCymrnLry7VV7xihrie66WUVnIg"
DASH_SHEET = "1bBGfZIkjuqBBQL9U79cMWQUgz9FLrbIKm2SObAXEKF8"
OUT_ABA = "reconciliacao_planilha_cards"
CLASSIF_ABA = "classificacao_match_ivan"
MAPA_ABA = "mapa_nomes"
# Abas de deal do Ivan (header-name mapping; layouts diferem). Reuniões/Maio_* vazias hoje.
DEAL_TABS = ["Controle de Vendas", "MATCH - Mar_Abr_26", "Mar_Abr_Vendas", "Maio_Vendas", "Maio_MATCH"]
INCENTIVADOR_PIPE = "default"
VALUE_TOL = 0.02


# ---------- helpers ----------
def parse_brl(s):
    """Parser de valor BRASILEIRO (planilha): '2.575.309,88' -> 2575309.88. NÃO usar em
    valores do HubSpot (que vêm como decimal limpo '2575309.88' — usar hs_num)."""
    s = re.sub(r"[^\d,.-]", "", str(s or ""))
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def hs_num(s):
    """Parser de número do HubSpot (decimal limpo: '2575309.88', '2400000', '')."""
    s = str(s or "").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def numkey(s):
    k = re.sub(r"[^0-9a-z]", "", str(s or "").lower())
    return k if len(k) >= 4 else ""


# stopwords de fonte/lei (não são nome de empresa) — evitam cross-match por "iss"/"cultura".
# iss/imposto/datas/forma-societária já saem via _norm_key2.
_STOP = {"icms", "cultura", "esporte", "lei", "fundo", "incentivo"}


def toks(s):
    return set(t for t in sync._norm_key2(s).split() if len(t) >= 3 and t not in _STOP)


def name_sig(a, b):
    """Sinal de nome: token-overlap OU fuzzy de caractere (pega typo Galoitto~Galiotto).
    Retorna 'nome' (forte), 'nome~' (fraco) ou None."""
    ta, tb = toks(a), toks(b)
    if not ta or not tb:
        return None
    if len(ta & tb) / len(ta) >= 0.5:
        return "nome"
    best = max((SequenceMatcher(None, x, y).ratio() for x in ta for y in tb), default=0)
    if best >= 0.82:
        return "nome"
    if (ta & tb) or best >= 0.70:
        return "nome~"
    return None


def val_close(a, b, tol=VALUE_TOL):
    return bool(a) and bool(b) and abs(a - b) <= tol * max(a, b)


def g(r, i):
    return r[i].strip() if (i is not None and i < len(r)) else ""


# ---------- carregar deals (HubSpot) ----------
def load_deals():
    deals = sync.fetch_all_deals()
    d2c = sync.fetch_associated_companies([d["id"] for d in deals])
    comps = sync.fetch_companies(list(set(d2c.values())))

    def cprop(cid, k):
        c = comps.get(str(cid)) if isinstance(comps, dict) else None
        return ((c or {}).get("properties", {}) or {}).get(k, "") if c else ""

    by_id = {}
    inc_pool = []
    for d in deals:
        p = d["properties"]
        cid = d2c.get(d["id"])
        rec = dict(
            id=d["id"], dn=p.get("dealname", "") or "", cn=cprop(cid, "name") or "",
            cnpj=cprop(cid, "cnpj") or "",
            valor=hs_num(p.get("valor_do_aporte")), amount=hs_num(p.get("amount")),
            num=numkey(p.get("numero_do_projeto")), num_raw=(p.get("numero_do_projeto") or "").strip(),
            cat=(p.get("linha_de_imposto_categoria") or "").upper(),
            close=(p.get("closedate") or "")[:7], prop=(p.get("nome_do_proponente") or "").strip(),
            pipe=p.get("pipeline", ""), produto=(p.get("produto") or "").strip(),
        )
        by_id[d["id"]] = rec
        if rec["pipe"] == INCENTIVADOR_PIPE:
            inc_pool.append(rec)
    return by_id, inc_pool


# ---------- carregar classificacao_match_ivan (bridge curado) ----------
def load_classificacao(gc):
    v = gc.open_by_key(DASH_SHEET).worksheet(CLASSIF_ABA).get_all_values()
    out = []
    for r in v[2:]:  # row0 = instrução, row1 = header
        did = g(r, 0)
        if not re.fullmatch(r"\d{8,}", did):
            continue
        out.append(dict(did=did, cli=g(r, 2), valor=parse_brl(g(r, 3)),
                        proj=g(r, 4), close=g(r, 5), ie=g(r, 7)))
    return out


# ---------- carregar planilha (3 abas, header-name) ----------
def colidx(hdr, *names):
    for i, h in enumerate(hdr):
        hl = h.strip().lower()
        if any(n in hl for n in names):
            return i
    return None


def load_planilha(gc):
    sh = gc.open_by_key(IVAN_SHEET)
    titles = {w.title for w in sh.worksheets()}
    rows = []
    for tab in DEAL_TABS:
        if tab not in titles:
            continue
        v = sh.worksheet(tab).get_all_values()
        if not v:
            continue
        hdr = v[0]
        ic = colidx(hdr, "cliente", "lead")
        ifo = colidx(hdr, "fonte")
        ipr = colidx(hdr, "proponente")
        ipj = colidx(hdr, "projeto")
        inu = colidx(hdr, "numero do projeto", "número do projeto", "numero")
        iv = colidx(hdr, "valor")
        idt = colidx(hdr, "data do aporte", "data")
        stream = "MATCH" if "match" in tab.lower() else ("Elaboracao" if "elabora" in tab.lower() else "Vendas")
        for r in v[1:]:
            cli = g(r, ic)
            if not cli or cli.lower() in ("total", "totais"):
                continue
            rows.append(dict(
                tab=tab, stream=stream, cli=cli, fonte=g(r, ifo), prop=g(r, ipr),
                proj=g(r, ipj), num_raw=g(r, inu), num=numkey(g(r, inu)),
                valor=parse_brl(g(r, iv)), data=g(r, idt),
            ))
    # dedupe por (nome, valor, numero) -> mantém parcelas/valores distintos (Matific 4x)
    seen, dedup = set(), []
    for r in rows:
        key = (sync._norm_key(r["cli"]), round(r["valor"]), r["num"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)
    return dedup


# ---------- matching ----------
def best_classificacao(pl, classif):
    """Bridge: melhor deal curado p/ a linha. EXIGE sinal de nome (valor sozinho colide);
    valor só desempata entre mesmos-nome. Retorna (entry, signals, ambiguous)."""
    cands = []
    for c in classif:
        ns = name_sig(pl["cli"], c["cli"])
        if not ns:
            continue
        sc = 2 if ns == "nome" else 1
        sig = [ns]
        if val_close(pl["valor"], c["valor"]):
            sc += 2
            sig.append("valor")
        cands.append((sc, c, sig))
    if not cands:
        return None, [], False
    cands.sort(key=lambda x: -x[0])
    bsc, bc, bsig = cands[0]
    ambiguous = len([x for x in cands if x[0] == bsc]) > 1 and "valor" not in bsig
    return bc, bsig, ambiguous


def best_fallback(pl, pool):
    """Fallback multi-sinal sobre o pipeline Incentivador. Retorna (deal, signals)."""
    best, bscore, bsig = None, 0, []
    for d in pool:
        sc, sig = 0, []
        if val_close(pl["valor"], d["valor"]) or val_close(pl["valor"], d["amount"]):
            sc += 50
            sig.append("valor")
        if pl["num"] and d["num"] and pl["num"] == d["num"]:
            sc += 50
            sig.append("numero")  # nível-projeto: corrobora
        ns = name_sig(pl["cli"], d["cn"]) or name_sig(pl["cli"], d["dn"])
        if ns:
            sc += 30 if ns == "nome" else 12
            sig.append(ns)
        f = pl["fonte"].upper()
        for cat in ("ICMS", "ISS", "IR"):
            if cat in f and d["cat"] == cat:
                sc += 10
                sig.append("cat")
                break
        if sc > bscore:
            bscore, best, bsig = sc, d, sig
    return (best, bsig) if bscore > 0 else (None, [])


def confianca(fonte_match, sig, ambiguous=False):
    has_nome = "nome" in sig
    has_hard = ("valor" in sig) or ("numero" in sig)
    if fonte_match == "bridge":
        if ambiguous:
            return "MEDIA"            # vários mesmos-nome sem valor pra desempatar
        if has_nome:
            return "ALTA"             # nome forte único = deal_id curado
        return "MEDIA"                # só nome~ (fuzzy fraco)
    # fallback (sem deal curado): nome é obrigatório p/ confiança alta;
    # numero/cat sozinhos são nível-projeto (colidem) -> nunca passam de BAIXA.
    if has_nome and ("valor" in sig or "numero" in sig):
        return "ALTA"
    if ("nome~" in sig) and ("valor" in sig or "numero" in sig):
        return "MEDIA"
    if has_nome:
        return "MEDIA"
    if has_hard or "nome~" in sig:
        return "BAIXA"
    return "GAP"


def resolve_row(pl, classif, pool, by_id):
    c, csig, amb = best_classificacao(pl, classif)
    if c:
        d = by_id.get(c["did"], {})
        deal_id, fonte = c["did"], "bridge"
        sig, deal = csig, d
        classif_val = c["valor"]
        conf = confianca(fonte, sig, amb)
    else:
        d, fsig = best_fallback(pl, pool)
        if not d:
            return dict(deal_id="", deal=None, conf="GAP", fonte="", sinais=[],
                        valor_resolvido=pl["valor"], conflito="", gap_motivo="sem candidato")
        deal_id, fonte, sig, deal = d["id"], "fallback", fsig, d
        classif_val = 0.0
        conf = confianca(fonte, sig)
    # valor resolvido + conflito
    hs_vals = [x for x in (deal.get("valor"), deal.get("amount"), classif_val) if x]
    conflito = ""
    if pl["valor"] and hs_vals:
        if not any(val_close(pl["valor"], x) for x in hs_vals):
            conflito = f"planilha {pl['valor']:.0f} != HS {max(hs_vals):.0f}"
    valor_resolvido = pl["valor"] or (deal.get("valor") or deal.get("amount") or classif_val)
    return dict(deal_id=deal_id, deal=deal, conf=conf, fonte=fonte, sinais=sig,
                valor_resolvido=valor_resolvido, conflito=conflito, gap_motivo="")


# ---------- main ----------
def main():
    write_tab = "--write-tab" in sys.argv
    backfill = "--backfill" in sys.argv or "--backfill-execute" in sys.argv
    backfill_exec = "--backfill-execute" in sys.argv

    gc = sync.get_sheets_client()
    print("Carregando deals do HubSpot...")
    by_id, inc_pool = load_deals()
    classif = load_classificacao(gc)
    plan = load_planilha(gc)
    print(f"deals={len(by_id)} | Incentivador_pool={len(inc_pool)} | "
          f"classificacao={len(classif)} | planilha_rows(dedupe)={len(plan)}\n")

    # FORWARD: cada linha da planilha
    results = []
    used_deal_ids = set()
    stat = defaultdict(int)
    for pl in plan:
        r = resolve_row(pl, classif, inc_pool, by_id)
        results.append((pl, r))
        if r["deal_id"]:
            used_deal_ids.add(r["deal_id"])
        stat[r["conf"]] += 1
        if r["conflito"]:
            stat["_conflito"] += 1

    print(f"=== FORWARD ({len(results)} linhas) | confiança: {dict(stat)} ===")
    for pl, r in sorted(results, key=lambda x: (x[0]["stream"], x[0]["cli"])):
        d = r["deal"] or {}
        tgt = f"{r['deal_id']} {(d.get('cn') or d.get('dn') or '')[:24]}" if r["deal_id"] else "(GAP)"
        cf = f" CONFLITO[{r['conflito']}]" if r["conflito"] else ""
        print(f"  {pl['cli'][:22]:22s} {pl['stream']:9s} {pl['valor']:>11,.0f} -> {tgt:30s} "
              f"{r['conf']:5s} [{r['fonte']}] {sorted(r['sinais'])}{cf}")

    # REVERSE: deals curados sem linha na planilha
    rev = [c for c in classif if c["did"] not in used_deal_ids]
    print(f"\n=== REVERSE: {len(rev)} deals curados SEM linha na planilha ===")
    for c in rev:
        d = by_id.get(c["did"], {})
        print(f"  {c['did']} {c['cli'][:26]:26s} valor_classif={c['valor']:>11,.0f} "
              f"hs_valor={d.get('valor', 0):>11,.0f} proj={c['proj'][:22]}")

    # casos-teste
    print("\n--- CASOS-TESTE ---")
    for pl, r in results:
        if any(x in pl["cli"].lower() for x in ("galoitto", "jc risco", "inove", "corrida")):
            d = r["deal"] or {}
            print(f"  {pl['cli']!r} ({pl['stream']}) -> {r['deal_id']} "
                  f"{(d.get('cn') or d.get('dn') or '')!r} {r['conf']} {r['sinais']} "
                  f"conflito={r['conflito']!r}")

    if write_tab:
        _write_resolution_tab(gc, results, rev, by_id)
        _fix_mapa_nomes(gc, results)
    else:
        print("\n  DRY-RUN: rode --write-tab pra escrever a aba + corrigir mapa_nomes.")

    if backfill:
        _backfill_crm(results, backfill_exec)


def _write_resolution_tab(gc, results, rev, by_id):
    header_instr = ("INSTRUÇÃO: cada linha planilha ligada ao deal real (bridge=classificacao_match_ivan curado; "
                    "fallback=multi-sinal). Confira deal_id; se errado, cole o certo em escolha_bruno. "
                    "CONFLITO = valor planilha != HS (não sobrescrito). REVERSE = deals curados sem linha na planilha.")
    header = ["origem_aba", "stream", "cliente_planilha", "valor_planilha", "fonte_planilha", "numproj_planilha",
              "deal_id", "company_hs", "valor_hs", "amount_hs", "valor_resolvido", "numproj_hs", "cat_hs",
              "proponente_hs", "confianca", "fonte_match", "sinais", "conflito", "gap_motivo", "acao", "escolha_bruno"]
    rows = []
    for pl, r in results:
        d = r["deal"] or {}
        acao = ("confirmar" if r["conf"] == "ALTA" else
                "conflito-valor" if r["conflito"] else
                "criar-deal/investigar" if r["conf"] == "GAP" else "revisar")
        rows.append([
            pl["tab"], pl["stream"], pl["cli"], f"{pl['valor']:.0f}", pl["fonte"], pl["num_raw"],
            r["deal_id"], d.get("cn", "") or d.get("dn", ""), f"{d.get('valor', 0):.0f}", f"{d.get('amount', 0):.0f}",
            f"{r['valor_resolvido']:.0f}", d.get("num_raw", ""), d.get("cat", ""), d.get("prop", ""),
            r["conf"], r["fonte"], ",".join(sorted(r["sinais"])), r["conflito"], r["gap_motivo"], acao, "",
        ])
    # separador + reverse
    rows.append([""] * len(header))
    rows.append(["=== REVERSE: deals curados (classificacao) SEM linha na planilha ==="] + [""] * (len(header) - 1))
    for c in rev:
        d = by_id.get(c["did"], {})
        rows.append([
            "(reverse)", "MATCH", c["cli"], "", "", "",
            c["did"], d.get("cn", "") or d.get("dn", ""), f"{d.get('valor', 0):.0f}", f"{d.get('amount', 0):.0f}",
            f"{c['valor']:.0f}", d.get("num_raw", ""), d.get("cat", ""), d.get("prop", ""),
            "", "reverse", "", "", "sem linha na planilha", "Ivan: lançar comissão?", "",
        ])
    out = [[header_instr] + [""] * (len(header) - 1), header] + rows
    sh = gc.open_by_key(DASH_SHEET)
    try:
        ws = sh.worksheet(OUT_ABA)
        ws.clear()
    except Exception:
        ws = sh.add_worksheet(title=OUT_ABA, rows=len(out) + 5, cols=len(header))
    ws.update(values=out, range_name="A1")
    print(f"\n  OK aba '{OUT_ABA}' escrita ({len(out)} linhas).")


def _fix_mapa_nomes(gc, results):
    """Corrige escolha_bruno nos ALTA com company limpa (match por nome do candidato)."""
    ws = gc.open_by_key(DASH_SHEET).worksheet(MAPA_ABA)
    v = ws.get_all_values()
    # mapa nome_ivan -> (deal company name resolvida) p/ ALTA
    resolved = {}
    for pl, r in results:
        if r["conf"] == "ALTA" and r["deal"]:
            resolved[sync._norm_key2(pl["cli"])] = (r["deal"].get("cn") or "").strip()
    updates = 0
    for idx in range(2, len(v)):
        row = v[idx]
        nome = g(row, 0)
        if not nome:
            continue
        existing = g(row, 12)
        if existing:
            continue
        cn = resolved.get(sync._norm_key2(nome))
        if not cn:
            continue
        # achar qual candidato (col 3/6/9) bate com a company resolvida
        for ci, num in ((3, "1"), (6, "2"), (9, "3")):
            cand = g(row, ci)
            if cand and (toks(cand) & toks(cn)):
                ws.update_cell(idx + 1, 13, num)
                updates += 1
                break
    print(f"  mapa_nomes: {updates} escolha_bruno corrigidas (ALTA com company limpa).")


def _fmt_val(v):
    return str(int(v)) if abs(v - round(v)) < 0.005 else f"{v:.2f}"


def _backfill_crm(results, execute):
    """GATED — política PLANILHA VENCE (planilha do financeiro = fonte da verdade; corrige a
    inflação ×10/×100 do HS). Só ALTA E 1:1 (deal_id usado por 1 linha só) — os N:1 (Nubank,
    RMed, MedWriters, Asia) são pulados e ficam pra revisão manual com o Ivan (sobrescrever
    valor ambíguo seria perigoso). dry-run a menos que --backfill-execute."""
    import time
    import json as _json
    from collections import Counter
    usage = Counter(r["deal_id"] for _, r in results if r["deal_id"])
    mode = "EXECUTE (PATCH real)" if execute else "DRY-RUN (sem PATCH)"
    print(f"\n=== BACKFILL CRM ({mode}) — ALTA + 1:1, planilha vence ===")
    log = []
    patches = pulou = skip_n1 = 0
    for pl, r in results:
        if r["conf"] != "ALTA" or not r["deal_id"] or not r["deal"]:
            continue
        if usage[r["deal_id"]] > 1:
            skip_n1 += 1  # N:1 ambíguo -> não sobrescreve sem o Ivan
            continue
        d = r["deal"]
        props = {}
        # valor: planilha VENCE (sobrescreve se difere; corrige inflação)
        if pl["valor"] and abs((d.get("valor") or 0) - pl["valor"]) > 0.01:
            props["valor_do_aporte"] = _fmt_val(pl["valor"])
        if pl["num"] and not d.get("num"):
            props["numero_do_projeto"] = pl["num_raw"]
        # proponente: só se HS vazio e normaliza p/ entidade conhecida (evita inversão da planilha)
        if pl["prop"] and not d.get("prop"):
            ent = sync._map_proponente_interno(pl["prop"])
            if ent:
                props["nome_do_proponente"] = pl["prop"]
        if not props:
            pulou += 1
            continue
        entry = {"deal_id": r["deal_id"], "cliente": pl["cli"],
                 "hs_valor_antes": d.get("valor"), "props": props}
        log.append(entry)
        if execute:
            rr = sync.req("PATCH", f"/crm/v3/objects/deals/{r['deal_id']}", json={"properties": props})
            if rr.status_code == 200:
                patches += 1
            else:
                print(f"  [erro] {r['deal_id']}: {rr.status_code} {rr.text[:120]}")
            time.sleep(0.05)
        else:
            patches += 1
    for e in log[:25]:
        print(f"  deal {e['deal_id']} ({e['cliente'][:20]}) HS_antes={e['hs_valor_antes']}: {e['props']}")
    print(f"\n  backfill: {'PATCHED' if execute else 'WOULD-PATCH'}={patches} | pulou(ja-igual)={pulou} | "
          f"skip_N1_tangled={skip_n1} | total={len(log)}")
    logpath = os.path.join(os.path.dirname(__file__), "backfill_planilha_log.jsonl")
    with open(logpath, "w", encoding="utf-8") as f:
        for e in log:
            f.write(_json.dumps(e, ensure_ascii=False) + "\n")
    print(f"  log: {logpath}")


if __name__ == "__main__":
    main()
