"""Fixer de espelhamento financeiro (Sprint 1.6): faz o HubSpot == planilha do Ivan.

Grão = 1 card por NÚMERO de projeto (parcelas com mesmo numero somam). Planilha vence
no valor (corrige inflação ×100). Pós-venda = ganho. Decide o máximo por conta própria;
só os tangles ambíguos vão pro Ivan.

Reusa os loaders/normalizadores do resolver (resolver_planilha_multicampo).

Uso:
    python fixer_espelhamento.py                 # dry-run: imprime PLANO DE AÇÃO
    python fixer_espelhamento.py --fix-values    # PATCH valor (planilha vence) nos CONFIANTES
    python fixer_espelhamento.py --create         # POST cards faltantes inequívocos (idempotente)
"""
import os
import re
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import resolver_planilha_multicampo as R  # noqa: E402
sync = R.sync

GANHO_STAGE = "1253324968"   # "Ganho - Incentivador"
INCENT_PIPE = "default"
PRODUTO_MATCH = "Match"
# Deals segurados pra decisão do Ivan (conflito de valor planilha x outra fonte) — NÃO auto-corrigir.
HOLD_IVAN = {"58362233425"}  # Nubank "Corrida Vai Bem": planilha 2.268.822,54 x 2.424.690,12


def retry(fn, *a, **k):
    for i in range(5):
        try:
            return fn(*a, **k)
        except Exception:
            time.sleep(3)
    raise SystemExit("falhou apos retries")


_BRANDS = [("nu bank", "nubank"), ("nubank", "nubank"), ("medwrites", "medwriters"),
           ("medwriters", "medwriters"), ("rmed cursos medicos", "rmed"), ("rmed", "rmed"),
           ("funeraria maracana", "fune"), ("fune maracana", "fune"), ("funeraria", "fune"),
           ("seel servicos", "seel"), ("seel", "seel"), ("casa do alemao", "casa do alemao"),
           ("asia shipping transportes", "asia"), ("asia shipping", "asia"), ("asia", "asia"),
           ("vinicola galiotto", "galiotto"), ("galoitto", "galiotto"), ("galiotto", "galiotto")]


def cnk(s):
    k = sync._norm_key2(s)
    for a, b in _BRANDS:
        if k.startswith(a):
            return b
    return k.split()[0] if k.split() else k


def numk(s):
    return re.sub(r"[^0-9a-z]", "", str(s or "").lower())


def fonte_to_lei(fonte):
    f = (fonte or "").lower()
    cat = "ISS" if ("iss" in f or "promac" in f) else ("ICMS" if "icms" in f else "IR")
    if "crianc" in f:
        leif, leil = "valor_lei_da_crianca_e_do_adolescente", "FIA (Crianca e Adolescente)"
    elif "esporte" in f:
        leif, leil = ("valor_lei_do_esporte_estadual", "Esporte Estadual") if cat == "ICMS" \
            else ("valor_lei_do_esporte", "Esporte Federal")
    else:  # cultura (default)
        leif, leil = ("valor_lei_da_cultura_municipal", "Cultura Municipal") if cat == "ISS" \
            else (("valor_lei_da_cultura", "Cultura Estadual") if cat == "ICMS"
                  else ("valor_lei_rouanet", "Rouanet"))
    flag = "" if any(k in f for k in ("cultura", "esporte", "crianc", "cultural", "aud", "rouanet")) else "TEMA?"
    return cat, leif, leil, flag


def vmatch(hs_val, target, tol=0.02):
    if not target:
        return False
    return any(abs(hs_val - f * target) <= max(1.0, tol * f * target) for f in (1, 10, 100))


def build_plan():
    by_id, pool = retry(R.load_deals)
    plan = R.load_planilha(R.sync.get_sheets_client())
    # agrupa planilha por (empresa, numero) -> projeto
    proj = defaultdict(list)
    for p in plan:
        nk = cnk(p["cli"])
        num = numk(p["num_raw"])
        key = (nk, num if len(num) >= 4 else f"noproj:{round(p['valor'])}")
        proj[key].append(p)
    # HS Incentivador por empresa
    hs_by = defaultdict(list)
    for d in pool:
        hs_by[cnk(d["cn"] or d["dn"])].append(dict(d, _used=False))

    actions = []  # (empresa, numero, target, fonte, cat, lei, deal_id, hs_val, acao, conf)
    # processa por empresa pra atribuir cards corretamente
    projs_by_co = defaultdict(list)
    for (nk, num), rows in proj.items():
        projs_by_co[nk].append((num, rows))
    for nk, plist in projs_by_co.items():
        cards = hs_by.get(nk, [])
        for num, rows in plist:
            target = round(sum(r["valor"] for r in rows), 2)
            fonte = next((r["fonte"] for r in rows if r["fonte"]), "")
            cat, leif, leil, flag = fonte_to_lei(fonte)
            real_num = num if not num.startswith("noproj") else ""
            # 1) match por numero
            cand = [c for c in cards if not c["_used"] and real_num and numk(c["num_raw"]) == real_num]
            # 2) match por valor (incl inflado)
            if not cand:
                cand = [c for c in cards if not c["_used"] and c["valor"] and vmatch(c["valor"], target)]
            # 3) card vazio (valor 0)
            empty = [c for c in cards if not c["_used"] and not c["valor"]]
            if cand:
                c = cand[0]
                c["_used"] = True
                if abs((c["valor"] or 0) - target) < 0.01:
                    acao, conf = "OK", "alta"
                else:
                    acao, conf = "CORRIGIR-VALOR", ("alta" if (real_num and numk(c["num_raw"]) == real_num) or vmatch(c["valor"], target) else "media")
                actions.append((nk, num, target, fonte, cat, leil, flag, c["id"], c["valor"], acao, conf))
            elif len(empty) == 1 and len([pp for pp in plist if not numk(pp[0]).startswith("noproj")]) >= 0 and len(empty) >= len([p2 for p2 in plist]) - sum(1 for a in actions if a[0] == nk and a[9] != "CRIAR"):
                # único card vazio e (heurística) sem competição -> preencher
                c = empty[0]
                c["_used"] = True
                actions.append((nk, num, target, fonte, cat, leil, flag, c["id"], 0, "PREENCHER", "media"))
            elif empty:
                # vários vazios / ambíguo -> tangle
                actions.append((nk, num, target, fonte, cat, leil, flag, "", "", "TANGLE-ambiguo", "baixa"))
            else:
                actions.append((nk, num, target, fonte, cat, leil, flag, "", "", "CRIAR", "media"))
    # cards HS com valor>0 sem projeto na planilha -> reverse
    reverse = []
    for nk, cards in hs_by.items():
        for c in cards:
            if not c["_used"] and c["valor"]:
                reverse.append((nk, c["id"], c["cn"] or c["dn"], c["valor"]))
    return actions, reverse, by_id


def main():
    fix_values = "--fix-values" in sys.argv
    actions, reverse, by_id = build_plan()
    print(f"{'empresa':14s} {'numero':16s} {'target':>13s} {'cat':4s} {'acao':16s} {'conf':5s} deal/HS_val  fonte")
    by_acao = defaultdict(int)
    for nk, num, target, fonte, cat, lei, flag, did, hsval, acao, conf in sorted(actions, key=lambda x: (x[9], x[0])):
        by_acao[acao] += 1
        hs = f"{did} HS={hsval}" if did else "(sem card)"
        print(f"{nk[:14]:14s} {num[:16]:16s} {target:>13,.2f} {cat:4s} {acao:16s} {conf:5s} {hs:24s} {fonte[:14]} {flag}")
    print(f"\nRESUMO ações: {dict(by_acao)}")
    print(f"\nREVERSE (HS valor>0 sem projeto na planilha): {len(reverse)}")
    for nk, did, nome, val in sorted(reverse, key=lambda x: -x[3])[:20]:
        print(f"  {did} {nome[:32]:32s} HS={val:,.2f}")

    if fix_values:
        print("\n=== APLICANDO CORRIGIR-VALOR/PREENCHER confiantes (conf=alta) ===")
        n = 0
        for nk, num, target, fonte, cat, lei, flag, did, hsval, acao, conf in actions:
            if did in HOLD_IVAN:
                print(f"  HOLD {did} {nk} (conflito p/ Ivan) — não toco")
                continue
            if acao in ("CORRIGIR-VALOR", "PREENCHER") and conf == "alta" and did and target:
                val = str(int(target)) if abs(target - int(target)) < 0.005 else f"{target:.2f}"
                rr = sync.req("PATCH", f"/crm/v3/objects/deals/{did}", json={"properties": {"valor_do_aporte": val}})
                ok = rr.status_code == 200
                print(f"  {'OK' if ok else 'ERRO'} {did} {nk} {hsval} -> {val}" + ("" if ok else f" {rr.text[:100]}"))
                if ok:
                    n += 1
                time.sleep(0.05)
        print(f"  PATCHED={n}")


if __name__ == "__main__":
    main()
