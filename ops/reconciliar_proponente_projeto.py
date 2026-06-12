"""
Reconciliar nome_do_proponente / nome_do_projeto dos deals Incentivador won
com a planilha OFICIAL Vendas_25_26 (13timE4).

Bruno 12/06: a coluna "Projeto" da oficial sao TODOS nomes de projeto validos
(corrigiu minha hipotese de "contaminacao") -> backfill nome_do_projeto a partir
dela. nome_do_proponente ja esta 39/55 ok; aqui so preenche o que falta / alinha
divergentes a oficial.

Dois niveis (folha = comissao -> nada sem --execute):
  Tier 1 (fill-if-empty): preenche nome_do_projeto e nome_do_proponente VAZIOS.
          E o "adicionar" que o Bruno liberou. Nao sobrescreve nada.
  Tier 2 (--overwrite): alinha tambem onde o HS ja tem valor DIFERENTE da oficial
          (sobrescrita). Gated a parte.

Reusa o matcher provado de reconciliacao_oficial.py (08/06). NAO toca o caminho
de apply daquele modulo. So mexe em pipeline=default (Incentivador).

Uso:
  python ops/reconciliar_proponente_projeto.py                       # dry-run
  python ops/reconciliar_proponente_projeto.py --execute             # aplica Tier 1
  python ops/reconciliar_proponente_projeto.py --execute --overwrite # Tier 1 + Tier 2
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import reconciliacao_oficial as RO
from reconciliacao_oficial import load_oficial, group_oficial, build_hs_index, match_grupo, ck2

sync = RO.sync
BASE = "https://api.hubapi.com"
PROPS = ["dealname", "nome_do_proponente", "nome_do_projeto", "numero_do_projeto", "valor_do_aporte"]
LOG = r"C:\tmp\reconciliar_proponente_projeto_log.jsonl"


def _headers():
    return {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}"}


def load_won_incentivador():
    """Deals pipeline=default closed-won (mesmo escopo da planilha financeira)."""
    out, after = [], None
    while True:
        body = {"filters": [{"propertyName": "pipeline", "operator": "EQ", "value": "default"},
                            {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"}],
                "properties": PROPS, "limit": 100}
        if after:
            body["after"] = after
        r = requests.post(f"{BASE}/crm/v3/objects/deals/search", headers=_headers(), json=body).json()
        out += r.get("results", [])
        after = r.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return out


def build_pool(deals):
    pool = []
    for d in deals:
        p = d["properties"]
        try:
            val = float(p.get("valor_do_aporte") or 0)
        except (TypeError, ValueError):
            val = 0.0
        pool.append({"id": d["id"], "dn": p.get("dealname", "") or "", "valor": val,
                     "num_raw": p.get("numero_do_projeto", "") or "",
                     "prop": p.get("nome_do_proponente", "") or "",
                     "projeto": p.get("nome_do_projeto", "") or ""})
    return pool


def _eq(a, b):
    return bool(a) and bool(b) and ck2(a) == ck2(b)


def reconcile(gc):
    """Retorna (tier1, tier2, n_deals, n_match). Cada item: dict por deal."""
    deals = load_won_incentivador()
    pool = build_pool(deals)
    grupos = group_oficial(load_oficial(gc))
    hs_by, by_num = build_hs_index(pool)
    all_cards = [c for v in hs_by.values() for c in v]

    tier1, tier2, n_match = [], [], 0
    for g in grupos:
        m = match_grupo(g, hs_by, by_num, all_cards)
        c = m.get("card")
        if not c:
            continue
        n_match += 1
        did, sig, cli = c["id"], m["sig"], c["_cli"]
        Ph, Jh = c.get("prop", ""), c.get("projeto", "")
        Pg, Jg = g.get("proponente", ""), g.get("projeto", "")

        fill, over = {}, {}
        # nome_do_projeto
        if Jg and not Jh:
            fill["nome_do_projeto"] = {"old": "", "new": Jg}
        elif Jg and Jh and not _eq(Jh, Jg):
            over["nome_do_projeto"] = {"old": Jh, "new": Jg}
        # nome_do_proponente
        if Pg and not Ph:
            fill["nome_do_proponente"] = {"old": "", "new": Pg}
        elif Pg and Ph and not _eq(Ph, Pg):
            over["nome_do_proponente"] = {"old": Ph, "new": Pg}

        if fill:
            tier1.append({"id": did, "cli": cli, "sig": sig, "campos": fill})
        if over:
            tier2.append({"id": did, "cli": cli, "sig": sig, "campos": over})
    return tier1, tier2, len(deals), n_match


def _print_tier(nome, itens):
    print(f"\n=== {nome}: {len(itens)} deals ===")
    for it in itens:
        for campo, vv in it["campos"].items():
            old = f"'{vv['old']}'" if vv["old"] else "(vazio)"
            print(f"  {it['cli'][:24]:24} | {campo:20} {old} -> '{vv['new']}'  [{it['sig']}]")


def apply(itens, execute):
    """PATCH por deal. Sem --execute: so conta."""
    patched, log = 0, []
    for it in itens:
        props = {campo: vv["new"] for campo, vv in it["campos"].items()}
        rec = {"deal_id": it["id"], "cli": it["cli"], "props": props,
               "antes": {campo: vv["old"] for campo, vv in it["campos"].items()}}
        if execute:
            r = sync.req("PATCH", f"/crm/v3/objects/deals/{it['id']}", json={"properties": props})
            ok = r.status_code in (200, 201)
            rec["status"] = r.status_code
            if ok:
                patched += 1
            else:
                print(f"  [ERRO] {it['id']} {r.status_code}: {r.text[:120]}")
        else:
            patched += 1
        log.append(rec)
    if execute and log:
        os.makedirs(os.path.dirname(LOG), exist_ok=True)
        with open(LOG, "a", encoding="utf-8") as f:
            for rec in log:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return patched


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="obrigatorio p/ escrever no CRM")
    ap.add_argument("--overwrite", action="store_true", help="aplica tambem Tier 2 (sobrescrita)")
    args = ap.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    for line in open(r"C:\Users\bruno\.brada-secrets\hubspot.env", encoding="utf-8-sig"):
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

    gc = sync.get_sheets_client()
    tier1, tier2, n_deals, n_match = reconcile(gc)
    print(f"won Incentivador={n_deals} | grupos casados={n_match} | "
          f"Tier1(fill)={len(tier1)} | Tier2(overwrite)={len(tier2)}")
    _print_tier("TIER 1 — preencher vazios (fill-if-empty, Bruno liberou)", tier1)
    _print_tier("TIER 2 — alinhar divergentes a oficial (SOBRESCRITA, precisa OK)", tier2)

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    print(f"\n[{mode}]")
    n1 = apply(tier1, args.execute)
    print(f"  Tier 1 {'PATCHED' if args.execute else 'would-patch'}: {n1}")
    if args.overwrite:
        n2 = apply(tier2, args.execute)
        print(f"  Tier 2 {'PATCHED' if args.execute else 'would-patch'}: {n2}")
    else:
        print(f"  Tier 2 retido (use --overwrite p/ aplicar os {len(tier2)} de sobrescrita)")
    if not args.execute:
        print("\n[dry-run] nada escrito no CRM. Use --execute p/ Tier 1.")


if __name__ == "__main__":
    main()
