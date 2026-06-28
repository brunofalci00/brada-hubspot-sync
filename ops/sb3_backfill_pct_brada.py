"""Backfill one-shot de `percentual_brada` nos Match Won classificados (HubSpot).

S3 (27/06, PROMPT_S3_Percentual_Real_Match_Brada): grava o 10/15 ATUAL como ponto
de partida da property nova, pra o dashboard nao regredir quando o sync.py passar a
ler a property. Identidade matematica: Interno->15, Externo->10 (= mesmo calculo de
hoje no consolidado). O executivo depois sobrescreve com o % real.

Replica a cascata EXATA de interno/externo do sync.py (build_consolidado_layer):
  tipo=="Externo" -> Externo ; tipo!="" -> Interno ;
  _map_proponente_interno(nome) -> Interno ; nome!="" -> Externo ; senao "" (skip).

Escopo: produto in {Match, Match interno, Match externo} (decisao Bruno 27/06).
CRIAPE/Elaboracao ficam de fora (filtro de produto).
Idempotente: pula deal que JA tem percentual_brada (nao sobrescreve valor real).

Uso: python sb3_backfill_pct_brada.py            # dry-run (default)
     python sb3_backfill_pct_brada.py --execute  # PATCH real via batch/update
"""
import argparse
import os
import sys
import time
from collections import Counter

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import requests

ENV_PATH = r"C:\Users\bruno\.brada-secrets\hubspot.env"
with open(ENV_PATH, encoding="utf-8-sig") as fh:
    for line in fh:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

BASE = "https://api.hubapi.com"
TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Closed-won dos 2 pipelines + pos-venda (= STAGES_GANHO do sync.py).
STAGES_GANHO = ["1253324968", "1246571362", "contractsent",
                "1247329455", "1247329456", "1246571363", "1253441207"]
PRODUTOS_MATCH = ["Match", "Match interno", "Match externo"]
PCT_INTERNO = 15
PCT_EXTERNO = 10

# ---- copiado VERBATIM de sync.py (1699-1762) pra garantir identidade de calculo ----


def _norm_key(s):
    if not s:
        return ""
    s = str(s).strip().lower()
    for a, b in (("á", "a"), ("à", "a"), ("ã", "a"), ("â", "a"), ("é", "e"), ("ê", "e"),
                 ("í", "i"), ("ó", "o"), ("ô", "o"), ("õ", "o"), ("ú", "u"), ("ç", "c")):
        s = s.replace(a, b)
    return " ".join(s.split())


PROPONENTE_INTERNO_MATCHERS = [
    ("egp cir", "EGP Cir. Soc. Cultura IR"),
    ("egp", "EGP"),
    ("encaminhando", "Encaminhando"),
    ("conectados do bem", "Conectados do Bem"),
    ("conectados caxias", "Conectados Caxias"),
    ("circuito social", "Circuito Social de Corrida"),
    ("conectados", "Conectados"),
    ("brada digital", "Brada Digital"),
    ("proj.casa", "Brada Digital"),
    ("somos brada", "Brada Digital"),
    ("brada", "Brada Digital"),
]


def _map_proponente_interno(nome):
    p = _norm_key(nome)
    if not p:
        return None
    for sub, entidade in PROPONENTE_INTERNO_MATCHERS:
        if sub in p:
            return entidade
    return None


# ---- fim do trecho copiado ----


def interno_externo(tipo_prop, nome_prop):
    """Cascata identica ao sync.py (sem o ramo CRIAPE — filtramos produto=Match)."""
    tipo_prop = (tipo_prop or "").strip()
    nome_prop = (nome_prop or "").strip()
    if tipo_prop == "Externo":
        return "Externo"
    if tipo_prop:
        return "Interno"
    if _map_proponente_interno(nome_prop):
        return "Interno"
    if nome_prop:
        return "Externo"
    return ""


def fetch_match_won():
    deals = []
    after = None
    while True:
        body = {
            "limit": 100,
            "properties": ["produto", "dealstage", "tipo_de_proponente",
                           "nome_do_proponente", "percentual_brada",
                           "closedate", "dealname", "valor_do_aporte"],
            "filterGroups": [{
                "filters": [
                    {"propertyName": "dealstage", "operator": "IN", "values": STAGES_GANHO},
                    {"propertyName": "produto", "operator": "IN", "values": PRODUTOS_MATCH},
                ]
            }],
        }
        if after:
            body["after"] = after
        r = requests.post(f"{BASE}/crm/v3/objects/deals/search", headers=H, json=body, timeout=40)
        if r.status_code != 200:
            print(f"ERRO search: {r.status_code} {r.text[:300]}", file=sys.stderr)
            sys.exit(1)
        d = r.json()
        deals.extend(d.get("results", []))
        nxt = d.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt.get("after")
        time.sleep(0.2)
    return deals


def build_plan(deals):
    inputs = []
    stats = Counter()
    detalhe = []
    for d in deals:
        pr = d.get("properties", {}) or {}
        ja = (pr.get("percentual_brada") or "").strip()
        if ja:
            stats["skip_ja_preenchido"] += 1
            continue
        ie = interno_externo(pr.get("tipo_de_proponente"), pr.get("nome_do_proponente"))
        if ie == "Interno":
            pct = PCT_INTERNO
        elif ie == "Externo":
            pct = PCT_EXTERNO
        else:
            stats["skip_sem_classificacao"] += 1
            continue
        via = "tipo" if (pr.get("tipo_de_proponente") or "").strip() else "nome"
        stats[f"patch_{ie}_{pct}"] += 1
        stats[f"via_{via}"] += 1
        inputs.append({"id": d["id"], "properties": {"percentual_brada": pct}})
        detalhe.append((d["id"], ie, pct, via, (pr.get("closedate") or "")[:4],
                        (pr.get("dealname") or "")[:34]))
    return inputs, stats, detalhe


def execute_batches(inputs):
    ok = erros = 0
    for i in range(0, len(inputs), 100):
        chunk = inputs[i:i + 100]
        r = requests.post(f"{BASE}/crm/v3/objects/deals/batch/update",
                          headers=H, json={"inputs": chunk}, timeout=60)
        if r.status_code in (200, 207):
            ok += len(chunk)
        else:
            erros += len(chunk)
            print(f"BATCH ERRO chunk {i}: {r.status_code} {r.text[:300]}", file=sys.stderr)
        time.sleep(0.3)
    return ok, erros


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="DESATIVA dry-run. PATCH real.")
    args = ap.parse_args()
    if not TOKEN:
        print("ERRO: HUBSPOT_TOKEN nao setado", file=sys.stderr)
        sys.exit(1)
    mode = "EXECUTE — PATCH REAL" if args.execute else "DRY-RUN (sem PATCH)"
    print(f"=== Backfill percentual_brada (Match Won) — {mode} ===\n")

    deals = fetch_match_won()
    print(f"Match Won (produto in {PRODUTOS_MATCH}): {len(deals)} deals\n")

    inputs, stats, detalhe = build_plan(deals)
    print("Plano:")
    for k, v in sorted(stats.items()):
        print(f"  {k:26s} {v}")
    print(f"\n  -> {len(inputs)} deals receberao percentual_brada (15 Interno / 10 Externo)\n")

    print("Amostra (ate 25):")
    print(f"  {'deal_id':14s} {'ie':8s} {'pct':>3s} {'via':5s} {'ano':5s} deal")
    for did, ie, pct, via, ano, dn in detalhe[:25]:
        print(f"  {did:14s} {ie:8s} {pct:3d} {via:5s} {ano:5s} {dn}")

    if not args.execute:
        print(f"\nDRY-RUN. {len(inputs)} seriam patchados. Rode com --execute.")
        return

    print(f"\nExecutando batch update em {len(inputs)} deals...")
    t0 = time.time()
    ok, erros = execute_batches(inputs)
    print(f"PATCH: {ok} ok, {erros} erros ({time.time()-t0:.1f}s)")


if __name__ == "__main__":
    main()
