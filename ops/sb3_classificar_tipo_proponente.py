"""F1 (S3) — lista os Match Won sem tipo_de_proponente pra Bruno classificar.

NAO auto-PATCHa: a maioria e lixo/clone 2025 (NPS ENVIADO, reunioes, clones).
Dry-run imprime a tabela nominal + sugestao de auto-derivacao (a mesma cascata do
sync.py: nome interno -> Interno; nome presente nao-interno -> Externo; nome vazio
-> '?' caso a caso). Bruno preenche CLASSIFICACOES e roda --execute pra PATCH.

Impacto na meta 2026 ~ zero (8 dos 9 sao 2025; o unico 2026 e o clone Athie sem valor).

Uso: python sb3_classificar_tipo_proponente.py            # dry-run (lista)
     python sb3_classificar_tipo_proponente.py --execute  # PATCH so o que estiver em CLASSIFICACOES
"""
import os
import sys
import time

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
H = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
STAGES_GANHO = ["1253324968", "1246571362", "contractsent",
                "1247329455", "1247329456", "1246571363", "1253441207"]
PRODUTOS_MATCH = ["Match", "Match interno", "Match externo"]

# Bruno preenche {deal_id: "Interno"|"Externo"} e roda --execute. Vazio = ninguem patchado.
CLASSIFICACOES = {}

INTERNOS = ["egp cir", "egp", "encaminhando", "conectados do bem", "conectados caxias",
            "circuito social", "conectados", "brada digital", "proj.casa", "somos brada", "brada"]


def norm(s):
    import unicodedata
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower().strip()


def sugestao(nome):
    p = norm(nome)
    if not p:
        return "? (nome vazio - caso a caso)"
    if any(sub in p for sub in INTERNOS):
        return "Interno (nome do grupo)"
    return "Externo (nome de terceiro)"


def fetch_sem_tipo():
    deals, after = [], None
    while True:
        body = {
            "limit": 100,
            "properties": ["produto", "dealstage", "tipo_de_proponente",
                           "nome_do_proponente", "closedate", "dealname", "valor_do_aporte"],
            "filterGroups": [{
                "filters": [
                    {"propertyName": "dealstage", "operator": "IN", "values": STAGES_GANHO},
                    {"propertyName": "produto", "operator": "IN", "values": PRODUTOS_MATCH},
                    {"propertyName": "tipo_de_proponente", "operator": "NOT_HAS_PROPERTY"},
                ]
            }],
        }
        if after:
            body["after"] = after
        r = requests.post(f"{BASE}/crm/v3/objects/deals/search", headers=H, json=body, timeout=40)
        r.raise_for_status()
        d = r.json()
        deals.extend(d.get("results", []))
        nxt = d.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt.get("after")
        time.sleep(0.2)
    return deals


def main():
    execute = "--execute" in sys.argv
    deals = fetch_sem_tipo()
    print(f"=== Match Won SEM tipo_de_proponente: {len(deals)} ===\n")
    print(f"  {'deal_id':14s} {'ano':5s} {'valor':>12s} {'nome_proponente':24s} {'deal':34s} sugestao")
    for d in sorted(deals, key=lambda x: (x['properties'].get('closedate') or '')):
        pr = d["properties"]
        ano = (pr.get("closedate") or "")[:4] or "s/data"
        nome = (pr.get("nome_do_proponente") or "")
        val = pr.get("valor_do_aporte") or ""
        print(f"  {d['id']:14s} {ano:5s} {str(val):>12s} {nome[:24]:24s} {(pr.get('dealname') or '')[:34]:34s} {sugestao(nome)}")

    print("\nPreencha CLASSIFICACOES = {deal_id: 'Interno'|'Externo'} no topo e rode --execute.")
    if not execute:
        return
    if not CLASSIFICACOES:
        print("\nCLASSIFICACOES vazio — nada a PATCHar.")
        return
    inputs = [{"id": did, "properties": {"tipo_de_proponente": tp}} for did, tp in CLASSIFICACOES.items() for tp in [tp]]
    r = requests.post(f"{BASE}/crm/v3/objects/deals/batch/update", headers=H, json={"inputs": inputs}, timeout=60)
    print(f"\nPATCH {len(inputs)} deals: status {r.status_code}")
    if r.status_code not in (200, 207):
        print(r.text[:300])


if __name__ == "__main__":
    main()
