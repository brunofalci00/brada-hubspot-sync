"""
Sprint 2 follow-up (30/04) — Gera queries variantes pros 28 BAIXA do Sprint 2.

Logica: query exata `site:casadosdados.com.br "{nome}"` falhou. Tenta:
  Q1: site:casadosdados.com.br "{tokens significativos primeiros 2-3}"
  Q2: site:casadosdados.com.br "{token-chave isolado + 1 contexto}"
  Q3: site:casadosdados.com.br {sem aspas, todos tokens significativos}

Output: imprime queries que Claude executa via WebSearch tool em batches.
Resultados sao colados em sprint2_baixa_requery.json (mesmo formato que
sprint2_websearch_combined.json) e processados via process_sprint2.py.

Uso:
    python requery_baixa_sprint2.py
"""

import csv
import json
import os
import re

DIR = os.path.dirname(__file__)
BAIXA_CSV = os.path.join(DIR, "sprint2_baixa.csv")
OUT_QUERIES = os.path.join(DIR, "sprint2_baixa_queries.json")

STOPWORDS = {
    "ltda", "ltda.", "s.a", "s.a.", "sa", "s/a", "s/a.", "me", "epp", "eireli",
    "mei", "do", "da", "de", "dos", "das", "e", "no", "na", "nos", "nas",
    "brasil", "br", "do brasil", "&",
}

NOISE_TOKENS = {"-", ",", ".", "(", ")", "—", "–"}


def tokenize(name):
    """Lowercase, split por espaço e separadores comuns, remove pontuação."""
    s = re.sub(r"[\(\)\[\]\.,/\-\|]+", " ", name.lower())
    tokens = [t for t in s.split() if t and t not in NOISE_TOKENS]
    return tokens


def significant_tokens(name, max_n=4):
    """Tokens com >=4 chars que não são stopwords corporativas."""
    tokens = tokenize(name)
    sig = []
    for t in tokens:
        if t in STOPWORDS:
            continue
        if len(t) < 3:
            continue
        sig.append(t)
        if len(sig) >= max_n:
            break
    return sig


def gen_queries(name):
    """Retorna lista de queries em ordem de tentativa."""
    sig = significant_tokens(name, max_n=4)
    if not sig:
        return []

    queries = []

    # Q1: 2-3 tokens significativos com aspas
    if len(sig) >= 2:
        q = " ".join(sig[:3])
        queries.append(f'site:casadosdados.com.br "{q}"')

    # Q2: token-chave isolado (mais distintivo) + 1 contexto
    if len(sig) >= 1:
        token_chave = sig[0]  # primeiro token significativo
        contexto = sig[1] if len(sig) > 1 else ""
        if contexto:
            queries.append(f'site:casadosdados.com.br "{token_chave}" {contexto}')

    # Q3: sem aspas, todos tokens
    if len(sig) >= 1:
        queries.append(f"site:casadosdados.com.br " + " ".join(sig))

    # dedup mantendo ordem
    seen = set()
    out = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            out.append(q)
    return out


def main():
    with open(BAIXA_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"=== Sprint 2 re-query BAIXA — {len(rows)} Companies ===\n")

    out = []
    for r in rows:
        name = r["name_hubspot"]
        cid = r["company_id"]
        queries = gen_queries(name)
        out.append({
            "company_id": cid,
            "name": name,
            "state": "",
            "prioridade": r.get("prioridade_company", "MEDIA"),
            "candidates": [],
            "queries": queries,
        })

    with open(OUT_QUERIES, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f"Salvo {len(out)} entradas com queries em {OUT_QUERIES}")
    print()
    print("Sample (primeiros 5):")
    for it in out[:5]:
        print(f"  {it['name']!r}")
        for q in it["queries"]:
            print(f"    -> {q}")
        print()


if __name__ == "__main__":
    main()
