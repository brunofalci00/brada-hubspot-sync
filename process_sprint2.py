"""
Sprint 2 (29/04) - Processa resultados de busca por nome -> CNPJ candidatos.

Input: sprint2_websearch_results.json
  [{"company_id": "...", "name": "...", "state": "...", "prioridade": "ALTA",
    "candidates": [{"cnpj": "61038592000125", "title": "MARSH CORRETORA..."}]}]

Logica:
  - Agrupa CNPJs por raiz (8 primeiros digitos)
  - 1 unica raiz: pega matriz (final 0001-XX) se houver, senao 1o resultado -> ALTA
  - 2+ raizes: ambiguo -> MEDIA
  - 0 candidatos -> BAIXA

Cross-validacao via BrasilAPI:
  - Pega CNPJ escolhido
  - Consulta BrasilAPI -> razao_social, situacao_cadastral
  - Se BAIXADA: rebaixa pra MEDIA
  - Se nome_hubspot vs razao_social diverge muito: rebaixa pra MEDIA

Output:
  - sprint2_alta.csv  (PATCH-able apos revisao Bruno)
  - sprint2_media.csv (revisao manual / override Ivan)
  - sprint2_baixa.csv (sem candidato -> Sprint 3)
"""

import csv
import json
import os
import re
import sys
import time
from collections import defaultdict

import requests

CACHE_PATH = os.path.join(os.path.dirname(__file__), "brasilapi_cache.json")


# ===================================================
# HELPERS
# ===================================================

def validar_cnpj(cnpj_raw):
    """Valida CNPJ via checksum oficial RF."""
    nums = "".join(c for c in str(cnpj_raw) if c.isdigit())
    if len(nums) != 14 or nums == nums[0] * 14:
        return False
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(nums[i]) * pesos1[i] for i in range(12))
    resto = soma % 11
    dv1 = 0 if resto < 2 else 11 - resto
    if int(nums[12]) != dv1:
        return False
    pesos2 = [6] + pesos1
    soma = sum(int(nums[i]) * pesos2[i] for i in range(13))
    resto = soma % 11
    dv2 = 0 if resto < 2 else 11 - resto
    return int(nums[13]) == dv2


def normalize_name(s):
    """Normaliza nome pra comparacao: lowercase, remove acentos, remove sufixos LTDA/SA/etc."""
    s = (s or "").lower().strip()
    # Remove acentos basico
    rep = {"á":"a","à":"a","â":"a","ã":"a","é":"e","ê":"e","í":"i",
           "ó":"o","ô":"o","õ":"o","ú":"u","ç":"c"}
    for k, v in rep.items():
        s = s.replace(k, v)
    # Remove sufixos comuns
    sufixos = [" ltda.", " ltda", " s.a.", " s/a", " s.a", " sa",
               " me", " eireli", " epp", " mei",
               " - me", " - epp"]
    for suf in sufixos:
        if s.endswith(suf):
            s = s[:-len(suf)]
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def name_overlap(a, b):
    """Score 0-1 de overlap de tokens entre dois nomes normalizados."""
    a_tokens = set(normalize_name(a).split())
    b_tokens = set(normalize_name(b).split())
    if not a_tokens or not b_tokens:
        return 0.0
    inter = a_tokens & b_tokens
    return len(inter) / max(len(a_tokens), len(b_tokens))


# ===================================================
# BRASIL API
# ===================================================

def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def brasilapi_lookup(cnpj, cache):
    """Consulta BrasilAPI. Retorna dict ou None. Cacheia."""
    cnpj = "".join(c for c in cnpj if c.isdigit())
    if cnpj in cache:
        return cache[cnpj]
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                cache[cnpj] = data
                save_cache(cache)
                return data
            if r.status_code == 404:
                cache[cnpj] = None
                save_cache(cache)
                return None
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            cache[cnpj] = None
            return None
        except Exception:
            time.sleep(1)
    cache[cnpj] = None
    return None


# ===================================================
# CLASSIFICACAO
# ===================================================

def escolher_cnpj(candidates):
    """Dado lista de candidates, retorna (cnpj_escolhido, num_raizes, motivo).

    Logica:
    - Filtra invalidos (checksum)
    - Agrupa por raiz (8 primeiros digitos)
    - 1 raiz: pega matriz (final 0001-XX) ou 1o
    - 2+ raizes: pega 1o resultado mas marca ambiguidade
    """
    validos = [c for c in candidates if validar_cnpj(c["cnpj"])]
    if not validos:
        return None, 0, "sem_candidato_valido"

    raizes = defaultdict(list)
    for c in validos:
        raiz = c["cnpj"][:8]
        raizes[raiz].append(c)

    def pick_matriz(cs):
        return next((c for c in cs if c["cnpj"][8:12] == "0001"), cs[0])

    if len(raizes) == 1:
        cs = list(raizes.values())[0]
        chosen = pick_matriz(cs)
        motivo = "matriz_unica" if chosen["cnpj"][8:12] == "0001" else "filial_unica"
        return chosen["cnpj"], 1, motivo

    # Multiplas raizes. Procura raiz dominante (>=70% dos validos).
    raiz_counts = sorted(raizes.items(), key=lambda x: -len(x[1]))
    top_raiz, top_cs = raiz_counts[0]
    if len(top_cs) / len(validos) >= 0.7:
        chosen = pick_matriz(top_cs)
        motivo = "raiz_dominante_matriz" if chosen["cnpj"][8:12] == "0001" else "raiz_dominante_filial"
        return chosen["cnpj"], len(raizes), motivo

    # Raizes pulverizadas. Pega 1o apresentado (top do search).
    return validos[0]["cnpj"], len(raizes), "multiplas_entidades"


def classificar(nome_hubspot, candidates, cache):
    """Retorna dict com decisao final + cnpj_escolhido + razao + confianca."""
    cnpj, num_raizes, motivo = escolher_cnpj(candidates)
    res = {
        "cnpj_escolhido": cnpj or "",
        "num_raizes": num_raizes,
        "motivo_escolha": motivo,
        "razao_social_brasilapi": "",
        "situacao_brasilapi": "",
        "name_overlap": 0.0,
        "confianca": "BAIXA",
        "obs": "",
    }
    if cnpj is None:
        res["confianca"] = "BAIXA"
        res["obs"] = motivo
        return res

    # BrasilAPI cross-check
    info = brasilapi_lookup(cnpj, cache)
    if info is None:
        res["confianca"] = "MEDIA"
        res["obs"] = "brasilapi_nao_retornou"
        return res

    razao = info.get("razao_social", "") or ""
    nome_fantasia = info.get("nome_fantasia", "") or ""
    situacao = info.get("descricao_situacao_cadastral", "") or ""
    res["razao_social_brasilapi"] = razao
    res["situacao_brasilapi"] = situacao

    # Score do melhor entre razao_social e nome_fantasia
    overlap = max(name_overlap(nome_hubspot, razao),
                  name_overlap(nome_hubspot, nome_fantasia))
    res["name_overlap"] = round(overlap, 3)

    # Decisao de confianca
    if situacao.upper() not in ("ATIVA", ""):
        res["confianca"] = "MEDIA"
        res["obs"] = f"situacao={situacao}"
        return res

    # Multi-raiz so vira MEDIA se nao tem raiz dominante
    if num_raizes >= 2 and motivo not in ("raiz_dominante_matriz", "raiz_dominante_filial"):
        res["confianca"] = "MEDIA"
        res["obs"] = f"ambiguo_{num_raizes}_entidades"
        return res

    if overlap >= 0.5:
        res["confianca"] = "ALTA"
        res["obs"] = motivo
    elif overlap >= 0.3:
        res["confianca"] = "MEDIA"
        res["obs"] = f"overlap_baixo_{overlap:.2f}"
    else:
        res["confianca"] = "MEDIA"
        res["obs"] = f"overlap_muito_baixo_{overlap:.2f}"
    return res


# ===================================================
# MAIN
# ===================================================

def main():
    in_path = sys.argv[1] if len(sys.argv) > 1 else "sprint2_websearch_results.json"
    with open(in_path, encoding="utf-8") as f:
        items = json.load(f)

    print(f"=== Process Sprint 2: {len(items)} Companies ===")
    cache = load_cache()

    out_rows = []
    for i, item in enumerate(items, 1):
        candidates = item.get("candidates", []) or []
        result = classificar(item["name"], candidates, cache)
        out_rows.append({
            "company_id": item["company_id"],
            "name_hubspot": item["name"],
            "prioridade_company": item.get("prioridade", ""),
            "num_candidatos": len(candidates),
            **result,
            "candidatos_brutos": ";".join(c["cnpj"] for c in candidates[:5]),
        })
        if i % 10 == 0:
            print(f"  [{i}/{len(items)}] {item['name'][:35]:35s} -> {result['confianca']}")
        time.sleep(0.5)  # throttle BrasilAPI

    # Output 3 CSVs por confianca
    fields = list(out_rows[0].keys())
    for conf in ("ALTA", "MEDIA", "BAIXA"):
        path = os.path.join(os.path.dirname(__file__), f"sprint2_{conf.lower()}.csv")
        rows_filt = [r for r in out_rows if r["confianca"] == conf]
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows_filt)
        print(f"  {conf:6s}: {len(rows_filt):4d} -> {path}")


if __name__ == "__main__":
    main()
