"""
Scraper CNPJ via domain - E4 Onda A (22/04)

Para Companies com `domain` preenchido mas `cnpj` vazio, tenta descobrir
o CNPJ do site da empresa (home / sobre / contato). Valida via checksum
oficial RF e PATCH no HubSpot (so se --execute).

Uso:
    python scrape_cnpj_from_domain.py --dry-run --limit 20    # teste pequeno
    python scrape_cnpj_from_domain.py --dry-run               # tudo, sem PATCH
    python scrape_cnpj_from_domain.py --execute               # PATCH real

Produz CSV log detalhado (status por Company) no mesmo diretorio.
"""

import argparse
import csv
import datetime
import os
import re
import sys
import time
from collections import Counter

import requests

BASE = "https://api.hubapi.com"
TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
SCRAPER_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

CNPJ_REGEX = re.compile(r"\b(\d{2}[.\s]?\d{3}[.\s]?\d{3}[/\s]?\d{4}[-\s]?\d{2})\b")


# ===================================================
# CNPJ VALIDATION
# ===================================================

def validar_cnpj(cnpj_raw):
    """Valida CNPJ via checksum oficial RF (2 digitos verificadores)."""
    nums = "".join(c for c in cnpj_raw if c.isdigit())
    if len(nums) != 14:
        return False
    if nums == nums[0] * 14:
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


def format_cnpj(cnpj_raw):
    """Retorna CNPJ so com digitos (14 chars).

    HubSpot property 'cnpj' valida INVALID_NUMERIC se enviado com mascara.
    Testado 22/04: PATCH '63.535.436/0001-03' retorna 400; '63535436000103' ok.
    """
    nums = "".join(c for c in cnpj_raw if c.isdigit())
    return nums if len(nums) == 14 else cnpj_raw


def normalize_domain(domain_raw):
    """Limpa domain: tira http, www, paths. Retorna 'example.com'."""
    d = (domain_raw or "").strip().lower()
    for prefix in ("https://", "http://"):
        if d.startswith(prefix):
            d = d[len(prefix):]
    if d.startswith("www."):
        d = d[4:]
    d = d.split("/")[0].split("?")[0].split("#")[0]
    return d


# ===================================================
# HUBSPOT API
# ===================================================

def fetch_companies_com_domain_sem_cnpj():
    """Retorna Companies com `domain` HAS_PROPERTY e `cnpj` NOT_HAS_PROPERTY."""
    companies = []
    after = None
    while True:
        body = {
            "limit": 100,
            "properties": ["name", "cnpj", "domain"],
            "filterGroups": [{
                "filters": [
                    {"propertyName": "domain", "operator": "HAS_PROPERTY"},
                    {"propertyName": "cnpj", "operator": "NOT_HAS_PROPERTY"},
                ]
            }],
        }
        if after:
            body["after"] = after
        r = requests.post(
            f"{BASE}/crm/v3/objects/companies/search",
            headers=HEADERS, json=body, timeout=30,
        )
        if r.status_code != 200:
            print(f"ERRO fetch: {r.status_code} {r.text[:200]}", file=sys.stderr)
            sys.exit(1)
        d = r.json()
        companies.extend(d.get("results", []))
        nxt = d.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt.get("after")
        time.sleep(0.2)
    return companies


def patch_cnpj_hubspot(company_id, cnpj_formatted):
    """PATCH Company.cnpj no HubSpot. Retorna (ok_bool, http_status)."""
    body = {"properties": {"cnpj": cnpj_formatted}}
    for attempt in range(3):
        r = requests.patch(
            f"{BASE}/crm/v3/objects/companies/{company_id}",
            headers=HEADERS, json=body, timeout=30,
        )
        if r.status_code in (200, 201):
            return True, r.status_code
        if r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        return False, r.status_code
    return False, 429


# ===================================================
# SCRAPER
# ===================================================

def try_fetch(url, timeout=10):
    """GET uma URL. Retorna (http_status, html_or_marker).

    Markers especiais quando status=0: '__timeout__', '__connection__', '__err__'
    """
    try:
        r = requests.get(url, headers=SCRAPER_HEADERS, timeout=timeout,
                         allow_redirects=True)
        return r.status_code, r.text if r.status_code == 200 else ""
    except requests.exceptions.Timeout:
        return 0, "__timeout__"
    except requests.exceptions.ConnectionError:
        return 0, "__connection__"
    except Exception:
        return 0, "__err__"


def find_cnpj_in_html(html):
    """Busca CNPJs validos no HTML. Retorna o mais frequente ou None."""
    matches = CNPJ_REGEX.findall(html)
    if not matches:
        return None
    valid_nums = []
    for m in matches:
        if validar_cnpj(m):
            valid_nums.append("".join(c for c in m if c.isdigit()))
    if not valid_nums:
        return None
    # Pegar o mais frequente (empresa dona geralmente aparece em header/footer)
    count = Counter(valid_nums)
    best = count.most_common(1)[0][0]
    return format_cnpj(best)


def build_urls_for_domain(domain):
    """URLs a tentar em ordem, priorizando home com www."""
    return [
        f"https://www.{domain}/",
        f"https://{domain}/",
        f"https://www.{domain}/sobre",
        f"https://www.{domain}/contato",
    ]


def process_company(company, dry_run, verbose=False):
    """Processa 1 Company: tenta scraping, valida, (opcionalmente) PATCH."""
    cid = company["id"]
    props = company.get("properties", {}) or {}
    name = props.get("name", "") or ""
    raw_domain = props.get("domain", "") or ""
    domain = normalize_domain(raw_domain)

    result = {
        "company_id": cid, "company_name": name, "domain": domain,
        "status": "", "url_sucesso": "", "cnpj_encontrado": "",
    }

    if not domain:
        result["status"] = "empty_domain"
        return result

    urls = build_urls_for_domain(domain)
    had_connection_error = False
    had_403 = False

    for url in urls:
        status, html = try_fetch(url)
        if status == 200:
            cnpj = find_cnpj_in_html(html)
            if cnpj:
                result["url_sucesso"] = url
                result["cnpj_encontrado"] = cnpj
                if dry_run:
                    result["status"] = "ok_dry_run"
                else:
                    ok, code = patch_cnpj_hubspot(cid, cnpj)
                    result["status"] = f"ok_patched_{code}" if ok else f"patch_failed_{code}"
                return result
            # HTTP 200 sem CNPJ: continua tentando outras URLs
            continue
        if status == 403:
            had_403 = True
            continue
        if status == 429:
            # rate limit global; esperar e continuar
            time.sleep(5)
            continue
        if status == 0 and html == "__connection__":
            had_connection_error = True
            continue
        # outros erros: segue tentando

    if had_403:
        result["status"] = "blocked_403"
    elif had_connection_error:
        result["status"] = "dns_or_connection"
    else:
        result["status"] = "no_cnpj_found"
    return result


# ===================================================
# MAIN
# ===================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--execute", action="store_true",
                        help="DESATIVA dry-run. PATCH real no HubSpot.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Processar so N Companies (teste)")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--sleep-between", type=float, default=2.0,
                        help="Segundos de rate limit entre Companies")
    args = parser.parse_args()

    # Default: se --execute nao passado, assume dry-run
    if args.execute:
        args.dry_run = False
    elif not args.dry_run:
        args.dry_run = True

    if not TOKEN:
        print("ERRO: HUBSPOT_TOKEN nao setado.", file=sys.stderr)
        sys.exit(1)

    mode = "DRY-RUN (sem PATCH)" if args.dry_run else "EXECUTE - PATCH REAL"
    print(f"=== Scraper CNPJ via domain - {mode} ===")

    print("Fetching Companies com domain preenchido + cnpj vazio...")
    companies = fetch_companies_com_domain_sem_cnpj()
    total = len(companies)
    print(f"Encontrou {total} Companies elegiveis")

    if args.limit:
        companies = companies[:args.limit]
        print(f"Limitando a {len(companies)} (teste)")

    log_name = f"scrape_cnpj_log_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M')}.csv"
    log_path = os.path.join(os.path.dirname(__file__), log_name)

    counts = Counter()
    t0 = time.time()
    fields = ["company_id", "company_name", "domain", "status", "url_sucesso", "cnpj_encontrado"]

    with open(log_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for i, c in enumerate(companies, 1):
            res = process_company(c, args.dry_run, args.verbose)
            writer.writerow(res)
            counts[res["status"]] += 1
            if i % 10 == 0 or args.verbose:
                elapsed = int(time.time() - t0)
                print(f"  [{i}/{len(companies)}] ({elapsed}s) "
                      f"{res['company_name'][:40]:40s} | {res['status']}")
            time.sleep(args.sleep_between)

    elapsed_total = int(time.time() - t0)
    print()
    print(f"=== RESUMO ({elapsed_total}s = {elapsed_total/60:.1f} min) ===")
    for status, n in counts.most_common():
        print(f"  {status:25s} {n}")
    print()
    sucesso_dry = counts.get("ok_dry_run", 0)
    sucesso_patch = sum(v for k, v in counts.items() if k.startswith("ok_patched"))
    if args.dry_run:
        print(f"CNPJs que seriam patchados: {sucesso_dry}/{len(companies)} "
              f"({100*sucesso_dry/max(len(companies),1):.1f}%)")
    else:
        print(f"CNPJs PATCHED com sucesso: {sucesso_patch}/{len(companies)} "
              f"({100*sucesso_patch/max(len(companies),1):.1f}%)")
    print(f"Log CSV: {log_path}")


if __name__ == "__main__":
    main()
