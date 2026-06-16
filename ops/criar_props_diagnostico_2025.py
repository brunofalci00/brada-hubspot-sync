"""Separa o diagnóstico por ano no card da Company (HubSpot) — idempotente.

Reunião comercial 16/06: o diagnóstico muda ano a ano (recompra). Hoje os 12 campos
do grupo `diagnostico` (valor total + 11 leis) estão sem disciplina de ano (misturado).

Estratégia (menor risco — ver plano Diagnóstico 2025 vs 2026):
  1. Os 12 campos ATUAIS (nome técnico unsuffixed) viram o ANO CORRENTE: relabel do
     grupo `diagnostico` -> "Diagnóstico 2026". Nome técnico imutável -> dashboards/sync/gap
     seguem ligados nos mesmos campos, sem quebra.
  2. Cria grupo `diagnostico_2025` ("Diagnóstico 2025") + 12 campos `*_2025` (number),
     espelhando os atuais. Vazios agora; planilha do Ivan preenche depois (task parqueada).

NÃO popula nenhuma empresa. Idempotente: pula grupo/campos que já existem; o relabel é
seguro de repetir.

Uso: python ops/criar_props_diagnostico_2025.py            # dry-run
     python ops/criar_props_diagnostico_2025.py --apply    # aplica
"""
import os
import sys

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

H = {"Authorization": f"Bearer {os.environ['HUBSPOT_TOKEN']}", "Content-Type": "application/json"}
PROPS_BASE = "https://api.hubapi.com/crm/v3/properties/companies"
GROUPS_BASE = "https://api.hubapi.com/crm/v3/properties/companies/groups"

OLD_GROUP = "diagnostico"
OLD_GROUP_NEW_LABEL = "Diagnóstico 2026"
NEW_GROUP = "diagnostico_2025"
NEW_GROUP_LABEL = "Diagnóstico 2025"

# (sufixo_name, label_base) — espelha os 12 campos atuais do grupo diagnostico.
# Os nomes técnicos ganham _2025; o label ganha " 2025" pra ficar inequívoco na busca.
FIELDS = [
    ("valor_total_do_diagnostico", "Valor total do diagnóstico"),
    ("valor_lei_rouanet", "Valor – Lei Rouanet"),
    ("valor_lei_do_esporte", "Valor – Lei do Esporte (Federal)"),
    ("valor_lei_do_esporte_estadual", "Valor – Lei do Esporte (Estadual)"),
    ("valor_lei_do_bem", "Valor – Lei do Bem"),
    ("valor_lei_da_cultura", "Valor – Lei da Cultura (Estadual)"),
    ("valor_lei_da_cultura_municipal", "Valor – Lei da Cultura (Municipal)"),
    ("valor_lei_da_crianca_e_do_adolescente", "Valor – FIA (Criança e Adolescente)"),
    ("valor_lei_do_idoso", "Valor – Fundo do Idoso"),
    ("valor_lei_da_reciclagem", "Valor – Lei da Reciclagem"),
    ("valor_pronas", "Valor – PRONAS"),
    ("valor_pronon", "Valor – PRONON"),
]


def ensure_group(apply):
    r = requests.get(f"{GROUPS_BASE}/{NEW_GROUP}", headers=H, timeout=30)
    if r.status_code == 200:
        print(f"[GRUPO JA EXISTE] {NEW_GROUP} (label={r.json().get('label')!r})")
        return
    if r.status_code != 404:
        print(f"[ERRO] GET grupo {NEW_GROUP} status={r.status_code}: {r.text[:200]}")
        return
    print(f"[CRIAR GRUPO] {NEW_GROUP} (label={NEW_GROUP_LABEL!r})")
    if not apply:
        return
    cr = requests.post(GROUPS_BASE, headers=H,
                       json={"name": NEW_GROUP, "label": NEW_GROUP_LABEL}, timeout=30)
    print(f"  {'OK' if cr.status_code in (200, 201) else 'FALHA ' + str(cr.status_code)}: {cr.text[:200]}")


def relabel_old_group(apply):
    r = requests.get(f"{GROUPS_BASE}/{OLD_GROUP}", headers=H, timeout=30)
    if r.status_code != 200:
        print(f"[ERRO] grupo {OLD_GROUP} status={r.status_code}: {r.text[:200]}")
        return
    cur = r.json().get("label")
    if cur == OLD_GROUP_NEW_LABEL:
        print(f"[RELABEL JA OK] grupo {OLD_GROUP} já é {OLD_GROUP_NEW_LABEL!r}")
        return
    print(f"[RELABEL GRUPO] {OLD_GROUP}: {cur!r} -> {OLD_GROUP_NEW_LABEL!r} (nome técnico imutável)")
    if not apply:
        return
    pr = requests.patch(f"{GROUPS_BASE}/{OLD_GROUP}", headers=H,
                        json={"label": OLD_GROUP_NEW_LABEL}, timeout=30)
    print(f"  {'OK' if pr.status_code == 200 else 'FALHA ' + str(pr.status_code)}: {pr.text[:200]}")


def create_field(name, label, apply):
    r = requests.get(f"{PROPS_BASE}/{name}", headers=H, timeout=30)
    if r.status_code == 200:
        j = r.json()
        print(f"[JA EXISTE] {name} (group={j.get('groupName')})")
        return
    if r.status_code != 404:
        print(f"[ERRO] GET {name} status={r.status_code}: {r.text[:200]}")
        return
    payload = {
        "name": name, "label": label, "type": "number", "fieldType": "number",
        "groupName": NEW_GROUP, "formField": False,
        "description": "Diagnóstico do ciclo 2025 (capacidade de incentivo da empresa). "
                       "Preenchido via planilha do Ivan / reconciliação.",
    }
    print(f"[CRIAR CAMPO] {name} ({label}) number/number group={NEW_GROUP}")
    if not apply:
        return
    cr = requests.post(PROPS_BASE, headers=H, json=payload, timeout=30)
    print(f"  {'OK' if cr.status_code in (200, 201) else 'FALHA ' + str(cr.status_code)}: {cr.text[:200]}")


def main():
    apply = "--apply" in sys.argv
    print(f"{'APLICANDO' if apply else 'DRY-RUN'} — diagnóstico por ano (Company)\n")
    print("== Grupo 2025 ==")
    ensure_group(apply)
    print("\n== Relabel grupo atual -> 2026 ==")
    relabel_old_group(apply)
    print(f"\n== 12 campos *_2025 (grupo {NEW_GROUP}) ==")
    for suffix, label_base in FIELDS:
        create_field(f"{suffix}_2025", f"{label_base} 2025", apply)
    if not apply:
        print("\nDRY-RUN. Rode com --apply pra criar/relabelar de fato.")


if __name__ == "__main__":
    main()
