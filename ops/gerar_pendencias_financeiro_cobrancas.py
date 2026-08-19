# -*- coding: utf-8 -*-
"""Gera fila Ivan e rascunhos por owner a partir do HubSpot (nao envia email)."""
import argparse
import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
ENV_PATH = r"C:\Users\bruno\.brada-secrets\hubspot.env"
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, encoding="utf-8-sig") as fh:
        for line in fh:
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())

import sync
from financeiro_match_common import completeness_gaps, deal_link, norm, select_match_won


def load_rows():
    stages = sync.load_stages()
    owners = sync.load_owner_map()
    deals = sync.fetch_all_deals(sync.DEAL_PROPERTIES)
    deal_to_company = sync.fetch_associated_companies([d["id"] for d in deals])
    companies = sync.fetch_companies(deal_to_company.values())
    enriched = [sync.enrich(d, stages, deal_to_company, companies, owners=owners) for d in deals]
    return select_match_won(sync.build_consolidado_layer(enriched, stages=stages))


def line(row):
    gaps = completeness_gaps(row)
    return (f"- [{row.get('cliente') or '(sem cliente)'} — {row.get('nome_projeto') or '(sem projeto)'}]"
            f"({deal_link(row)}) — owner: {row.get('owner') or '(sem owner)'}; "
            f"faltam: {', '.join(gaps) if gaps else 'nenhum'}")


def draft(owner, rows):
    first = "Jaqueline" if owner == "jaqueline" else "Danielle"
    body = [
        "---", f"name: rascunho_email_{owner}_campos_financeiros_2026-08-06", f"description: Rascunho consolidado para {first} preencher dados financeiros no HubSpot.", "type: doc", f"title: Rascunho — campos financeiros HubSpot — {first}", "date: 2026-08-06",
        "tags:", "  - brada/reporting_financeiro", "  - rascunho-email", "status: aguardando-atribuicao", "---", "",
        f"# Rascunho de e-mail — {first}", "", "**CC:** Ivan", "",
        "**Assunto:** Preenchimento dos dados financeiros nos negócios ganhos", "",
        f"Oi, {first},", "",
        "Criamos no HubSpot quatro campos financeiros obrigatórios para os negócios em Ganho - Incentivador: número do contrato, recibo ou nota fiscal, condições de pagamento e número de parcelas (1 = à vista).",
        "", "Antes da primeira carga automática para a planilha de comissões/cobranças, preciso que você complete os negócios abaixo e também os demais gaps listados em cada item. O preenchimento deve ser feito no HubSpot, não na planilha.", "",
    ]
    body.extend(line(row) for row in rows)
    body.extend(["", "Quando concluir, por favor responda este e-mail para liberarmos o dry-run final e a primeira carga.", "", "Obrigado!"])
    return "\n".join(body) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", required=True)
    args = ap.parse_args()
    rows = load_rows()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    assignment = [r for r in rows if not str(r.get("owner", "")).strip() or norm(r.get("owner")) in {"sem owner", "ivan amaral", "ivan"}]
    header = [
        "---", "name: pendencias_atribuicao_ivan_2026-08-06", "description: Fila de negócios MATCH sem owner ou com owner Ivan antes dos e-mails às executivas.", "type: doc", "title: Fila inicial de atribuição — Ivan", "date: 2026-08-06", "tags:",
        "  - brada/reporting_financeiro", "  - pendencia", "status: aguardando-ivan", "---", "",
        "# Fila inicial de atribuição — Ivan", "",
        "> [!warning] Gate", "> Os rascunhos por executiva só devem ser enviados depois desta atribuição e da resolução dos vínculos ambíguos.", ">", "> Casos ambíguos: [[pendencias_vinculos_ambiguos_2026-08-06]].", "",
        f"MATCH ganhos analisados: {len(rows)}. Negócios sem owner ou com owner Ivan: {len(assignment)}.", "",
    ]
    header.extend(line(row) for row in assignment)
    (out / "pendencias_atribuicao_ivan_2026-08-06.md").write_text("\n".join(header) + "\n", encoding="utf-8")
    for key in ("jaqueline", "danielle"):
        owned = [r for r in rows if key in norm(r.get("owner")) and completeness_gaps(r)]
        (out / f"rascunho_email_{key}_campos_financeiros_2026-08-06.md").write_text(draft(key, owned), encoding="utf-8")
        print(f"{key}: {len(owned)} deals")
    print(f"MATCH won={len(rows)} fila Ivan={len(assignment)} output={out}")


if __name__ == "__main__":
    main()
