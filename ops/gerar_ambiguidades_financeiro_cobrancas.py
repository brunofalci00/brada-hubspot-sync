# -*- coding: utf-8 -*-
"""Lista vínculos ambíguos entre linhas legadas e deals; somente leitura."""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ops.gerar_pendencias_financeiro_cobrancas import load_rows
from financeiro_match_common import deal_link, reconcile
from sheets_comissoes_ivan import read_state as read_vendas, SCHEMA as VENDAS_SCHEMA
from sheets_cobrancas_bia import read_state as read_bia, SCHEMA as BIA_SCHEMA
import sync


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    rows = load_rows()
    gc = sync.get_sheets_client()
    sh = gc.open_by_key("1XVRuIMN9kGto35gL8FPhXIUTgUV8t0CY4IKl8IHhScI")
    found = []
    for title, reader, schema in (
        ("Controle de Vendas", read_vendas, VENDAS_SCHEMA),
        ("Controle de Cobranças - Bia", read_bia, BIA_SCHEMA),
    ):
        _matches, ambiguous, _unmatched = reconcile(reader(sh, title)["records"], rows, schema)
        for item in ambiguous:
            found.append((title, item["row"]["row_number"], item["candidates"]))
    text = [
        "---", "name: pendencias_vinculos_ambiguos_2026-08-06", "description: Linhas legadas com mais de um negócio HubSpot candidato; nenhuma escrita automática.", "type: doc", "title: Vínculos ambíguos — Comissões 2026", "date: 2026-08-06",
        "tags:", "  - brada/reporting_financeiro", "  - pendencia", "status: aguardando-ivan", "---", "",
        "# Vínculos ambíguos — revisão Ivan", "",
        "> [!warning] Sem escrita", "> Nenhum caso desta lista será alterado enquanto houver mais de um negócio candidato.", "",
    ]
    for title, row_number, candidates in found:
        links = ", ".join(f"[{d['deal_id']}]({deal_link(d)}) — {d.get('cliente')} / {d.get('nome_projeto')}" for d in candidates)
        text.append(f"- {title}, linha {row_number}: {links}")
    if not found:
        text.append("- Nenhum vínculo ambíguo detectado pelas regras atuais.")
    Path(args.output).write_text("\n".join(text) + "\n", encoding="utf-8")
    print(f"ambiguidades={len(found)} output={args.output}")


if __name__ == "__main__":
    main()
