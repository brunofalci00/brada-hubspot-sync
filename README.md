# brada-hubspot-sync

Sync **HubSpot CRM → Google Sheets** a cada hora via GitHub Actions. A Google Sheet alimenta o Dashboard Comercial da Vanessa no Looker Studio.

Padrão espelhado do repo `brada-tickets-sync` (dashboard inscrições de corridas).

## Arquitetura

```
GitHub Actions (cron 0 * * * *)
    │
    ├─ HubSpot Private App API → puxa deals + companies
    ├─ Enriquece com campos calculados (stages, flags, valores, datas)
    │
    └─ gspread escreve em Google Sheets "Brada_Dashboard_Deals" (aba raw_deals)
            │
            └─ Looker Studio conectado à Sheet → Dashboard Comercial Vanessa
```

**Latência total HubSpot → Looker**: até 1h (cron) + até 15min (cache Looker) = ~1h15min pior caso.

## Arquivos

- `sync.py` — script principal
- `.github/workflows/sync.yml` — workflow cron + manual trigger
- `requirements.txt` — dependências Python
- `service-account-key.json` — credenciais Google **(não commitar, já no .gitignore)**

## Setup (primeira vez)

### 1. Criar Google Sheet

1. Google Drive → nova Sheet chamada `Brada_Dashboard_Deals`
2. Copiar o ID da URL: `https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit`
3. Compartilhar a Sheet com a service account (Editor):
   `brada-sheets@brada-tickets.iam.gserviceaccount.com`

> A service account é a mesma do dashboard corridas. Se quiser isolar escopos no futuro, criar nova em `https://console.cloud.google.com`.

### 2. Criar repo no GitHub

```bash
cd "C:\Users\bruno\Documents\Brada\HubSpot\github-actions"
git init
git add .
git commit -m "Initial commit — sync HubSpot to Google Sheets"
gh repo create brunofalci00/brada-hubspot-sync --public --source=. --push
```

> Repo público: GitHub Actions minutos ilimitados em repos públicos; nenhuma credencial é commitada (tudo em Secrets).

### 3. Configurar secrets

No repo GitHub → Settings → Secrets and variables → Actions:

| Secret | Valor |
|---|---|
| `HUBSPOT_TOKEN` | Private App token do Portal HubSpot 50771078 (ver local `.env` / gestor de senhas) |
| `SPREADSHEET_ID` | ID da Sheet criada no passo 1 |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | JSON completo da service account (copiar do repo `brada-tickets-sync`) |

### 4. Primeiro run manual

Actions → Sync HubSpot -> Sheets → Run workflow → Run

Verificar logs: deve imprimir `Deals puxados: N` e `Sheets atualizado: N linhas em raw_deals`.

### 5. Conectar Looker Studio à Sheet

1. [lookerstudio.google.com](https://lookerstudio.google.com) → Create → Data Source
2. Google Sheets → autenticar com conta que tem acesso à Sheet
3. Selecionar `Brada_Dashboard_Deals` → aba `raw_deals` → "Use first row as headers"
4. Salvar como `Brada_Deals_Sheet`

## Desenvolvimento local

### Pré-requisitos

- Python 3.12
- `service-account-key.json` no diretório pai (ou ajustar `GOOGLE_SERVICE_ACCOUNT_FILE`)

### Rodar

```bash
cd "C:\Users\bruno\Documents\Brada\HubSpot\github-actions"
pip install -r requirements.txt

# Windows (Git Bash)
export HUBSPOT_TOKEN="<seu-token-hubspot>"
export SPREADSHEET_ID="<id-da-sheet>"
python sync.py

# Ou usando env vars no PowerShell
$env:HUBSPOT_TOKEN="<token>"; $env:SPREADSHEET_ID="<id>"; python sync.py
```

## Manutenção

### Adicionar campo novo ao sync

1. Adicionar o nome do property em `DEAL_PROPERTIES` ou `COMPANY_PROPERTIES` no topo de `sync.py`
2. Adicionar o campo no dict retornado pela função `enrich()`
3. Commit + push → próximo cron já pega

### Troubleshooting

- **"HUBSPOT_TOKEN nao configurado"** → secret faltando no repo
- **"Credenciais Google nao encontradas"** → `GOOGLE_SERVICE_ACCOUNT_JSON` faltando ou malformado
- **"ERRO search deals: 401"** → token HubSpot expirado ou scopes insuficientes
- **"ERRO assoc batch: 207"** → *não é erro*, é status multi-status OK; o código aceita 200 e 207

### Logs

Actions → Sync HubSpot -> Sheets → ver execução → step "Sync HubSpot -> Sheets".

Últimas 90 dias de runs ficam no GitHub Actions gratuitamente.

## Próximos passos (Sprint 1+)

- Adicionar sync de contacts (quando a Automatize começar a popular)
- Adicionar aba `raw_activities` com log de interações (calls/emails/meetings)
- Substituir gspread writes por batchUpdate para deals > 10k (hoje 563 — não é gargalo)
