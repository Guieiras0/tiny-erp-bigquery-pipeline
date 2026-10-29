# Tiny ERP V3 → BigQuery: Multi-tenant ETL

Extracts data from the Tiny ERP API V3 and loads it into Google BigQuery, in the raw layer of a medallion data lake. Runs as two Cloud Run Jobs, scheduled daily, serving multiple clients from a single codebase.

## 🚀 How it runs

The pipeline is split into two jobs:

1. **PAI (headers):** syncs top-level records and indexes: orders (`pedidos`), invoices (`notas`), products (`produtos`) and accounts (`contas`).
2. **FILHO (details):** syncs the details of each record: order items (`itens`) and installments (`parcelas`).

Both jobs are multi-tenant: the client list, access tokens and refresh tokens are stored in a BigQuery config table (`tiny_configs.clients`), not in the code.

> Table and entity names are kept in Portuguese because they mirror the Tiny ERP API.

## ⚙️ Environment variables

Set these on the Cloud Run Job:

| Variable | Required | Default | Description |
|---|---|---|---|
| `PROJECT_ID` | Yes | — | Your GCP project ID |
| `BQ_LOCATION` | No | `US` | BigQuery region |
| `SLACK_WEBHOOK_URL` | No | — | Webhook for the run report in Slack |
| `ONLY_CLIENTS` | No | `ALL` | Comma-separated client list (e.g. `cliente_a,cliente_b`) |
| `ONLY_ENTITIES` | No | `ALL` | Comma-separated endpoint list (e.g. `pedidos,notas`) |
| `LOOKBACK_DAYS` | No (PAI) | `0` | Manual backfill: how many days back from today to reprocess |
| `PAI_OVERLAP_DAYS` | No (PAI) | `3` | Overlap window to catch records created retroactively |
| `REFRESH_DAYS` | No (FILHO) | `0` | Reprocesses details from the last N days (DELETE + INSERT) |
| `FORCE_FULL_REFRESH` | No (FILHO) | `false` | Forces a full load, ignoring the watermark |
| `FORCE_RERUN` | No (FILHO) | `false` | Ignores the "already completed today" check |
| `MAX_WORKERS` | No (FILHO) | `10` | Parallel threads for fetching details |

## 📦 Deployment

### 1. Build the image (Artifact Registry)

```bash
gcloud builds submit --tag southamerica-east1-docker.pkg.dev/[GCP_PROJECT_ID]/tiny-repo/tiny-v3-etl .
```

Replace `[GCP_PROJECT_ID]` with your GCP project ID.

### 2. Deploy as Cloud Run Jobs

```bash
gcloud run jobs create tiny-etl-pai \
  --image southamerica-east1-docker.pkg.dev/[GCP_PROJECT_ID]/tiny-repo/tiny-v3-etl \
  --region southamerica-east1 \
  --set-env-vars PROJECT_ID=[GCP_PROJECT_ID] \
  --command python \
  --args append_pai.py
```

```bash
gcloud run jobs create tiny-etl-filho \
  --image southamerica-east1-docker.pkg.dev/[GCP_PROJECT_ID]/tiny-repo/tiny-v3-etl \
  --region southamerica-east1 \
  --set-env-vars PROJECT_ID=[GCP_PROJECT_ID] \
  --command python \
  --args append_filho.py
```

### 3. Scheduling (Cloud Scheduler)

Each job can be triggered by Cloud Scheduler at a fixed time (e.g. daily at 3 a.m., Brasília time).

## 🗂️ Data design

- **PAI** writes one record per top-level entity (`pedidos`, `notas`, `contas_pagar`, etc.) to the raw layer, using `WRITE_APPEND` (incremental, watermark-based) or `WRITE_TRUNCATE` (full load for reference tables).
- **FILHO** fetches the details of every ID produced by PAI (order items, installments, etc.) and writes them to `*_detalhe` tables, tracking what was already processed to avoid unnecessary reprocessing.
- **Watermarks:** each entity's cutoff date is stored in dedicated tables (`pai_watermarks`, `child_watermarks`), so each run only fetches new data.

## 🔒 Security

No credentials (tokens, client secrets) live in the code. Everything is read from a BigQuery config table, and access tokens are renewed automatically through the OAuth refresh token when they expire.
