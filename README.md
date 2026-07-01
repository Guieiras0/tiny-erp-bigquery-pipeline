# Tiny ERP V3 - Central Auth ETL

Este projeto realiza a extração de dados da API V3 do Tiny ERP e carrega no Google BigQuery em camadas de Data Lake (RAW).

## 🚀 Estrutura de Execução

O projeto é dividido em dois processos principais:

1.  **PAI (Headers):** Sincroniza cabeçalhos e índices (`pedidos`, `notas`, `produtos`, `contas`).
2.  **FILHO (Details):** Sincroniza detalhes específicos (`itens`, `parcelas`) de cada registro.

Ambos os jobs são multi-tenant: a lista de clientes, tokens de acesso e refresh tokens ficam
armazenados em uma tabela de configuração no BigQuery (`tiny_configs.clients`), não no código.

## ⚙️ Variáveis de Ambiente

Configure estas variáveis no Cloud Run Job:

| Variável | Obrigatória | Padrão | Descrição |
|---|---|---|---|
| `PROJECT_ID` | Sim | — | ID do seu projeto no GCP |
| `BQ_LOCATION` | Não | `US` | Região do BigQuery |
| `SLACK_WEBHOOK_URL` | Não | — | Webhook para relatório de execução no Slack |
| `ONLY_CLIENTS` | Não | `ALL` | Lista de clientes separados por vírgula (ex: `cliente_a,cliente_b`) |
| `ONLY_ENTITIES` | Não | `ALL` | Lista de endpoints separados por vírgula (ex: `pedidos,notas`) |
| `LOOKBACK_DAYS` | Não (PAI) | `0` | Reprocessamento manual: quantos dias voltar a partir de hoje |
| `PAI_OVERLAP_DAYS` | Não (PAI) | `3` | Janela de sobreposição para capturar pedidos cadastrados retroativamente |
| `REFRESH_DAYS` | Não (FILHO) | `0` | Reprocessa detalhes dos últimos N dias (DELETE+INSERT) |
| `FORCE_FULL_REFRESH` | Não (FILHO) | `false` | Força carga completa, ignorando watermark |
| `FORCE_RERUN` | Não (FILHO) | `false` | Ignora o controle de "já concluído hoje" |
| `MAX_WORKERS` | Não (FILHO) | `10` | Threads paralelas para busca de detalhes |

## 📦 Como fazer o Deploy

### 1. Build da Imagem (Artifact Registry)

```bash
gcloud builds submit --tag southamerica-east1-docker.pkg.dev/[GCP_PROJECT_ID]/tiny-repo/tiny-v3-etl .
```

Troque `[GCP_PROJECT_ID]` pelo ID do seu projeto no GCP.

### 2. Deploy como Cloud Run Job

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

### 3. Agendamento (Cloud Scheduler)

Cada job pode ser agendado via Cloud Scheduler, disparando o Cloud Run Job em um
horário fixo (ex: diariamente às 03h, horário de Brasília).

## 🗂️ Estrutura de dados

- **PAI**: grava um registro por entidade de topo (`pedidos`, `notas`, `contas_pagar`, etc.) na
  camada RAW, usando `WRITE_APPEND` (incremental, com watermark) ou `WRITE_TRUNCATE`
  (tabelas de referência, carga completa).
- **FILHO**: busca o detalhe de cada ID gerado pelo PAI (itens do pedido, parcelas, etc.) e
  grava em tabelas `*_detalhe`, controlando o que já foi processado para evitar reprocessamento
  desnecessário.
- **Watermarks**: cada entidade tem sua data de corte salva em tabelas dedicadas
  (`pai_watermarks`, `child_watermarks`), garantindo que apenas dados novos sejam buscados
  a cada execução.

## 🔒 Segurança

Nenhuma credencial (token, client secret) fica no código. Tudo é lido de uma tabela de
configuração no BigQuery e renovado automaticamente via OAuth refresh token quando expira.
