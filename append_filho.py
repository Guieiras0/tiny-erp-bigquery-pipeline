#!/usr/bin/env python3
# coding: utf-8
"""
Tiny → BigQuery | APPEND FILHOS (detalhes) — PRODUÇÃO v6f
Atom Analytics

Correções v6f (2026-06-21):
- FIX CRÍTICO: erros transitórios (429, timeout, exceção) NÃO salvam mais
  registro "VAZIO" no BQ.

  Problema antes (v5f):
    fetch_detail retornava `None` para qualquer falha:
      404/410 (pedido não existe)
      429 esgotado após MAX_AUTH_RETRIES (rate limit)
      Timeout (rede)
      Exception genérica
    No loop principal, `None` virava registro com `_status=VAZIO`.
    Resultado: pedido com itens reais ficava marcado como vazio
    eternamente, sem retentativas.

  Solução agora:
    fetch_detail retorna strings distintas:
      "NOT_FOUND" (404/410) → salva como VAZIO, é definitivo
      "FETCH_ERROR" (429 esgotado, timeout, exceção) → NÃO salva nada
      None ou DataFrame normal → fluxo normal
    
    Quando FETCH_ERROR ocorre, o ID não fica na tabela filha. Na próxima
    execução, o FILHO detecta como pendente (não está em
    list_existing_child_ids) e tenta novamente.

- Aumentado MAX_RETRIES_RATELIMIT para 5 (era implicitamente 1 dentro do
  loop AUTH_RETRIES). Backoff exponencial em 429.

- Log claro: quando FETCH_ERROR acontece, loga "❌ FETCH_ERROR id=X
  (tentará novamente)" para que apareça no Slack.

Correções v5f (2026-05-04):
- DELETE movido para DENTRO do loop de flush.

Correções v5b (anterior):
- IDs vazio/404 marcados com _status=VAZIO.

Env vars:
- Mesmas anteriores.
- MAX_RETRIES_429 (default 5): número de retries em rate limit antes de
  desistir do pedido (que vai virar FETCH_ERROR).
"""

from __future__ import annotations

import os
import time
import json
import random
import threading
import requests
import pandas as pd
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Dict, List, Optional, Any

from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
PROJECT_ID  = os.getenv("PROJECT_ID", "your-gcp-project-id")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

API_BASE = "https://api.tiny.com.br/public-api/v3"
AUTH_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"

TBL_CLIENTES   = f"{PROJECT_ID}.tiny_configs.clients"
TBL_WATERMARK  = f"{PROJECT_ID}.tiny_configs.child_watermarks"
TBL_AUDITORIA  = f"{PROJECT_ID}.tiny_configs.tb_auditoria"
TBL_EXECUCOES  = f"{PROJECT_ID}.tiny_configs.child_execucoes_diarias"

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL") or os.getenv("WEBHOOK_URL")

ONLY_CLIENTS = os.getenv("ONLY_CLIENTS", "ALL")
ONLY_ENTS    = os.getenv("ONLY_ENTITIES", "ALL")

MAX_WORKERS  = int(os.getenv("MAX_WORKERS", "10"))
BATCH_ROWS   = int(os.getenv("BATCH_ROWS", "500"))
BATCH_SUBMIT = int(os.getenv("BATCH_SUBMIT", "200"))

DEFAULT_WATERMARK_DATE = os.getenv("DEFAULT_WATERMARK_DATE", "2020-01-01")
FORCE_RERUN  = os.getenv("FORCE_RERUN", "false").lower() == "true"
REFRESH_DAYS = int(os.getenv("REFRESH_DAYS", "0"))
FORCE_FULL_REFRESH = os.getenv("FORCE_FULL_REFRESH", "false").lower() == "true"

MAX_AUTH_RETRIES   = 2
MAX_RETRIES_429    = int(os.getenv("MAX_RETRIES_429", "5"))


# ==============================================================================
# MAPA FILHO → PAI
# ==============================================================================
CHILD_CONFIG: Dict[str, Dict[str, str]] = {
    "notas":                         {"parent": "notas",          "date": "dataEmissao", "path": "notas/{id}"},
    "notas_marcadores":              {"parent": "notas",          "date": "dataEmissao", "path": "notas/{id}/marcadores"},
    "pedidos":                       {"parent": "pedidos",        "date": "dataCriacao", "path": "pedidos/{id}"},
    "pedidos_marcadores":            {"parent": "pedidos",        "date": "dataCriacao", "path": "pedidos/{id}/marcadores"},
    "contas_pagar":                  {"parent": "contas_pagar",   "date": "data",        "path": "contas-pagar/{id}"},
    "contas_pagar_recebimentos":     {"parent": "contas_pagar",   "date": "data",        "path": "contas-pagar/{id}/recebimentos"},
    "contas_pagar_marcadores":       {"parent": "contas_pagar",   "date": "data",        "path": "contas-pagar/{id}/marcadores"},
    "contas_receber":                {"parent": "contas_receber", "date": "data",        "path": "contas-receber/{id}"},
    "contas_receber_recebimentos":   {"parent": "contas_receber", "date": "data",        "path": "contas-receber/{id}/recebimentos"},
    "contas_receber_marcadores":     {"parent": "contas_receber", "date": "data",        "path": "contas-receber/{id}/marcadores"},
    "produtos":                      {"parent": "produtos",       "date": "dataCriacao", "path": "produtos/{id}"},
    "produto_custos":                {"parent": "produtos",       "date": "dataCriacao", "path": "produtos/{id}/custos"},
    "produtos_tags":                 {"parent": "produtos",       "date": "dataCriacao", "path": "produtos/{id}/tags"},
    "produtos_kit":                  {"parent": "produtos",       "date": "dataCriacao", "path": "produtos/{id}/kit"},
    "produtos_fabricado":            {"parent": "produtos",       "date": "dataCriacao", "path": "produtos/{id}/fabricado"},
    "produtos_estoque":              {"parent": "produtos",       "date": "dataCriacao", "path": "produtos/{id}/estoque"},
    "servicos":                      {"parent": "servicos",       "date": "dataCriacao", "path": "servicos/{id}"},
    "expedicao":                     {"parent": "expedicao",      "date": "data",        "path": "expedicao/{id}"},
    "separacao":                     {"parent": "separacao",      "date": "dataCriacao", "path": "separacao/{id}"},
    "contatos":                      {"parent": "contatos",       "date": "dataCriacao", "path": "contatos/{id}"},
    "contatos_pessoas":              {"parent": "contatos",       "date": "dataCriacao", "path": "contatos/{id}/pessoas"},
    "ordem_compra":                  {"parent": "ordem_compra",   "date": "data",        "path": "ordem-compra/{id}"},
    "ordem_compra_marcadores":       {"parent": "ordem_compra",   "date": "data",        "path": "ordem-compra/{id}/marcadores"},
    "ordem_servico":                 {"parent": "ordem_servico",  "date": "data",        "path": "ordem-servico/{id}"},
    "ordem_servico_marcadores":      {"parent": "ordem_servico",  "date": "data",        "path": "ordem-servico/{id}/marcadores"},
}

FULL_PARENTS = {"produtos", "servicos", "expedicao", "contatos"}

BQ         = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
_lock_auth = threading.Lock()
_tls       = threading.local()


# ==============================================================================
# LOG / SLACK
# ==============================================================================
def log(msg, client="-", ent="-"):
    print(f"{datetime.now():%H:%M:%S} | {client:<28} | {ent:<30} | {msg}", flush=True)

def enviar_slack(texto):
    if not SLACK_WEBHOOK:
        return
    try:
        requests.post(SLACK_WEBHOOK, json={"text": texto}, timeout=15)
    except Exception as e:
        log(f"⚠️ Slack: {e}")


# ==============================================================================
# AUDITORIA
# ==============================================================================
def registrar_auditoria(cliente, entidade, status, qtd, duracao):
    rows = [{
        "id": int(time.time() * 1000) + random.randint(1, 999),
        "modo": "APPEND",
        "nm_cliente": cliente,
        "dt_execucao": datetime.now(timezone.utc).isoformat(),
        "job_tipo": "FILHO",
        "nm_entidade": entidade,
        "status": status,
        "qtd_registros": int(qtd),
        "tempo_carga_segundos": int(duracao),
    }]
    try:
        errors = BQ.insert_rows_json(TBL_AUDITORIA, rows)
        if errors:
            log(f"⚠️ Auditoria: {errors}", client=cliente, ent=entidade)
    except Exception as e:
        log(f"⚠️ Auditoria: {e}", client=cliente, ent=entidade)


# ==============================================================================
# CONTROLE EXECUÇÃO DIÁRIA
# ==============================================================================
def cliente_ja_concluido_hoje(client_name):
    try:
        sql = f"""SELECT 1 FROM `{TBL_EXECUCOES}`
                  WHERE des_clients=@c AND data_execucao=CURRENT_DATE('America/Sao_Paulo')
                  AND status='CONCLUIDO' LIMIT 1"""
        df = BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("c", "STRING", client_name),
        ])).to_dataframe()
        return not df.empty
    except:
        return False

def registrar_conclusao_diaria(client_name, status="CONCLUIDO"):
    rows = [{
        "des_clients": client_name,
        "data_execucao": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "status": status,
        "dt_registro": datetime.now(timezone.utc).isoformat(),
    }]
    try:
        BQ.insert_rows_json(TBL_EXECUCOES, rows)
    except Exception as e:
        log(f"⚠️ Conclusão diária: {e}", client=client_name)


# ==============================================================================
# NOMENCLATURA
# ==============================================================================
def dataset_name_from_client(des_clients):
    slug = des_clients.strip().lower().replace(" ", "_")
    if not slug.startswith("cliente_"):
        slug = f"cliente_{slug}"
    return f"{slug}_raw"

def child_table_name(entity):
    t = entity.replace("produto_custos", "produtos_custos")
    suffixes_sem_detalhe = [
        "custos", "recebimentos", "marcadores",
        "tags", "kit", "fabricado", "estoque", "pessoas",
    ]
    if any(s in t for s in suffixes_sem_detalhe):
        return t
    return f"{t}_detalhe"


# ==============================================================================
# HTTP SESSION
# ==============================================================================
def get_session(token):
    s = getattr(_tls, "session", None)
    if not s:
        s = requests.Session()
        retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[500, 502, 503, 504],
                      allowed_methods=["GET"], raise_on_status=False)
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _tls.session = s
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


# ==============================================================================
# RENOVAÇÃO DE TOKEN
# ==============================================================================
def renovar_token(cli, token_que_falhou):
    with _lock_auth:
        if cli.get("access_token") != token_que_falhou:
            return True
        client_name = cli["des_clients"]
        log("🔄 Renovando token...", client=client_name)
        payload = {
            "grant_type": "refresh_token",
            "client_id": cli["client_id"],
            "client_secret": cli["secret_id"],
            "refresh_token": cli["refresh_token"],
        }
        try:
            r = requests.post(AUTH_URL, data=payload, timeout=30)
            if r.status_code != 200:
                log(f"❌ Renovação HTTP {r.status_code}: {r.text[:200]}", client=client_name)
                return False
            data = r.json()
            new_at = data["access_token"]
            new_rt = data.get("refresh_token", cli["refresh_token"])
            sql = f"""UPDATE `{TBL_CLIENTES}` SET access_token=@at, refresh_token=@rt,
                      dt_atualizacao=CURRENT_TIMESTAMP() WHERE client_id=@cid"""
            cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("at", "STRING", new_at),
                bigquery.ScalarQueryParameter("rt", "STRING", new_rt),
                bigquery.ScalarQueryParameter("cid", "STRING", cli["client_id"]),
            ])
            for t in range(6):
                try:
                    BQ.query(sql, job_config=cfg).result()
                    break
                except Exception as e:
                    if any(x in str(e) for x in ["Too many DML", "rateLimitExceeded", "resources exceeded"]) and t < 5:
                        time.sleep(random.uniform(2, 8) * (1.2 ** t))
                    else:
                        raise
            cli["access_token"] = new_at
            cli["refresh_token"] = new_rt
            cli["auth_failed"] = False
            log("✅ Token renovado.", client=client_name)
            return True
        except Exception as e:
            log(f"⚠️ Renovação: {e}", client=client_name)
            return False


# ==============================================================================
# WATERMARK
# ==============================================================================
def get_watermark_date(client_name, entity):
    try:
        sql = f"""SELECT CAST(watermark_date AS STRING) AS wm FROM `{TBL_WATERMARK}`
                  WHERE des_clients=@c AND entity=@e LIMIT 1"""
        df = BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("c", "STRING", client_name),
            bigquery.ScalarQueryParameter("e", "STRING", entity),
        ])).to_dataframe()
        if not df.empty and df["wm"].iloc[0]:
            return str(df["wm"].iloc[0])[:10]
    except Exception as e:
        log(f"⚠️ Watermark: {e}", client=client_name, ent=entity)

    parent = CHILD_CONFIG.get(entity, {}).get("parent")
    if parent:
        siblings = [k for k, v in CHILD_CONFIG.items() if v["parent"] == parent and k != entity]
        if siblings:
            try:
                sql = f"""SELECT MAX(CAST(watermark_date AS STRING)) AS wm
                          FROM `{TBL_WATERMARK}`
                          WHERE des_clients=@c AND entity IN UNNEST(@sibs)"""
                df = BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("c", "STRING", client_name),
                    bigquery.ArrayQueryParameter("sibs", "STRING", siblings),
                ])).to_dataframe()
                if not df.empty and df["wm"].iloc[0]:
                    inherited = str(df["wm"].iloc[0])[:10]
                    log(f"📎 Watermark herdado de irmãs: {inherited}", client=client_name, ent=entity)
                    return inherited
            except Exception as e:
                log(f"⚠️ Watermark herança: {e}", client=client_name, ent=entity)

    return DEFAULT_WATERMARK_DATE

def upsert_watermark_date(client_name, entity, watermark_date):
    sql = f"""MERGE `{TBL_WATERMARK}` T
              USING (SELECT @c AS des_clients, @e AS entity, DATE(@wm) AS watermark_date) S
              ON T.des_clients=S.des_clients AND T.entity=S.entity
              WHEN MATCHED THEN UPDATE SET watermark_date=S.watermark_date, updated_at=CURRENT_TIMESTAMP()
              WHEN NOT MATCHED THEN INSERT (des_clients,entity,watermark_date,updated_at)
              VALUES (S.des_clients,S.entity,S.watermark_date,CURRENT_TIMESTAMP())"""
    for t in range(6):
        try:
            BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("c", "STRING", client_name),
                bigquery.ScalarQueryParameter("e", "STRING", entity),
                bigquery.ScalarQueryParameter("wm", "STRING", watermark_date),
            ])).result()
            return
        except Exception as e:
            if any(x in str(e) for x in ["Too many DML", "rateLimitExceeded", "resources exceeded"]) and t < 5:
                time.sleep(random.uniform(2, 10) * (1.2 ** t))
            else:
                log(f"⚠️ Watermark save: {str(e)[:160]}", client=client_name, ent=entity)
                return


# ==============================================================================
# BQ HELPERS
# ==============================================================================
def ensure_dataset(dataset):
    ds = bigquery.Dataset(f"{PROJECT_ID}.{dataset}")
    ds.location = BQ_LOCATION
    BQ.create_dataset(ds, exists_ok=True)

def list_parent_ids_since_watermark(dataset, parent_table, parent_date_col, watermark_date, client_name="", entity=""):
    full = f"{PROJECT_ID}.{dataset}.{parent_table}"
    try:
        BQ.get_table(full)
    except Exception:
        log(f"⚠️ Tabela pai NÃO EXISTE: {full}", client=client_name, ent=entity)
        return []
    try:
        sql_count = f"SELECT COUNT(DISTINCT id) AS total FROM `{full}`"
        total = int(BQ.query(sql_count).to_dataframe()["total"].iloc[0])
        if total == 0:
            log(f"⚠️ Tabela pai VAZIA: {full}", client=client_name, ent=entity)
            return []

        sql = f"""SELECT DISTINCT CAST(id AS STRING) AS id FROM `{full}`
                  WHERE SAFE_CAST({parent_date_col} AS DATE) >= DATE(@wm) ORDER BY id"""
        df = BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("wm", "STRING", watermark_date),
        ])).to_dataframe()
        ids = df["id"].astype(str).tolist() if not df.empty else []

        if ids:
            log(f"📊 {len(ids)}/{total} IDs (watermark={watermark_date})", client=client_name, ent=entity)
            return ids

        sql_valid = f"""SELECT
            COUNTIF(SAFE_CAST({parent_date_col} AS DATE) IS NOT NULL) AS datas_validas,
            MAX(SAFE_CAST({parent_date_col} AS DATE)) AS max_data
            FROM `{full}`"""
        df_valid = BQ.query(sql_valid).to_dataframe()
        datas_validas = int(df_valid["datas_validas"].iloc[0])
        max_data = df_valid["max_data"].iloc[0]

        if datas_validas == 0:
            log(f"⚠️ FALLBACK: {total} IDs no pai, 0 datas válidas. Buscando TODOS.", client=client_name, ent=entity)
            return _list_all_ids_raw(full)

        if max_data is not None and str(max_data) < watermark_date:
            log(f"⚠️ Watermark ({watermark_date}) > max data pai ({max_data}). Nenhum ID novo.", client=client_name, ent=entity)
            return []

        log(f"⚠️ FALLBACK: {total} IDs, {datas_validas} válidas, 0 >= watermark. Buscando TODOS.", client=client_name, ent=entity)
        return _list_all_ids_raw(full)

    except Exception as e:
        log(f"⚠️ IDs pai: {e}", client=client_name, ent=entity)
        return []

def _list_all_ids_raw(full_table):
    try:
        sql = f"SELECT DISTINCT CAST(id AS STRING) AS id FROM `{full_table}` ORDER BY id"
        df = BQ.query(sql).to_dataframe()
        return df["id"].astype(str).tolist() if not df.empty else []
    except Exception as e:
        log(f"⚠️ IDs pai (all): {e}")
        return []

def list_all_parent_ids(dataset, parent_table, client_name="", entity=""):
    full = f"{PROJECT_ID}.{dataset}.{parent_table}"
    try:
        BQ.get_table(full)
    except Exception:
        log(f"⚠️ Tabela pai NÃO EXISTE: {full}", client=client_name, ent=entity)
        return []
    return _list_all_ids_raw(full)

def list_existing_child_ids(dataset, entity, client_name="", ent=""):
    """
    Retorna conjunto de id_pai já presentes na tabela filha.
    v6f: AGORA EXCLUI os marcados com _status='FETCH_ERROR' (mas mantém VAZIO),
    para que erros transitórios sejam retentados na próxima execução.
    """
    full = f"{PROJECT_ID}.{dataset}.{child_table_name(entity)}"
    try:
        BQ.get_table(full)
    except Exception:
        return set()
    try:
        # Verifica se a tabela tem coluna _status
        tbl = BQ.get_table(full)
        cols = {f.name for f in tbl.schema}
        if "_status" in cols:
            sql = f"""SELECT DISTINCT CAST(id_pai AS STRING) AS id_pai
                      FROM `{full}`
                      WHERE _status IS NULL OR _status != 'FETCH_ERROR'"""
        else:
            sql = f"SELECT DISTINCT CAST(id_pai AS STRING) AS id_pai FROM `{full}`"
        df = BQ.query(sql).to_dataframe()
        return set(df["id_pai"].astype(str).tolist()) if not df.empty else set()
    except Exception as e:
        log(f"⚠️ Leitura child IDs: {e}", client=client_name, ent=ent)
        return set()

def get_max_parent_date_for_ids(dataset, parent_table, parent_date_col, ids):
    if not ids:
        return None
    full = f"{PROJECT_ID}.{dataset}.{parent_table}"
    try:
        sql = f"""SELECT MAX(SAFE_CAST({parent_date_col} AS DATE)) AS mx FROM `{full}`
                  WHERE CAST(id AS STRING) IN UNNEST(@ids)"""
        df = BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("ids", "STRING", ids),
        ])).to_dataframe()
        if not df.empty and df["mx"].iloc[0] is not None:
            return str(df["mx"].iloc[0])
    except:
        pass
    return None

def delete_existing_ids(client_name, dataset, entity, ids):
    full = f"{PROJECT_ID}.{dataset}.{child_table_name(entity)}"
    try:
        BQ.get_table(full)
    except:
        return True
    sql = f"DELETE FROM `{full}` WHERE id_pai IN UNNEST(@ids)"
    params = [bigquery.ArrayQueryParameter("ids", "STRING", ids)]
    for t in range(6):
        try:
            BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
            return True
        except Exception as e:
            msg = str(e)
            if "Unrecognized name: id_pai" in msg:
                return True
            if any(x in msg for x in ["Too many DML", "rateLimitExceeded", "resources exceeded", "backendError"]) and t < 5:
                time.sleep(random.uniform(2, 10) * (1.2 ** t))
            else:
                log(f"❌ DELETE: {msg[:200]}", client=client_name, ent=entity)
                return False
    return False

def normalize_df(df):
    df = df.copy()
    df.columns = df.columns.str.replace(".", "_", regex=False)
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x, ensure_ascii=False)
            if isinstance(x, (dict, list))
            else (str(x) if x is not None else None)
        )
        df[col] = df[col].replace(["None", "nan", "NaN", "<NA>"], None)
    return df

def load_batch_to_bq(dataset, entity, dfs):
    if not dfs:
        return 0
    dfs = [d for d in dfs if not d.empty and not d.isna().all(axis=None)]
    if not dfs:
        return 0
    full = f"{PROJECT_ID}.{dataset}.{child_table_name(entity)}"
    df_all = normalize_df(pd.concat(dfs, ignore_index=True))
    rows = []
    for rec in df_all.to_dict(orient="records"):
        rows.append({
            k: (str(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None)
            for k, v in rec.items()
        })
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    schema_str = [bigquery.SchemaField(k, "STRING") for k in sorted(all_keys)]
    try:
        existing = BQ.get_table(full)
        ef = {f.name: f for f in existing.schema}
        final_schema = list(ef.values()) + [f for f in schema_str if f.name not in ef]
    except:
        final_schema = schema_str
    jc = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=final_schema,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        autodetect=False,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=False,
    )
    BQ.load_table_from_json(rows, full, job_config=jc).result()
    return len(rows)


# ==============================================================================
# EXPANSÃO DE ITENS DO PEDIDO
# ==============================================================================
def expand_pedido_itens(res_json: dict, id_pai: str, now_iso: str) -> pd.DataFrame:
    pagamento         = res_json.get("pagamento") or {}
    forma_pgto        = pagamento.get("formaPagamento") or {}
    forma_receb       = pagamento.get("formaRecebimento") or {}
    parcelas_list     = pagamento.get("parcelas") or []
    natureza          = res_json.get("naturezaOperacao") or {}
    deposito_obj      = res_json.get("deposito") or {}
    endereco_entrega  = res_json.get("enderecoEntrega") or {}

    header = {
        "valorFrete":                res_json.get("valorFrete"),
        "valorDesconto":             res_json.get("valorDesconto"),
        "valorOutrasDespesas":       res_json.get("valorOutrasDespesas"),
        "valorTotalProdutos":        res_json.get("valorTotalProdutos"),
        "valorTotalPedido":          res_json.get("valorTotalPedido"),
        "dataFaturamento":           res_json.get("dataFaturamento"),
        "pagamento_formaPagamento_nome": forma_pgto.get("nome") or forma_receb.get("nome"),
        "pagamento_parcelas":        str(len(parcelas_list)) if parcelas_list else None,
        "naturezaOperacao_nome":     natureza.get("nome"),
        "deposito_nome":             deposito_obj.get("nome"),
        "enderecoEntrega_municipio": endereco_entrega.get("municipio"),
        "enderecoEntrega_uf":        endereco_entrega.get("uf"),
        "enderecoEntrega_cep":       endereco_entrega.get("cep"),
    }

    itens = res_json.get("itens") or []

    if not itens:
        row = {"id_pai": id_pai, "data_extracao": now_iso, "_status": "SEM_ITENS"}
        for k, v in header.items():
            row[k] = str(v) if v is not None else None
        return pd.DataFrame([row])

    rows = []
    for item in itens:
        produto = item.get("produto") or {}
        row = {
            "id_pai":            id_pai,
            "data_extracao":     now_iso,
            "produto_id":        str(produto.get("id"))         if produto.get("id")         is not None else None,
            "produto_sku":       str(produto.get("sku"))        if produto.get("sku")        is not None else None,
            "produto_descricao": str(produto.get("descricao"))  if produto.get("descricao")  is not None else None,
            "produto_tipo":      str(produto.get("tipo"))       if produto.get("tipo")       is not None else None,
            "quantidade":        str(item.get("quantidade"))    if item.get("quantidade")    is not None else None,
            "valorUnitario":     str(item.get("valorUnitario")) if item.get("valorUnitario") is not None else None,
            "infoAdicional":     str(item.get("infoAdicional")) if item.get("infoAdicional") is not None else None,
        }
        for k, v in header.items():
            row[k] = str(v) if v is not None else None
        rows.append(row)

    return pd.DataFrame(rows)


# ==============================================================================
# BUSCA DE DETALHE
# ==============================================================================
def parse_payload(res_json):
    if not isinstance(res_json, dict):
        return res_json if isinstance(res_json, list) else [{"valor": str(res_json)}]

    data_val = res_json.get("data")
    if isinstance(data_val, dict):
        return [data_val]
    if isinstance(data_val, list):
        return data_val

    if "id" in res_json:
        return [res_json]

    for key in ("itens", "registro", "registros", "items"):
        val = res_json.get(key)
        if isinstance(val, dict):
            return [val]
        if isinstance(val, list):
            return val

    return [res_json]


def fetch_detail(cli, entity, id_pai):
    """
    Retorna:
    - pd.DataFrame com dados → sucesso
    - "NOT_FOUND" → 404/410 (definitivo, marca como VAZIO no BQ)
    - "FETCH_ERROR" → 429 esgotado, timeout, exceção (TRANSITÓRIO, NÃO grava nada)
    - "AUTH_ERROR" → auth falhou após retries
    - "SEM_ACESSO" → 403
    """
    if cli.get("auth_failed"):
        return "AUTH_ERROR"

    url = f"{API_BASE}/{CHILD_CONFIG[entity]['path'].format(id=id_pai)}"

    # Loop de auth retries
    for auth_attempt in range(MAX_AUTH_RETRIES):
        if cli.get("auth_failed"):
            return "AUTH_ERROR"
        token_atual = cli["access_token"]
        session = get_session(token_atual)

        # ═══ v6f: Loop interno de retries de rate limit ═══
        consecutive_429 = 0
        try:
            for rl_attempt in range(MAX_RETRIES_429 + 1):
                try:
                    r = session.get(url, timeout=25)
                except requests.exceptions.Timeout:
                    log(f"⏱ Timeout id={id_pai} (tentativa {rl_attempt+1})",
                        client=cli["des_clients"], ent=entity)
                    # Timeout: tenta de novo no mesmo loop, mas se já estiver no último, FETCH_ERROR
                    if rl_attempt < MAX_RETRIES_429:
                        time.sleep(2 + random.uniform(0, 2))
                        continue
                    return "FETCH_ERROR"
                except Exception as e:
                    log(f"⚠️ Conexão id={id_pai}: {str(e)[:80]}",
                        client=cli["des_clients"], ent=entity)
                    return "FETCH_ERROR"

                # 401 — quebra esse loop interno para tentar renovar token
                if r.status_code == 401:
                    break

                if r.status_code == 403:
                    return "SEM_ACESSO"

                # 404/410 — pedido realmente não existe no Tiny
                if r.status_code in (404, 410):
                    return "NOT_FOUND"

                if r.status_code == 429:
                    consecutive_429 += 1
                    # backoff exponencial: 5s, 10s, 20s, 40s, 80s
                    wait = (5 * (2 ** rl_attempt)) + random.uniform(1, 3)
                    log(f"⏱ Rate limit id={id_pai} (tentativa {rl_attempt+1}/{MAX_RETRIES_429+1}), "
                        f"aguardando {wait:.0f}s",
                        client=cli["des_clients"], ent=entity)
                    if rl_attempt < MAX_RETRIES_429:
                        time.sleep(wait)
                        continue
                    # Esgotou — NÃO marca como VAZIO, é erro transitório
                    log(f"❌ FETCH_ERROR id={id_pai}: rate limit não cedeu após "
                        f"{MAX_RETRIES_429+1} tentativas (tentará novamente)",
                        client=cli["des_clients"], ent=entity)
                    return "FETCH_ERROR"

                # Outro erro HTTP que não seja 200
                if r.status_code != 200:
                    log(f"⚠️ HTTP {r.status_code} id={id_pai}: {r.text[:100] if r.text else ''}",
                        client=cli["des_clients"], ent=entity)
                    # Erros 5xx são transitórios — não marca como vazio
                    if r.status_code >= 500:
                        return "FETCH_ERROR"
                    # 4xx genérico (não 401/403/404/410/429) — também trata como transitório
                    # por precaução, pra não perder dados
                    return "FETCH_ERROR"

                # ═══ 200 OK — processar payload ═══
                try:
                    res_json = r.json()
                except Exception as e:
                    log(f"⚠️ JSON parse id={id_pai}: {str(e)[:80]}",
                        client=cli["des_clients"], ent=entity)
                    return "FETCH_ERROR"

                now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f UTC")

                # Pedidos: expansão especial (1 linha por item)
                if entity == "pedidos":
                    df = expand_pedido_itens(res_json, str(id_pai), now_iso)
                    return df if not df.empty else "NOT_FOUND"

                # Comportamento genérico
                itens = parse_payload(res_json)
                if not itens:
                    return "NOT_FOUND"
                df = pd.json_normalize(itens)
                df["id_pai"] = str(id_pai)
                df["data_extracao"] = now_iso
                return df

            # Se chegou aqui via break do 401, tenta renovar token
            if auth_attempt < MAX_AUTH_RETRIES - 1 and renovar_token(cli, token_atual):
                continue
            cli["auth_failed"] = True
            return "AUTH_ERROR"

        except Exception as e:
            log(f"⚠️ Fetch id={id_pai}: {str(e)[:100]}",
                client=cli["des_clients"], ent=entity)
            return "FETCH_ERROR"

    return "FETCH_ERROR"


# ==============================================================================
# CANCELAMENTO
# ==============================================================================
def cancel_pending_futures(futures, client_name, ent):
    c = sum(1 for f in futures if f.cancel())
    if c:
        log(f"⚡ {c} requests cancelados.", client=client_name, ent=ent)
    return c


# ==============================================================================
# PROCESSAMENTO POR ENTIDADE
# ==============================================================================
def processar_entidade(cli, entity, dataset):
    t0 = time.time()
    client_name = cli["des_clients"]
    cfg = CHILD_CONFIG[entity]
    parent, date_col = cfg["parent"], cfg["date"]

    if cli.get("auth_failed"):
        log("🔄 Tentando recuperar token para esta entidade...", client=client_name, ent=entity)
        token_antigo = cli.get("access_token", "")
        if renovar_token(cli, token_antigo):
            cli["auth_failed"] = False
            log("✅ Token recuperado!", client=client_name, ent=entity)
        else:
            log("❌ Token irrecuperável. Pulando.", client=client_name, ent=entity)
            registrar_auditoria(client_name, entity, "ERRO_AUTH", 0, 0)
            return "ERRO_AUTH"

    # 1. Determinar IDs alvo
    if parent in FULL_PARENTS or FORCE_FULL_REFRESH:
        all_ids = list_all_parent_ids(dataset, parent, client_name=client_name, entity=entity)
        log(f"📂 {len(all_ids)} IDs no pai (FULL)", client=client_name, ent=entity)
    else:
        wm = get_watermark_date(client_name, entity)
        if REFRESH_DAYS > 0:
            refresh_date = (date.today() - timedelta(days=REFRESH_DAYS)).isoformat()
            if refresh_date < wm:
                wm = refresh_date
                log(f"⏱ watermark={wm} (REFRESH_DAYS={REFRESH_DAYS})", client=client_name, ent=entity)
            else:
                log(f"⏱ watermark={wm} (já anterior ao refresh)", client=client_name, ent=entity)
        else:
            log(f"⏱ watermark={wm}", client=client_name, ent=entity)
        all_ids = list_parent_ids_since_watermark(dataset, parent, date_col, wm,
                                              client_name=client_name, entity=entity)

    if not all_ids:
        log("⚠️ Sem IDs.", client=client_name, ent=entity)
        registrar_auditoria(client_name, entity, "SEM_NOVOS", 0, time.time() - t0)
        return "SEM_NOVOS"

    if FORCE_FULL_REFRESH or REFRESH_DAYS > 0:
        ids = all_ids
        if FORCE_FULL_REFRESH:
            log(f"📂 {len(ids)} IDs (FORCE_FULL_REFRESH → carga completa)",
                client=client_name, ent=entity)
        else:
            log(f"📂 {len(ids)} IDs (REFRESH_DAYS={REFRESH_DAYS} → DELETE+INSERT)",
                client=client_name, ent=entity)
    else:
        already_done = list_existing_child_ids(dataset, entity, client_name=client_name, ent=entity)
        if already_done:
            ids = [i for i in all_ids if i not in already_done]
            log(f"📂 {len(all_ids)} IDs pai, {len(already_done)} já na filha → {len(ids)} novos",
                client=client_name, ent=entity)
        else:
            ids = all_ids
            log(f"📂 {len(ids)} IDs (tabela filha vazia/inexistente → carga completa)",
                client=client_name, ent=entity)

    if not ids:
        log("✅ Todos os IDs já processados.", client=client_name, ent=entity)
        if parent not in FULL_PARENTS:
            mx = get_max_parent_date_for_ids(dataset, parent, date_col, all_ids)
            if mx:
                upsert_watermark_date(client_name, entity, mx)
        registrar_auditoria(client_name, entity, "OK", 0, time.time() - t0)
        return "OK (0 - já completo)"

    do_delete_per_batch = FORCE_FULL_REFRESH or REFRESH_DAYS > 0

    # ═══ v6f: contadores de erro para reportar ═══
    total_salvo = 0
    fetch_errors = 0
    not_found_count = 0
    dfs_buf, rows_buf, skip, sem_acesso = [], 0, False, False
    ids_processados_count = 0
    all_ids_done = True

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        id_batches = [ids[i:i + BATCH_SUBMIT] for i in range(0, len(ids), BATCH_SUBMIT)]
        for bi, batch in enumerate(id_batches):
            if cli.get("auth_failed") or skip or sem_acesso:
                remaining = sum(len(b) for b in id_batches[bi:])
                log(f"⚡ {remaining} IDs não submetidos (corte).", client=client_name, ent=entity)
                all_ids_done = False
                break

            futs = {ex.submit(fetch_detail, cli, entity, idp): idp for idp in batch}
            for f in as_completed(futs):
                if cli.get("auth_failed"):
                    cancel_pending_futures(futs, client_name, entity)
                    skip = True
                    all_ids_done = False
                    break
                res = f.result()

                if isinstance(res, str):
                    if res == "AUTH_ERROR":
                        cancel_pending_futures(futs, client_name, entity)
                        skip = True
                        all_ids_done = False
                        break
                    if res == "SEM_ACESSO":
                        cancel_pending_futures(futs, client_name, entity)
                        sem_acesso = True
                        all_ids_done = False
                        break
                    if res == "NOT_FOUND":
                        # ═══ v6f: 404/410 → marca como VAZIO (é definitivo) ═══
                        id_vazio = futs[f]
                        df_vazio = pd.DataFrame([{
                            "id_pai": str(id_vazio),
                            "_status": "VAZIO",
                            "data_extracao": datetime.now(timezone.utc).strftime(
                                "%Y-%m-%dT%H:%M:%S.%f UTC"
                            ),
                        }])
                        dfs_buf.append(df_vazio)
                        rows_buf += 1
                        not_found_count += 1
                    elif res == "FETCH_ERROR":
                        # ═══ v6f: erro transitório → NÃO grava nada ═══
                        # O ID fica fora da tabela filha → próxima execução tenta de novo
                        fetch_errors += 1
                        # all_ids_done = False  # IMPORTANTE: NÃO avançar watermark se houver FETCH_ERROR
                        all_ids_done = False
                elif isinstance(res, pd.DataFrame) and not res.empty:
                    dfs_buf.append(res)
                    rows_buf += len(res)
                # res None ou outro tipo: ignora (não deveria acontecer mais)

                ids_processados_count += 1
                if rows_buf >= BATCH_ROWS:
                    try:
                        if do_delete_per_batch:
                            ids_no_batch = list(set(
                                str(row)
                                for df in dfs_buf if isinstance(df, pd.DataFrame) and "id_pai" in df.columns
                                for row in df["id_pai"].dropna().unique()
                            ))
                            if ids_no_batch:
                                delete_existing_ids(client_name, dataset, entity, ids_no_batch)
                        total_salvo += load_batch_to_bq(dataset, entity, dfs_buf)
                    except Exception as e:
                        log(f"❌ BQ flush: {e}", client=client_name, ent=entity)
                        cancel_pending_futures(futs, client_name, entity)
                        skip = True
                        all_ids_done = False
                        break
                    dfs_buf, rows_buf = [], 0
                    pct = ids_processados_count * 100 // len(ids)
                    log(f"📈 {ids_processados_count}/{len(ids)} ({pct}%) | {total_salvo} salvos | "
                        f"errors={fetch_errors} | {time.time()-t0:.0f}s",
                        client=client_name, ent=entity)

    # Flush final
    if dfs_buf and not sem_acesso:
        try:
            if do_delete_per_batch:
                ids_no_batch = list(set(
                    str(row)
                    for df in dfs_buf if isinstance(df, pd.DataFrame) and "id_pai" in df.columns
                    for row in df["id_pai"].dropna().unique()
                ))
                if ids_no_batch:
                    delete_existing_ids(client_name, dataset, entity, ids_no_batch)
            total_salvo += load_batch_to_bq(dataset, entity, dfs_buf)
        except Exception as e:
            log(f"❌ BQ final: {e}", client=client_name, ent=entity)
            registrar_auditoria(client_name, entity, "ERRO_BQ", total_salvo, time.time() - t0)
            return "ERRO_BQ"

    if sem_acesso:
        log("🔒 Sem acesso (403).", client=client_name, ent=entity)
        registrar_auditoria(client_name, entity, "SEM_ACESSO", 0, time.time() - t0)
        return "SEM_ACESSO"
    if skip or cli.get("auth_failed"):
        log(f"⚠️ Parcial: {total_salvo} salvos, watermark NÃO avançado.", client=client_name, ent=entity)
        registrar_auditoria(client_name, entity, "ERRO_AUTH", total_salvo, time.time() - t0)
        return "ERRO_AUTH"

    # ═══ v6f: SÓ avança watermark se NÃO houve FETCH_ERROR ═══
    # Isso garante que pedidos com erro transitório sejam retentados.
    if all_ids_done and parent not in FULL_PARENTS:
        mx = get_max_parent_date_for_ids(dataset, parent, date_col, all_ids)
        if mx:
            upsert_watermark_date(client_name, entity, mx)
            log(f"💾 Watermark atualizado: {mx}", client=client_name, ent=entity)
    elif fetch_errors > 0:
        log(f"⚠️ {fetch_errors} FETCH_ERRORS — watermark NÃO avançado (próxima execução retenta)",
            client=client_name, ent=entity)

    dur = time.time() - t0
    status_msg = f"{total_salvo} registros em {dur:.1f}s."
    if not_found_count:
        status_msg += f" 🗑 {not_found_count} NOT_FOUND."
    if fetch_errors:
        status_msg += f" ❌ {fetch_errors} FETCH_ERRORS (retentar)."
    log(f"✅ {status_msg}", client=client_name, ent=entity)
    registrar_auditoria(client_name, entity, "OK", total_salvo, dur)

    if fetch_errors > 0:
        return f"OK ({total_salvo}, ❌{fetch_errors} retentar)"
    return f"OK ({total_salvo})"


# ==============================================================================
# PROCESSAMENTO POR CLIENTE
# ==============================================================================
def get_tabelas_pai_existentes(dataset):
    """
    v6f-opt: Lista as tabelas PAI que existem no dataset RAW do cliente.
    Permite pular entidades que o cliente nao usa (evita ~25 consultas BQ por execucao).
    """
    try:
        tables = list(BQ.list_tables(f"{PROJECT_ID}.{dataset}"))
        return {t.table_id for t in tables}
    except Exception as e:
        log(f"⚠️ list_tables: {str(e)[:80]}")
        return None


def processar_cliente(cli):
    client_name = cli["des_clients"]
    dataset = dataset_name_from_client(client_name)
    ents = (
        list(CHILD_CONFIG.keys())
        if ONLY_ENTS == "ALL"
        else [e.strip() for e in ONLY_ENTS.split(",") if e.strip()]
    )

    if not FORCE_RERUN and cliente_ja_concluido_hoje(client_name):
        log("⏭️ Já concluído hoje.", client=client_name)
        return {"_status": "JA_CONCLUIDO"}

    ensure_dataset(dataset)

    # v6f-opt: lista tabelas PAI existentes para filtrar entidades irrelevantes
    tabelas_existentes = get_tabelas_pai_existentes(dataset)
    if tabelas_existentes is not None:
        ents_filtradas = [
            e for e in ents
            if e in CHILD_CONFIG and CHILD_CONFIG[e]["parent"] in tabelas_existentes
        ]
        puladas = len(ents) - len(ents_filtradas)
        if puladas > 0:
            log(f"⚡ Skip de {puladas} entidades sem tabela pai. Processando {len(ents_filtradas)}/{len(ents)}.",
                client=client_name)
        ents = ents_filtradas

    log("🚀 Iniciando FILHOS...", client=client_name)

    resultados = {}
    for ent in ents:
        if ent not in CHILD_CONFIG:
            log(f"⚠️ Entidade '{ent}' não está no CHILD_CONFIG. Ignorando.", client=client_name)
            continue
        resultados[ent] = processar_entidade(cli, ent, dataset)

    registrar_conclusao_diaria(client_name)
    log("🏁 Finalizado.", client=client_name)
    return resultados


# ==============================================================================
# MAIN
# ==============================================================================
def fetch_clientes():
    # v6f: filtra clientes vivos (dt_atualizacao < 25h)
    # Clientes inativos (refresh_token expirado, sem re-auth) ficam com dt_atualizacao antiga.
    sql = f"""SELECT * FROM `{TBL_CLIENTES}`
              WHERE access_token IS NOT NULL
                AND dt_atualizacao IS NOT NULL
                AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), dt_atualizacao, HOUR) < 25"""
    df = BQ.query(sql).to_dataframe()
    return sorted(df.to_dict("records"), key=lambda x: (x.get("des_clients") or "").lower())

def filtrar_clientes_local(clientes):
    if ONLY_CLIENTS == "ALL":
        return clientes
    wanted = [x.strip().lower().replace("cliente_", "") for x in ONLY_CLIENTS.split(",") if x.strip()]
    filtered = []
    for c in clientes:
        nome_bq = (c.get("des_clients") or "").lower().replace("cliente_", "").strip()
        if nome_bq in wanted:
            filtered.append(c)
    return filtered

def formatar_bloco_slack(nome, resultados):
    linhas = [f"🏢 *{nome}* (FILHOS v6f)"]
    for ent, st in resultados.items():
        if st.startswith("OK"):
            e = "✅" if "retentar" not in st else "⚠️"
        elif st in ("SEM_NOVOS", "SEM_ACESSO"):
            e = "⚠️"
        else:
            e = "❌"
        linhas.append(f"   {e} {ent}: {st}")
    return "\n".join(linhas)

def run_sequential(clientes):
    relatorio = []
    for cli in filtrar_clientes_local(clientes):
        try:
            relatorio.append(formatar_bloco_slack(cli["des_clients"], processar_cliente(cli)))
        except Exception as e:
            relatorio.append(f"🏢 *{cli['des_clients']}* (FILHOS)\n   ❌ {str(e)[:80]}")
    return relatorio

def enviar_relatorio_slack(relatorio, duracao, modo):
    buf = f"🏁 *Job FILHOS v6f Finalizado ({modo})* | Duração: {duracao}\n"
    for b in relatorio:
        if len(buf) + len(b) > 3800:
            enviar_slack(buf)
            buf = b
        else:
            buf += f"\n{b}"
    enviar_slack(buf)

def main():
    print(f">>> [START] append_filho_v6f | Task Index: {os.getenv('CLOUD_RUN_TASK_INDEX')}", flush=True)
    print(f">>> [CONFIG] FORCE_RERUN={FORCE_RERUN} | REFRESH_DAYS={REFRESH_DAYS} | "
          f"FORCE_FULL_REFRESH={FORCE_FULL_REFRESH} | MAX_WORKERS={MAX_WORKERS} | "
          f"BATCH_SUBMIT={BATCH_SUBMIT} | MAX_RETRIES_429={MAX_RETRIES_429}", flush=True)
    print(f">>> [CONFIG] Entidades no CHILD_CONFIG: {len(CHILD_CONFIG)}", flush=True)

    start = datetime.now()
    ti = os.getenv("CLOUD_RUN_TASK_INDEX")

    todos_clientes = fetch_clientes()
    print(f">>> [DEBUG] Clientes totais no BQ: {len(todos_clientes)}", flush=True)

    clientes = filtrar_clientes_local(todos_clientes)
    print(f">>> [DEBUG] Clientes após filtro '{ONLY_CLIENTS[:50]}': {len(clientes)}", flush=True)

    if not clientes:
        print("❌ Nenhum cliente restou após a filtragem. Encerrando.", flush=True)
        return

    if ti is not None:
        idx = int(ti)
        if idx >= len(clientes):
            log(f"💤 Task {idx} ociosa (mais tasks que clientes filtrados).")
            return
        cli = clientes[idx]
        log(f"🚀 [TASK {idx}] FILHOS v6f: {cli['des_clients']}")
        resultado = processar_cliente(cli)
        enviar_slack(formatar_bloco_slack(cli["des_clients"], resultado))
        return

    log("⚠️ Modo local.")
    enviar_slack("🚀 *Job FILHOS v6f (Local)*")
    enviar_relatorio_slack(
        run_sequential(clientes),
        str(datetime.now() - start).split(".")[0],
        "Local",
    )

if __name__ == "__main__":
    main()
