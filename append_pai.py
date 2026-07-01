#!/usr/bin/env python3
# coding: utf-8
"""
Tiny → BigQuery | APPEND PAI — PRODUÇÃO v9
Atom Analytics

Correções v9 (2026-06-16):
- FIX: pedidos cadastrados retroativamente (dataCriacao no passado, mas inseridos
  dias depois no Tiny) agora são capturados.

  Problema: o scheduler diário usa watermark D-1. Se um pedido tem
  dataCriacao=15/06 mas foi cadastrado no Tiny em 16/06 09h, a execução do
  PAI do dia 17 (03h) com janela `data_ini=15/06, data_fim=16/06` deveria
  pegá-lo, mas na prática o filtro do Tiny não retorna pedidos cadastrados
  fora da janela esperada. Resultado: pedido fica de fora para sempre.

  Solução: nova env var PAI_OVERLAP_DAYS (default=3). Em execuções incrementais
  (sem LOOKBACK_DAYS), o data_ini é sempre `watermark - PAI_OVERLAP_DAYS`,
  garantindo que pedidos cadastrados retroativamente dentro dessa janela
  sejam capturados.

  Como o PAI já faz DELETE+INSERT por ID, não há risco de duplicação. E com
  a detecção de fantasmas do v8, também não há acúmulo de lixo.

  Exemplo (PAI_OVERLAP_DAYS=3):
    16/06 03h00 → watermark=15/06, data_ini=12/06, data_fim=15/06 (D-1)
                  reprocessa últimos 4 dias, captura retroativos.
                  watermark salvo = 15/06.
    17/06 03h00 → watermark=15/06, data_ini=12/06, data_fim=16/06 (D-1)
                  reprocessa últimos 5 dias, captura mais retroativos.
                  watermark salvo = 16/06.

  Em reprocessamento manual (LOOKBACK_DAYS > 0), o comportamento é o do
  LOOKBACK_DAYS, ignorando o overlap (não duplica).

Correções v8 (2026-06-16):
- FIX: pedidos/notas/contas DELETADOS no Tiny agora são removidos do BQ.
  Durante reprocessamento (LOOKBACK_DAYS > 0), compara IDs da API vs IDs no BQ.
  Diferença (BQ mas não API) = fantasmas, deletados automaticamente.

Correções v7 (2026-05-28):
- data_fim = D-1 (ontem) para entidades incrementais. Garante fechamento do dia.

Correções v6 (2026-05-13):
- WATERMARK EXPLÍCITO: tabela pai_watermarks. Só avança se OK.

Correções v5 (2026-02-25):
- AUTO-RETRY em 400 "fora do período permitido pelo plano"
- Paths confirmados contra Swagger
- Modo SEQUENCIAL (tasks=1 processa todos em 1 container)
"""

from __future__ import annotations

import os
import re
import time
import random
import threading
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Set

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
TBL_AUDITORIA  = f"{PROJECT_ID}.tiny_configs.tb_auditoria"
TBL_WATERMARK  = f"{PROJECT_ID}.tiny_configs.pai_watermarks"

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL") or os.getenv("WEBHOOK_URL")

ONLY_CLIENTS  = os.getenv("ONLY_CLIENTS", "ALL")
ONLY_ENTS     = os.getenv("ONLY_ENTITIES", "ALL")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))

# ═══ v9: overlap para capturar pedidos retroativos ═══
PAI_OVERLAP_DAYS = int(os.getenv("PAI_OVERLAP_DAYS", "3"))

DEFAULT_WATERMARK_DATE = os.getenv("DEFAULT_WATERMARK_DATE", "2020-01-01")

DATETIME_FORMAT_PARAMS = {"dataCriacao", "dataAlteracao", "dataAtualizacao"}
RE_PLAN_DATE = re.compile(r"a partir de (\d{2})/(\d{2})/(\d{4})")

# ==============================================================================
# MAPA DE CONFIGURAÇÃO DA API
# ==============================================================================
MAPA_CONFIG_API: Dict[str, Dict] = {
    "pedidos": {
        "campo_dt_bq": "dataCriacao",
        "api_p_ini":   "dataInicial",
        "api_p_fim":   "dataFinal",
        "path_api":    "pedidos",
        "write_mode":  "APPEND",
        "extra_params": {"situacoes": "0,1,2,3,4,5,6,7,8,9"}
    },
    "notas": {
        "campo_dt_bq": "dataEmissao",
        "api_p_ini":   "dataInicial",
        "api_p_fim":   "dataFinal",
        "path_api":    "notas",
        "write_mode":  "APPEND",
    },
    "contas_pagar": {
        "campo_dt_bq": "data",
        "api_p_ini":   "dataInicialEmissao",
        "api_p_fim":   "dataFinalEmissao",
        "path_api":    "contas-pagar",
        "write_mode":  "APPEND",
    },
    "contas_receber": {
        "campo_dt_bq": "data",
        "api_p_ini":   "dataInicialEmissao",
        "api_p_fim":   "dataFinalEmissao",
        "path_api":    "contas-receber",
        "write_mode":  "APPEND",
    },
    "ordem_compra": {
        "campo_dt_bq": "data",
        "api_p_ini":   "dataInicial",
        "api_p_fim":   "dataFinal",
        "path_api":    "ordem-compra",
        "write_mode":  "APPEND",
    },
    "ordem_servico": {
        "campo_dt_bq": "data",
        "api_p_ini":   "dataInicialEmissao",
        "api_p_fim":   "dataFinalEmissao",
        "path_api":    "ordem-servico",
        "write_mode":  "APPEND",
    },
    "separacao": {
        "campo_dt_bq": "dataCriacao",
        "api_p_ini":   "dataInicial",
        "api_p_fim":   "dataFinal",
        "path_api":    "separacao",
        "write_mode":  "APPEND",
    },
    "contatos": {
        "campo_dt_bq": "dataCriacao",
        "api_p_ini":   "dataCriacao",
        "api_p_fim":   None,
        "path_api":    "contatos",
        "write_mode":  "APPEND",
    },
    "produtos":                    {"campo_dt_bq": None, "api_p_ini": "dataCriacao", "api_p_fim": None, "path_api": "produtos",                "write_mode": "FULL"},
    "expedicao":                   {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "expedicao",                   "write_mode": "FULL"},
    "servicos":                    {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "servicos",                    "write_mode": "FULL"},
    "vendedores":                  {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "vendedores",                  "write_mode": "FULL"},
    "formas_pagamento":            {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "formas-pagamento",            "write_mode": "FULL"},
    "formas_envio":                {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "formas-envio",                "write_mode": "FULL"},
    "formas_recebimento":          {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "formas-recebimento",          "write_mode": "FULL"},
    "listas_precos":               {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "listas-precos",               "write_mode": "FULL"},
    "marcas":                      {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "marcas",                      "write_mode": "FULL"},
    "intermediadores":             {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "intermediadores",             "write_mode": "FULL"},
    "grupos_tags":                 {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "grupos-tags",                 "write_mode": "FULL"},
    "tags":                        {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "tags",                        "write_mode": "FULL"},
    "categorias":                  {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "categorias/todas",            "write_mode": "FULL", "sem_paginacao": True},
    "categorias_receita_despesa":  {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "categorias-receita-despesa",  "write_mode": "FULL"},
    "usuarios":                    {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "usuarios",                    "write_mode": "FULL"},
    "contatos_tipos":              {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "contatos/tipos",              "write_mode": "FULL"},
    "info":                        {"campo_dt_bq": None, "api_p_ini": None, "api_p_fim": None, "path_api": "info",                        "write_mode": "FULL", "sem_paginacao": True},
}

BQ = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
_lock_auth = threading.Lock()


# ==============================================================================
# LOG / SLACK
# ==============================================================================
def log(msg: str, client: str = "-") -> None:
    print(f"{datetime.now():%H:%M:%S} | {client:<28} | {msg}", flush=True)

def enviar_slack(texto: str) -> None:
    if not SLACK_WEBHOOK:
        return
    try:
        requests.post(SLACK_WEBHOOK, json={"text": texto}, timeout=15)
    except Exception as e:
        log(f"⚠️ Falha ao enviar Slack: {e}")


# ==============================================================================
# AUDITORIA
# ==============================================================================
def registrar_auditoria(modo, cliente, job_tipo, entidade, status, qtd, duracao):
    id_unico = int(time.time() * 1000) + random.randint(1, 999)
    rows = [{"id": id_unico, "modo": modo, "nm_cliente": cliente,
             "dt_execucao": datetime.now(timezone.utc).isoformat(),
             "job_tipo": job_tipo, "nm_entidade": entidade,
             "status": status, "qtd_registros": int(qtd),
             "tempo_carga_segundos": int(duracao)}]
    try:
        errors = BQ.insert_rows_json(TBL_AUDITORIA, rows)
        if errors: log(f"⚠️ Erros auditoria: {errors}", cliente)
    except Exception as e:
        log(f"⚠️ Erro auditoria: {e}", cliente)


# ==============================================================================
# NOMENCLATURA
# ==============================================================================
def dataset_name_from_client(des_clients: str) -> str:
    slug = des_clients.strip().lower().replace(" ", "_")
    if not slug.startswith("cliente_"): slug = f"cliente_{slug}"
    return f"{slug}_raw"


# ==============================================================================
# HTTP SESSION
# ==============================================================================
def make_session(token: str) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[500, 502, 503, 504],
                  allowed_methods=["GET"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


# ==============================================================================
# RENOVAÇÃO DE TOKEN
# ==============================================================================
def renovar_token(cli: Dict[str, Any], token_que_falhou: str) -> bool:
    with _lock_auth:
        if cli.get("access_token") != token_que_falhou:
            return True
        client_name = cli["des_clients"]
        log("🔄 Renovando token...", client_name)
        payload = {"grant_type": "refresh_token", "client_id": cli["client_id"],
                   "client_secret": cli["secret_id"], "refresh_token": cli["refresh_token"]}
        try:
            r = requests.post(AUTH_URL, data=payload, timeout=30)
            if r.status_code != 200:
                log(f"❌ Erro renovação: {r.status_code}", client_name)
                return False
            data = r.json()
            new_at, new_rt = data["access_token"], data.get("refresh_token", cli["refresh_token"])
            sql = f"""UPDATE `{TBL_CLIENTES}` SET access_token=@at, refresh_token=@rt,
                      dt_atualizacao=CURRENT_TIMESTAMP() WHERE client_id=@cid"""
            cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("at", "STRING", new_at),
                bigquery.ScalarQueryParameter("rt", "STRING", new_rt),
                bigquery.ScalarQueryParameter("cid", "STRING", cli["client_id"])])
            for t in range(6):
                try: BQ.query(sql, job_config=cfg).result(); break
                except Exception as e:
                    if any(x in str(e) for x in ["Too many DML","rateLimitExceeded","resources exceeded"]) and t<5:
                        time.sleep(random.uniform(2,8)*(1.2**t))
                    else: raise
            cli["access_token"], cli["refresh_token"] = new_at, new_rt
            log("✅ Token renovado.", client_name)
            return True
        except Exception as e:
            log(f"⚠️ Erro renovação: {e}", client_name)
            return False


# ==============================================================================
# WATERMARK
# ==============================================================================
def get_watermark_date(client_name: str, dataset: str, alias: str, campo_dt: str) -> str:
    if LOOKBACK_DAYS > 0 and campo_dt is not None:
        return (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    if not campo_dt:
        return DEFAULT_WATERMARK_DATE

    # 1. Tabela explícita pai_watermarks
    try:
        sql = f"""SELECT CAST(watermark_date AS STRING) AS wm
                  FROM `{TBL_WATERMARK}`
                  WHERE des_clients=@c AND entity=@e LIMIT 1"""
        df = BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("c", "STRING", client_name),
            bigquery.ScalarQueryParameter("e", "STRING", alias),
        ])).to_dataframe()
        if not df.empty and df["wm"].iloc[0]:
            wm = str(df["wm"].iloc[0])[:10]
            log(f"📅 Watermark [{alias}]: {wm} (tabela explícita)", client_name)
            return wm
    except Exception as e:
        log(f"⚠️ Watermark tabela [{alias}]: {e}", client_name)

    # 2. Fallback: MAX(campo_dt) na tabela raw (primeira execução)
    full_table = f"{PROJECT_ID}.{dataset}.{alias}"
    try:
        BQ.get_table(full_table)
        sql = f"SELECT MAX(SAFE_CAST({campo_dt} AS DATE)) AS wm FROM `{full_table}`"
        df = BQ.query(sql).to_dataframe()
        if not df.empty and df["wm"].iloc[0] is not None:
            wm = str(df["wm"].iloc[0])
            log(f"📅 Watermark [{alias}]: {wm} (MAX raw — primeira execução)", client_name)
            return wm
    except Exception as e:
        log(f"⚠️ Watermark MAX [{alias}]: {e}", client_name)

    return DEFAULT_WATERMARK_DATE


def upsert_watermark_date(client_name: str, alias: str, watermark_date: str) -> None:
    sql = f"""MERGE `{TBL_WATERMARK}` T
              USING (SELECT @c AS des_clients, @e AS entity, DATE(@wm) AS watermark_date) S
              ON T.des_clients=S.des_clients AND T.entity=S.entity
              WHEN MATCHED THEN UPDATE SET watermark_date=S.watermark_date, updated_at=CURRENT_TIMESTAMP()
              WHEN NOT MATCHED THEN INSERT (des_clients, entity, watermark_date, updated_at)
              VALUES (S.des_clients, S.entity, S.watermark_date, CURRENT_TIMESTAMP())"""
    for t in range(6):
        try:
            BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("c",  "STRING", client_name),
                bigquery.ScalarQueryParameter("e",  "STRING", alias),
                bigquery.ScalarQueryParameter("wm", "STRING", watermark_date),
            ])).result()
            log(f"💾 Watermark [{alias}] salvo: {watermark_date}", client_name)
            return
        except Exception as e:
            if any(x in str(e) for x in ["Too many DML","rateLimitExceeded","resources exceeded"]) and t < 5:
                time.sleep(random.uniform(2, 8) * (1.2 ** t))
            else:
                log(f"⚠️ Watermark save [{alias}]: {str(e)[:160]}", client_name)
                return


# ==============================================================================
# BQ HELPERS
# ==============================================================================
def ensure_dataset(dataset):
    ds = bigquery.Dataset(f"{PROJECT_ID}.{dataset}"); ds.location = BQ_LOCATION
    BQ.create_dataset(ds, exists_ok=True)

def delete_ids_from_table(table_id, ids, client_name, alias):
    try: BQ.get_table(table_id)
    except: return True
    sql = f"DELETE FROM `{table_id}` WHERE CAST(id AS STRING) IN UNNEST(@ids)"
    params = [bigquery.ArrayQueryParameter("ids", "STRING", ids)]
    for t in range(6):
        try: BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result(); return True
        except Exception as e:
            msg = str(e)
            if any(x in msg for x in ["Too many DML","rateLimitExceeded","resources exceeded","backendError"]) and t<5:
                time.sleep(random.uniform(2,10)*(1.2**t))
            else: log(f"❌ DELETE {alias}: {msg[:200]}", client_name); return False
    return False


# ═══ FIX v8: detecta IDs fantasmas (existem no BQ mas não vieram da API) ═══
def get_existing_ids_in_window(table_id: str, campo_dt: str, data_ini: str,
                                data_fim: str, client_name: str, alias: str) -> Set[str]:
    """
    Retorna conjunto de IDs no BQ dentro da janela [data_ini, data_fim].
    Usado para detectar fantasmas em reprocessamento.
    """
    try:
        BQ.get_table(table_id)
    except Exception:
        return set()
    try:
        sql = f"""SELECT DISTINCT CAST(id AS STRING) AS id FROM `{table_id}`
                  WHERE SAFE_CAST({campo_dt} AS DATE) BETWEEN DATE(@di) AND DATE(@df)"""
        df = BQ.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("di", "STRING", data_ini),
            bigquery.ScalarQueryParameter("df", "STRING", data_fim),
        ])).to_dataframe()
        if df.empty:
            return set()
        return set(df["id"].astype(str).tolist())
    except Exception as e:
        log(f"⚠️ get_existing_ids_in_window [{alias}]: {str(e)[:160]}", client_name)
        return set()


def normalize_df(df):
    df = df.copy(); df.columns = df.columns.str.replace(".", "_", regex=False)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
        df[col] = df[col].replace(["None","nan","NaN","<NA>"], None)
    return df


# ==============================================================================
# FORMATAÇÃO DE DATA / EXTRAÇÃO DE DATA DO PLANO
# ==============================================================================
def format_api_date(param_name, date_str, is_end=False):
    if param_name in DATETIME_FORMAT_PARAMS:
        return f"{date_str} {'23:59:59' if is_end else '00:00:00'}"
    return date_str

def extrair_data_plano(body):
    m = RE_PLAN_DATE.search(body)
    if m: return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


# ==============================================================================
# FLATTEN CATEGORIAS
# ==============================================================================
def flatten_categorias(node, path=""):
    rows = []
    if isinstance(node, dict):
        cp = f"{path}/{node.get('descricao','')}" if path else node.get("descricao","")
        rows.append({"id": node.get("id"), "descricao": node.get("descricao"), "caminho_completo": cp})
        for f in node.get("filhas", []): rows.extend(flatten_categorias(f, cp))
    elif isinstance(node, list):
        for item in node: rows.extend(flatten_categorias(item, path))
    return rows


# ═══ FIX v9: aplica overlap ao data_ini para capturar retroativos ═══
def aplicar_overlap(data_ini_str: str, overlap_days: int) -> str:
    """
    Subtrai overlap_days da data_ini para reprocessar uma janela maior.
    Garante captura de pedidos cadastrados retroativamente no Tiny.
    """
    try:
        d = datetime.strptime(data_ini_str, "%Y-%m-%d")
        return (d - timedelta(days=overlap_days)).strftime("%Y-%m-%d")
    except Exception:
        return data_ini_str


# ==============================================================================
# PROCESSAMENTO DE ENDPOINT
# ==============================================================================
def processar_endpoint(cli, alias, dataset):
    t0 = time.time()
    client_name = cli["des_clients"]
    cfg = MAPA_CONFIG_API[alias]
    campo_dt, api_p_ini, api_p_fim = cfg["campo_dt_bq"], cfg["api_p_ini"], cfg["api_p_fim"]
    path_api, write_mode = cfg["path_api"], cfg["write_mode"]
    is_inc = write_mode == "APPEND"
    sem_pag = cfg.get("sem_paginacao", False)
    table_id = f"{PROJECT_ID}.{dataset}.{alias}"
    modo_str = "RECALCULO" if LOOKBACK_DAYS > 0 else "APPEND" if is_inc else "FULL"

    # ═══ v7: data_fim = D-1 para incrementais ═══
    if is_inc and LOOKBACK_DAYS == 0:
        data_fim = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        data_fim = datetime.now().strftime("%Y-%m-%d")

    if os.getenv("BACKFILL_FIM"):
        data_fim = os.getenv("BACKFILL_FIM")

    if is_inc and api_p_ini:
        data_ini = get_watermark_date(client_name, dataset, alias, campo_dt)

        # ═══ FIX v9: aplica overlap APENAS em incremental (LOOKBACK_DAYS=0) ═══
        # Em reprocessamento manual, o LOOKBACK_DAYS já governa a janela.
        if LOOKBACK_DAYS == 0 and PAI_OVERLAP_DAYS > 0 and campo_dt:
            data_ini_original = data_ini
            data_ini = aplicar_overlap(data_ini, PAI_OVERLAP_DAYS)
            log(f"🔁 {alias.upper()} | overlap={PAI_OVERLAP_DAYS}d: "
                f"watermark={data_ini_original} → data_ini={data_ini}", client_name)

        log(f"🔎 {alias.upper()} | {data_ini} → {data_fim}", client_name)
    else:
        data_ini = None
        log(f"🔎 {alias.upper()} | carga full (WRITE_TRUNCATE)", client_name)

    auth_retries, plan_retry = 0, False
    limit, offset = 100, 0
    all_rows: List[Dict] = []
    session = make_session(cli["access_token"])

    while True:
        token_atual = cli["access_token"]
        session.headers.update({"Authorization": f"Bearer {token_atual}"})

        params: Dict[str, Any] = {}
        if not sem_pag:
            params["limit"], params["offset"] = limit, offset
            params["orderBy"] = "asc"

        if data_ini and api_p_ini:
            params[api_p_ini] = format_api_date(api_p_ini, data_ini, is_end=False)
            if is_inc and api_p_fim:
                params[api_p_fim] = format_api_date(api_p_fim, data_fim, is_end=True)

        if "extra_params" in cfg:
            params.update(cfg["extra_params"])

        try:
            r = session.get(f"{API_BASE}/{path_api}", params=params, timeout=45)
        except Exception as e:
            log(f"❌ Conexão {alias}: {e}", client_name)
            registrar_auditoria(modo_str, client_name, "PAI", alias, "ERRO_CONEXAO", 0, time.time()-t0)
            return "ERRO_CONEXAO"

        if r.status_code == 429:
            time.sleep(25 + random.uniform(2, 10)); continue

        if r.status_code == 401:
            if auth_retries < 1 and renovar_token(cli, token_atual):
                auth_retries += 1; session = make_session(cli["access_token"]); continue
            registrar_auditoria(modo_str, client_name, "PAI", alias, "ERRO_AUTH", 0, time.time()-t0)
            return "ERRO_AUTH"

        if r.status_code == 403:
            log(f"🔒 {alias.upper()}: sem acesso (403)", client_name)
            registrar_auditoria(modo_str, client_name, "PAI", alias, "SEM_ACESSO", 0, time.time()-t0)
            return "SEM_ACESSO"

        if r.status_code == 400:
            body = r.text[:500] if r.text else ""
            if not plan_retry:
                nova = extrair_data_plano(body)
                if nova:
                    log(f"📅 {alias.upper()}: plano limita a partir de {nova}. Ajustando...", client_name)
                    data_ini = nova; plan_retry = True; offset = 0; all_rows = []
                    continue
            log(f"⚠️ {alias.upper()}: 400 | {body[:200]}", client_name)
            registrar_auditoria(modo_str, client_name, "PAI", alias, "ERRO_400", 0, time.time()-t0)
            return "ERRO_400"

        if r.status_code == 404:
            log(f"⚠️ {alias.upper()}: 404", client_name)
            registrar_auditoria(modo_str, client_name, "PAI", alias, "SEM_ACESSO", 0, time.time()-t0)
            return "SEM_ACESSO"

        if r.status_code != 200:
            body = r.text[:300] if r.text else ""
            log(f"⚠️ {alias.upper()}: {r.status_code} | {body}", client_name)
            registrar_auditoria(modo_str, client_name, "PAI", alias, f"ERRO_{r.status_code}", 0, time.time()-t0)
            return f"ERRO_{r.status_code}"

        data = r.json()

        if sem_pag:
            if alias == "categorias":
                all_rows.extend(flatten_categorias(data))
            elif isinstance(data, dict):
                all_rows.append(data)
            elif isinstance(data, list):
                all_rows.extend(data)
            break

        lista = data.get("itens") or data.get("data") or data.get("registros") or []
        if not lista: break
        all_rows.extend(lista if isinstance(lista, list) else [lista])
        if len(lista) < limit: break
        offset += limit
        time.sleep(random.uniform(0.6, 1.2))

    # ═══ FIX v8: Detecção e remoção de fantasmas (em reprocessamento) ═══
    fantasmas_removidos = 0
    if is_inc and LOOKBACK_DAYS > 0 and campo_dt and data_ini and all_rows:
        try:
            ids_api = {str(r.get("id")) for r in all_rows if r.get("id") is not None}
            ids_bq = get_existing_ids_in_window(
                table_id, campo_dt, data_ini, data_fim, client_name, alias
            )
            ids_fantasmas = list(ids_bq - ids_api)
            if ids_fantasmas:
                log(f"👻 {alias.upper()}: {len(ids_fantasmas)} IDs fantasmas detectados. Deletando...", client_name)
                if delete_ids_from_table(table_id, ids_fantasmas, client_name, alias):
                    fantasmas_removidos = len(ids_fantasmas)
                    log(f"✅ {alias.upper()}: {fantasmas_removidos} fantasmas removidos.", client_name)
                else:
                    log(f"⚠️ {alias.upper()}: falha ao deletar fantasmas.", client_name)
        except Exception as e:
            log(f"⚠️ {alias.upper()}: erro na detecção de fantasmas: {str(e)[:160]}", client_name)

    if not all_rows:
        log(f"⚠️ {alias.upper()}: sem dados.", client_name)
        registrar_auditoria(modo_str, client_name, "PAI", alias, "SEM_DADOS", fantasmas_removidos, time.time()-t0)
        if is_inc and campo_dt:
            upsert_watermark_date(client_name, alias, data_fim)
        return f"SEM_DADOS (👻{fantasmas_removidos})" if fantasmas_removidos else "SEM_DADOS"

    # ─── GRAVAÇÃO NO BQ ───
    try:
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f UTC")
        df = pd.json_normalize(all_rows)
        df = normalize_df(df)
        df["data_extracao"] = now_utc
        df["_etl_loaded_at"] = now_utc
        ensure_dataset(dataset)

        if is_inc and "id" in df.columns:
            ids_novos = df["id"].dropna().astype(str).unique().tolist()
            if ids_novos and not delete_ids_from_table(table_id, ids_novos, client_name, alias):
                registrar_auditoria(modo_str, client_name, "PAI", alias, "ERRO_DELETE", fantasmas_removidos, time.time()-t0)
                return "ERRO_DELETE"

        wd = "WRITE_APPEND" if is_inc else "WRITE_TRUNCATE"
        if is_inc:
            jc = bigquery.LoadJobConfig(write_disposition=wd,
                 schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION], autodetect=True)
        else:
            jc = bigquery.LoadJobConfig(write_disposition=wd, autodetect=True)

        BQ.load_table_from_dataframe(df, table_id, job_config=jc).result()
        qtd, dur = len(df), time.time()-t0
        msg_ok = f"✅ {alias.upper()}: {qtd} registros em {dur:.1f}s."
        if fantasmas_removidos:
            msg_ok += f" 👻 {fantasmas_removidos} fantasmas removidos."
        log(msg_ok, client_name)
        registrar_auditoria(modo_str, client_name, "PAI", alias, "OK", qtd, dur)

        # watermark só avança após BQ load com sucesso
        if is_inc and campo_dt:
            upsert_watermark_date(client_name, alias, data_fim)

        return f"OK (👻{fantasmas_removidos})" if fantasmas_removidos else "OK"

    except Exception as e:
        log(f"❌ BQ {alias}: {e}", client_name)
        registrar_auditoria(modo_str, client_name, "PAI", alias, "ERRO_BQ", fantasmas_removidos, time.time()-t0)
        return "ERRO_BQ"


# ==============================================================================
# ORQUESTRAÇÃO
# ==============================================================================
def processar_cliente(cli):
    client_name = cli["des_clients"]
    dataset = dataset_name_from_client(cli["des_clients"])
    lista = list(MAPA_CONFIG_API.keys()) if ONLY_ENTS == "ALL" else [e.strip() for e in ONLY_ENTS.split(",") if e.strip()]
    ensure_dataset(dataset)
    log("🚀 Iniciando...", client_name)
    resultados = {}
    for alias in lista:
        if alias not in MAPA_CONFIG_API: continue
        status = processar_endpoint(cli, alias, dataset)
        resultados[alias] = status
        if status == "ERRO_AUTH":
            cli["auth_failed"] = True
            log("🛑 Auth falhou, parando.", client_name)
            break
    log("🏁 Finalizado.", client_name)
    return resultados

def fetch_clientes():
    # v9: filtra clientes vivos (dt_atualizacao < 25h)
    sql = f"""SELECT * FROM `{TBL_CLIENTES}`
              WHERE access_token IS NOT NULL
                AND dt_atualizacao IS NOT NULL
                AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), dt_atualizacao, HOUR) < 25"""
    df = BQ.query(sql).to_dataframe()
    return sorted(df.to_dict("records"), key=lambda x: (x.get("des_clients") or "").lower())

def filtrar_clientes_local(clientes):
    if ONLY_CLIENTS == "ALL": return clientes
    wanted = [x.strip().lower() for x in ONLY_CLIENTS.split(",") if x.strip()]
    return [c for c in clientes if any(w in (c.get("des_clients") or "").lower() for w in wanted)]

def formatar_bloco_slack(nome, resultados):
    linhas = [f"🏢 *{nome}*"]
    for ent, st in resultados.items():
        e = "✅" if st.startswith("OK") or st.startswith("SEM_DADOS") else ("⚠️" if st == "SEM_ACESSO" else "❌")
        linhas.append(f"   {e} {ent}: {st}")
    return "\n".join(linhas)

def run_sequential(clientes):
    relatorio = []
    for cli in filtrar_clientes_local(clientes):
        try: relatorio.append(formatar_bloco_slack(cli["des_clients"], processar_cliente(cli)))
        except Exception as e: relatorio.append(f"🏢 *{cli['des_clients']}*\n   ❌ {str(e)[:80]}")
    return relatorio

def enviar_relatorio_slack(relatorio, duracao, modo):
    buf = f"🏁 *Job PAI v9 Finalizado ({modo})* | Duração: {duracao}\n"
    for b in relatorio:
        if len(buf)+len(b) > 3800: enviar_slack(buf); buf = b
        else: buf += f"\n{b}"
    enviar_slack(buf)

def main():
    start = datetime.now()
    ti = os.getenv("CLOUD_RUN_TASK_INDEX")
    clientes = fetch_clientes()
    if not clientes: log("❌ Nenhum cliente."); return

    if ti is not None:
        tc = int(os.getenv("CLOUD_RUN_TASK_COUNT", "1"))
        if tc == 1:
            log("📋 Modo SEQUENCIAL (tasks=1)")
            if LOOKBACK_DAYS > 0: log(f"🔄 LOOKBACK_DAYS={LOOKBACK_DAYS}")
            if PAI_OVERLAP_DAYS > 0 and LOOKBACK_DAYS == 0:
                log(f"🔁 PAI_OVERLAP_DAYS={PAI_OVERLAP_DAYS}")
            enviar_slack("🚀 *Job PAI v9 iniciado (Sequencial)*")
            r = run_sequential(clientes)
            enviar_relatorio_slack(r, str(datetime.now()-start).split(".")[0], "Sequencial")
            return
        idx = int(ti)
        if idx >= len(clientes): log(f"💤 Task {idx} ociosa."); return
        cli = clientes[idx]
        log(f"🚀 [TASK {idx}] PAI v9: {cli['des_clients']}")
        enviar_slack(formatar_bloco_slack(cli["des_clients"], processar_cliente(cli)))
        return

    log("⚠️ Modo local.")
    enviar_slack("🚀 *Job PAI v9 (Local)*")
    enviar_relatorio_slack(run_sequential(clientes), str(datetime.now()-start).split(".")[0], "Local")

if __name__ == "__main__":
    main()
