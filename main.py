import os
import sys
import logging
import json
import dlt
from dlt.sources.sql_database import sql_database
from dlt.destinations.adapters import bigquery_adapter
from google.cloud.logging.handlers import StructuredLogHandler
from dotenv import load_dotenv
import oracledb

from google.cloud import secretmanager

load_dotenv()

# Configuration Cloud Logging
handler = StructuredLogHandler()
logging.getLogger().addHandler(handler)
log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.getLogger().setLevel(log_level)
logging.captureWarnings(True)

# Configuration dlt
os.environ["RUNTIME__LOG_LEVEL"] = log_level_name
os.environ["RUNTIME__LOG_FORMAT"] = "JSON"

# Paramètre de normalisation (requis pour éviter les erreurs de fork avec certains pilotes)
os.environ["NORMALIZE__START_METHOD"] = os.getenv("NORMALIZE_START_METHOD", "spawn")

# Mode Thick Oracle (requis pour les versions < 12.1)
if os.getenv("ENABLE_ORACLE_THICK_MODE", "").lower() == "true":
    lib_dir = os.getenv("ORACLE_IC_PATH")
    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)
        logging.info("Mode Oracle Thick activé (utilisation de l'Instant Client)")
    except Exception as e:
        logging.error(f"Erreur lors de l'activation du mode Oracle Thick: {e}")
        sys.exit(1)

def run_pipeline():
    # Configuration BDD
    secret_url = os.environ.get("DB_URL_SECRET")

    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": secret_url})
    db_url = response.payload.data.decode("UTF-8").strip()
    
    # Injection automatique de disable_oob=true pour le mode Oracle Thin
    # Cela évite les lenteurs/blocages liés au "Out of Band" breaks, fréquents dans Docker/K8s
    if "oracle" in db_url and "disable_oob=true" not in db_url and os.getenv("ENABLE_ORACLE_THICK_MODE", "").lower() != "true":
        separator = "&" if "?" in db_url else "?"
        db_url = f"{db_url}{separator}disable_oob=true"
        logging.info("Paramètre disable_oob=true ajouté à la chaîne de connexion (Mode Thin).")

    # Configuration des variables d'environnement
    db_schema = os.getenv("DB_SCHEMA", "").strip() or None
    bq_dataset_id = os.getenv("BQ_DATASET_ID", "").strip()
    bq_project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
    
    # Inclusion/Exclusion/Préfixe de tables
    tables_include = os.getenv("TABLES_INCLUDE") or os.getenv("TABLE_INCLUDE")
    tables_exclude = os.getenv("TABLES_EXCLUDE") or os.getenv("TABLE_EXCLUDE")
    tables_prefix = os.getenv("TABLES_PREFIX") or os.getenv("TABLE_PREFIX")

    # Nouvelles configurations pour l'incrémental et le mode hybride
    global_incremental_col = os.getenv("INCREMENTAL_COLUMN")
    global_primary_key = os.getenv("PRIMARY_KEY")
    global_write_disposition = os.getenv("WRITE_DISPOSITION", "replace") # Par défaut replace pour garder le comportement d'origine

    # Configuration spécifique par table (JSON)
    table_configs_raw = os.getenv("TABLE_CONFIGS", "{}")
    try:
        # On normalise les clés en minuscules pour une recherche insensible à la casse
        raw_configs = json.loads(table_configs_raw)
        table_configs = {k.lower(): v for k, v in raw_configs.items()}
    except json.JSONDecodeError as e:
        logging.error(f"Erreur lors du parsing de TABLE_CONFIGS (format JSON invalide): {e}")
        table_configs = {}

    if not secret_url or not bq_dataset_id:
        logging.error("DB_URL_SECRET et BQ_DATASET_ID sont requis.")
        sys.exit(1)

    logging.info(f"Démarrage de la pipeline vers BigQuery (Dataset: {bq_dataset_id})")

    # Configuration de la destination
    destination_params = {"location": os.getenv("BQ_LOCATION", "EU")}
    if bq_project_id:
        destination_params["project_id"] = bq_project_id

    pipeline = dlt.pipeline(
        pipeline_name='db_to_bq_generic',
        destination=dlt.destinations.bigquery(**destination_params, loader_file_format="parquet"),
        dataset_name=bq_dataset_id,
        progress="log",
    )

    # Chargement de la source SQL Database
    # sql_database permet de charger automatiquement toutes les tables d'un schéma
    chunk_size = int(os.getenv("SQL_CHUNK_SIZE", "100000"))  # Par défaut 100000
    source = sql_database(db_url, schema=db_schema, chunk_size=chunk_size)

    # --- LOGIQUE DE FILTRAGE CONSOLIDÉE ---
    all_resources = list(source.resources.keys())
    selected = [n for n in all_resources if not n.upper().startswith("BIN$")]

    if tables_include:
        include_list = [t.strip().lower() for t in tables_include.split(",")]
        selected = [n for n in selected if n.lower() in include_list]
    
    # Application des filtres d'exclusion (insensible à la casse)
    if tables_exclude:
        exclude_list = [t.strip().lower() for t in tables_exclude.split(",")]
        selected = [n for n in selected if n.lower() not in exclude_list]

    if tables_prefix:
        prefix = tables_prefix.strip().lower()
        selected = [n for n in selected if n.lower().startswith(prefix)]

    if not selected:
        logging.warning("Aucune table ne correspond aux filtres spécifiés.")
        return

    source = source.with_resources(*selected)
    
    # --- APPLICATION DES CONFIGURATIONS SPÉCIFIQUES (Incrémental, PK, Partitionnement) ---
    for res_name in selected:
        res = source.resources[res_name]
        # Recherche de config spécifique (insensible à la casse)
        config = table_configs.get(res_name.lower()) or {}
        
        inc_col = config.get("incremental") or global_incremental_col
        pk_col = config.get("primary_key") or global_primary_key
        w_disp = config.get("write_disposition") or global_write_disposition
        partition_col = config.get("partition")
        cluster_cols = config.get("cluster")
        exclude_cols = config.get("exclude")
        
        hints = {}
        if inc_col:
            hints["incremental"] = dlt.sources.incremental(inc_col, on_cursor_value_missing="include")
        if pk_col:
            hints["primary_key"] = pk_col
        if w_disp:
            hints["write_disposition"] = w_disp
        
        # Exclusion de colonnes (ex: bytea volumineux)
        if exclude_cols:
            if isinstance(exclude_cols, str):
                exclude_cols = [exclude_cols]
            
            # On s'assure que exclude_cols est traité comme une liste pour l'IDE (Pyre2)
            cols_to_skip = list(exclude_cols)
            res.add_map(lambda item: {k: v for k, v in item.items() if k not in cols_to_skip})
            logging.info(f"Colonnes exclues physiquement pour {res_name} : {cols_to_skip}")

        # Partitionnement et clustering BigQuery via l'adapter dédié
        # Cela empêche l'auto-partitionnement de DLT et donne un contrôle explicite
        adapter_kwargs = {}
        if partition_col:
            adapter_kwargs["partition"] = partition_col
        if cluster_cols:
            if isinstance(cluster_cols, str):
                cluster_cols = [cluster_cols]
            adapter_kwargs["cluster"] = list(cluster_cols)
        if adapter_kwargs:
            bigquery_adapter(res, **adapter_kwargs)
            logging.info(f"BigQuery adapter appliqué pour {res_name}: {adapter_kwargs}")

        if hints:
            res.apply_hints(**hints)
            logging.info(f"Configuration appliquée pour {res_name}: {hints}")

    logging.info(f"Ressources prêtes pour le transfert : {selected}")

    # Exécution
    try:
        logging.info("Exécution de la pipeline...")
        # On passe global_write_disposition comme défaut (sera écrasé par les hints si spécifié)
        load_info = pipeline.run(source, write_disposition=global_write_disposition)
        logging.info(f"Pipeline terminée avec succès. Info: {load_info}")
    except Exception as e:
        logging.error(f"Erreur lors de l'exécution de la pipeline: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
