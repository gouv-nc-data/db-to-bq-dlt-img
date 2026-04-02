import os
import sys
import logging
import json
import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION LOGGING (DOIT ÊTRE FAIT EN PREMIER POUR LE TRACE CLIENT) ---
log_format = os.getenv("LOG_FORMAT", "JSON").upper()
log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)

if log_format == "JSON":
    try:
        from google.cloud.logging.handlers import StructuredLogHandler
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        handler = StructuredLogHandler(project=project_id)
        logging.getLogger().addHandler(handler)
    except ImportError:
        logging.basicConfig(level=log_level)
    os.environ["RUNTIME__LOG_FORMAT"] = "JSON"
else:
    # Format texte standard pour environnement local
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout
    )
    os.environ["RUNTIME__LOG_FORMAT"] = "TEXT"

logging.getLogger().setLevel(log_level)
os.environ["RUNTIME__LOG_LEVEL"] = log_level_name
logging.captureWarnings(True)

# --- MONKEY PATCH ORACLEDB (EMPECHE LES CRASH SUR DATES INVALIDES) ---
import oracledb

def date_out_converter(val):
    """Convertisseur pour les dates Oracle hors intervalle Python (ex: année -5579)"""
    if val is None:
        return None
    try:
        # On vérifie l'année (4 premiers caractères) avant conversion
        year_str = val[:4]
        # Si l'année est négative ou hors intervalle [0001-9999]
        if year_str.startswith('-') or int(year_str) < 1 or int(year_str) > 9999:
            logging.error(f"!!! CRITICAL PATCH !!! Date Oracle invalide neutralisée : {val}")
            return None
        # On retourne un objet datetime ISO-compatible
        return datetime.datetime.fromisoformat(val.replace(' ', 'T'))
    except Exception:
        return None

def oracle_output_type_handler(cursor, metadata):
    """Handler global pour intercepter les types temporels et les traiter via date_out_converter"""
    if metadata.type in (oracledb.DB_TYPE_DATE, oracledb.DB_TYPE_TIMESTAMP, 
                         oracledb.DB_TYPE_TIMESTAMP_TZ, oracledb.DB_TYPE_TIMESTAMP_LTZ):
        return cursor.var(oracledb.DB_TYPE_VARCHAR, arraysize=cursor.arraysize, outconverter=date_out_converter)

def _apply_patch(conn):
    """Applique le handler sur une nouvelle connexion"""
    if hasattr(conn, "outputtypehandler"):
        conn.outputtypehandler = oracle_output_type_handler
    return conn

# Patche les points d'entrée de connexion pour garantir l'activation du handler
_original_connect = oracledb.connect
def _patched_connect(*args, **kwargs):
    return _apply_patch(_original_connect(*args, **kwargs))
oracledb.connect = _patched_connect

if hasattr(oracledb, "Connection"):
    _original_Connection = oracledb.Connection
    def _patched_Connection(*args, **kwargs):
        return _apply_patch(_original_Connection(*args, **kwargs))
    oracledb.Connection = _patched_Connection

if hasattr(oracledb, "Connect"):
    oracledb.Connect = _patched_connect

# --- IMPORTS DLT & SQLALCHEMY ---
import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dlt.destinations.adapters import bigquery_adapter
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.impl.bigquery import sql_client as bq_sql_client
from sqlalchemy import event
from sqlalchemy.engine import Engine
from google.cloud import secretmanager

# --- PATCH 1 : BIGQUERY TRUNCATE — ignore les tables utilisateur inexistantes ---
# DLT utilise `truncate-and-insert` par défaut pour write_disposition="replace".
# Il tente un TRUNCATE avant d'insérer. Si la table a été supprimée manuellement,
# le TRUNCATE échoue avec DatabaseUndefinedRelation. Ce patch l'intercepte table
# par table et continue silencieusement : DLT créera la table lors du chargement.
_original_truncate_tables = bq_sql_client.BigQuerySqlClient.truncate_tables

def _safe_truncate_tables(self, *tables: str) -> None:
    for table in tables:
        try:
            _original_truncate_tables(self, table)
        except DatabaseUndefinedRelation:
            logging.warning(
                f"Table '{table}' introuvable lors du TRUNCATE (mode replace) — "
                "elle sera créée lors du chargement des données."
            )

bq_sql_client.BigQuerySqlClient.truncate_tables = _safe_truncate_tables
logging.info("Patch 1 actif : truncate_tables tolère les tables supprimées.")


# NOTE : pour réinitialiser complètement DLT (ex: changement de colonnes), supprimer
# le DATASET ENTIER `lisa` dans BigQuery. DLT le recrée avec toutes ses tables internes.
# Ne JAMAIS supprimer uniquement les tables `_dlt_pipeline_state` ou `_dlt_loads`
# sans supprimer le dataset entier — DLT crasherait car il ne les recrée pas.


# Paramètre de normalisation (requis pour éviter les erreurs de fork avec certains pilotes)
os.environ["NORMALIZE__START_METHOD"] = os.getenv("NORMALIZE_START_METHOD", "spawn")

# Mode Thick Oracle (requis pour les versions < 12.1 ou les types complexes)
if os.getenv("ENABLE_ORACLE_THICK_MODE", "").lower() == "true":
    lib_dir = os.getenv("ORACLE_IC_PATH")
    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)
        logging.info("Mode Oracle Thick activé (utilisation de l'Instant Client)")
    except Exception as e:
        logging.error(f"Erreur lors de l'activation du mode Oracle Thick: {e}")
        sys.exit(1)

# Événements SQLAlchemy pour renforcer le réglage de session et le handler
# Ces handlers sont limités aux connexions oracledb pour ne pas perturber PostgreSQL.
@event.listens_for(Engine, "connect")
def set_oracle_params(dbapi_connection, connection_record):
    """Configuration de la session Oracle — ignoré si ce n'est pas une connexion oracledb."""
    # On vérifie que c'est bien une connexion oracledb avant toute chose
    # outputtypehandler est un attribut exclusif aux connexions oracledb
    # (absent de psycopg2/PostgreSQL) — c'est le moyen de détecter le type
    if not hasattr(dbapi_connection, "outputtypehandler"):
        return
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'")
    except Exception:
        pass
    finally:
        cursor.close()
    _apply_patch(dbapi_connection)

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Force l'installation du handler Oracle sur le curseur — ignoré si ce n'est pas oracledb."""
    if hasattr(cursor, "outputtypehandler") and hasattr(getattr(cursor, "connection", None), "outputtypehandler"):
        cursor.outputtypehandler = oracle_output_type_handler

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
    global_write_disposition = os.getenv("WRITE_DISPOSITION", "replace")
    global_cursor_missing = os.getenv("ON_CURSOR_VALUE_MISSING", "include")

    # Configuration spécifique par table (JSON)
    table_configs_raw = os.getenv("TABLE_CONFIGS", "{}")
    try:
        # On normalise les clés en minuscules pour une recherche insensible à la casse
        raw_configs = json.loads(table_configs_raw)
        table_configs = {k.lower(): v for k, v in raw_configs.items()}
    except json.JSONDecodeError as e:
        logging.error(f"Erreur lors du parsing de TABLE_CONFIGS: {e}")
        table_configs = {}

    if not secret_url or not bq_dataset_id:
        logging.error("DB_URL_SECRET et BQ_DATASET_ID sont requis.")
        sys.exit(1)

    logging.info(f"Démarrage de la pipeline vers BigQuery (Dataset: {bq_dataset_id})")

    # Configuration de la destination
    destination_params = {"location": os.getenv("BQ_LOCATION", "EU")}
    if bq_project_id:
        destination_params["project_id"] = bq_project_id

    # Staging GCS optionnel : accélère le chargement si un bucket est configuré
    bucket_url = os.getenv("BUCKET_URL")
    staging = 'filesystem' if bucket_url else None
    if bucket_url:
        logging.info(f"Staging GCS activé : {bucket_url}")

    # Nom du pipeline : auto-généré à partir du dataset pour isolation, ou via variable d'environnement
    default_pipeline_name = f"db_to_bq_{bq_dataset_id}" if bq_dataset_id else "db_to_bq_generic"
    pipeline_id = os.getenv("PIPELINE_NAME", default_pipeline_name)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination=dlt.destinations.bigquery(**destination_params, loader_file_format="parquet"),
        dataset_name=bq_dataset_id,
        staging=staging,
        progress="log",
    )



    # Chargement de la source SQL Database
    chunk_size = int(os.getenv("SQL_CHUNK_SIZE", "100000"))
    source = sql_database(
        db_url, 
        schema=db_schema, 
        chunk_size=chunk_size,
        backend_kwargs={"pool_pre_ping": True, "pool_recycle": 3600}
    )

    # --- LOGIQUE DE FILTRAGE ET REQUÊTES PERSONNALISÉES PAR INSTANCE ---
    custom_tables: dict[str, str] = {} # Dictionnaire pour stocker les requêtes personnalisées

    # 1. Chargement via variable d'environnement JSON (Format: {"table_name": "SELECT ..."})
    # Pratique pour une configuration directe via Terraform env_vars
    table_queries_raw = os.getenv("TABLE_QUERIES", "{}")
    try:
        parsed_queries = json.loads(table_queries_raw)
        if isinstance(parsed_queries, dict):
            custom_tables.update({str(k): str(v) for k, v in parsed_queries.items()})
    except json.JSONDecodeError as e:
        logging.error(f"Erreur lors du parsing de TABLE_QUERIES (JSON invalide): {e}")

    # 2. Application des requêtes au dlt source (Mode Query)
    for t_query_name, sql_query in custom_tables.items():
        try:
            # On force le nom de la table en minuscules pour correspondre à la normalisation dlt
            t_name_norm = t_query_name.lower()
            # On crée une ressource SQL spécifique
            custom_res = sql_table(db_url, schema=db_schema, table_name=t_name_norm, query=sql_query)
            source.resources.add(custom_res)
            logging.info(f"Ressource personnalisée '{t_name_norm}' enregistrée (Mode Query via Env Var).")
        except Exception as e:
            logging.error(f"Erreur lors de l'application de la requête pour {t_query_name}: {e}")

    # --- LOGIQUE DE FILTRAGE CONSOLIDÉE ---
    all_resources = list(source.resources.keys())
    # 1. Sélection par discovery (en excluant les tables BIN$ d'Oracle et les exclusions explicites)
    discovery_selected = [n for n in all_resources if not n.upper().startswith("BIN$")]
    
    # On normalise aussi les clés des requêtes personnalisées pour l'union finale
    custom_names_norm = [k.lower() for k in custom_tables.keys()]

    if tables_exclude:
        exclude_list = [t.strip().lower() for t in tables_exclude.split(",")]
        # On n'exclut que si ce n'est pas une table "custom" (priorité au SQL)
        discovery_selected = [n for n in discovery_selected if n.lower() not in exclude_list]

    if tables_include:
        include_list = [t.strip().lower() for t in tables_include.split(",")]
        discovery_selected = [n for n in discovery_selected if n.lower() in include_list]

    if tables_prefix:
        prefix = tables_prefix.strip().lower()
        discovery_selected = [n for n in discovery_selected if n.lower().startswith(prefix)]

    # 2. Union avec les tables personnalisées (qui outpassent les filtres d'exclusion/inclusion)
    selected = list(set(discovery_selected) | set(custom_names_norm))


    if not selected:
        logging.warning("Aucune table ne correspond aux filtres spécifiés.")
        return

    source = source.with_resources(*selected)
    
    # Utilisation du normaliseur natif de la source (déjà initialisé)
    naming = source.schema.naming

    def normalize_col(col):
        if not col:
            return col
        if isinstance(col, str):
            return naming.normalize_identifier(col)
        if isinstance(col, list):
            return [naming.normalize_identifier(c) for c in col]
        return col

    # --- APPLICATION DES CONFIGURATIONS SPÉCIFIQUES (Incrémental, PK, Partitionnement) ---
    for res_name in selected:
        res = source.resources[res_name]
        # Recherche de config spécifique (insensible à la casse)
        config = table_configs.get(res_name.lower()) or {}
        
        inc_col = normalize_col(config.get("incremental") or global_incremental_col)
        pk_col = normalize_col(config.get("primary_key") or global_primary_key)
        w_disp = config.get("write_disposition") or global_write_disposition
        partition_col = normalize_col(config.get("partition"))
        cluster_cols = normalize_col(config.get("cluster"))
        exclude_cols = normalize_col(config.get("exclude"))
        cursor_missing = config.get("on_cursor_value_missing", global_cursor_missing)

        hints = {}
        if inc_col:
            hints["incremental"] = dlt.sources.incremental(inc_col, on_cursor_value_missing=cursor_missing)
        if pk_col:
            hints["primary_key"] = pk_col
        if w_disp:
            hints["write_disposition"] = w_disp
        
        # Exclusion de colonnes (données + schéma)
        if exclude_cols:
            if isinstance(exclude_cols, str):
                exclude_cols = [exclude_cols]
            cols_to_skip = list(exclude_cols)
            # Filtre les données (bug closure corrigé via arg par défaut)
            def _make_col_filter(skip_cols: list):
                def _filter(item, meta=None):
                    if item is None:
                        return None
                    return {k: v for k, v in item.items() if k not in skip_cols}
                return _filter
            res.add_map(_make_col_filter(cols_to_skip))

            # Supprime les colonnes du schéma DLT pour qu'elles n'apparaissent pas dans BQ
            for col in cols_to_skip:
                try:
                    del res.columns[col]
                except (KeyError, TypeError):
                    pass
            logging.info(f"Colonnes exclues (données + schéma) pour {res_name} : {cols_to_skip}")


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
        # On ne passe plus write_disposition globalement à run() car cela réinitialise l'état incrémental.
        # Le mode par défaut (replace) est déjà appliqué individuellement à chaque ressource via les hints (ligne 142/153).
        load_info = pipeline.run(source)
        logging.info(f"Pipeline terminée avec succès. Info: {load_info}")
    except Exception as e:
        logging.error(f"Erreur lors de l'exécution de la pipeline: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
