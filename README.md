# db-to-bq-dlt-img

Cette image Docker permet de transférer des données depuis une base de données PostgreSQL ou Oracle vers BigQuery en utilisant [dlt](https://dlthub.com/).

## Configuration

L'image se configure via des variables d'environnement.

### Variables requises

| Variable | Description |
| --- | --- |
| `DB_URL_SECRET` | Nom du secret contenant l'url de connexion |
| `BQ_DATASET_ID` | Nom du dataset BigQuery de destination. |

### Configuration du chargement différentiel (Mode Hybride)

Vous pouvez configurer un comportement global ou spécifique par table pour n'extraire que les nouvelles données.

| Variable | Description | Par défaut |
|----------|-------------|------------|
| `WRITE_DISPOSITION` | Mode par défaut : `replace` (écrase), `append` (ajoute), `merge` (met à jour). | `replace` |
| `INCREMENTAL_COLUMN` | Colonne de curseur globale (ex: `updated_at`) pour l'incrémental. | (vide) |
| `PRIMARY_KEY` | Clé primaire globale (requis pour le mode `merge`). | (vide) |
| `TABLE_CONFIGS` | JSON de configuration spécifique par table (voir exemple ci-dessous). | `{}` |
| `ON_CURSOR_VALUE_MISSING` | Comportement si la valeur d'incrément (`updated`) est `NULL` (`include`, `exclude`, `raise`). | `include` |
| `GLOBAL_EXCLUDE` | Liste de colonnes à exclure sur TOUTES les tables (ex: `created_by,updated_by`). | (vide) |
| `NORMALIZE_START_METHOD`| Méthode de démarrage des workers (`spawn` ou `fork`). | `spawn` |

#### Format de `TABLE_CONFIGS`

Cette variable permet de définir des règles précises pour chaque table (insensible à la casse sur les noms de tables).

```json
{
  "MA_TABLE": {
    "incremental": "date_maj",
    "primary_key": "id",
    "write_disposition": "merge",
    "partition": "date_maj"
  },
  "AUTRE_TABLE": {
    "write_disposition": "append",
    "incremental": "id"
  },
  "DOCUMENT": {
    "exclude": ["content"],
    "incremental": "updated",
    "primary_key": "id",
    "write_disposition": "merge",
    "on_cursor_value_missing": "exclude"
  },
  "CONTACTS": {
    "include": ["id", "nom", "prenom", "email", "date_maj"],
    "incremental": "date_maj",
    "primary_key": "id",
    "write_disposition": "merge"
  },
  "T_ARTICLE": {
    "primary_key": "id",
    "incremental": "updated",
    "write_disposition": "merge",
    "include": ["id", "enr_acte_id", "nature_id", "assiette", "updated"],
    "columns": {
      "assiette": {
        "data_type": "wei"
      }
    }
  }
}
```

Clés supportées par table dans `TABLE_CONFIGS` :

- `incremental`, `primary_key`, `write_disposition`, `partition`, `cluster`
- `include`, `exclude`, `on_cursor_value_missing`
- `columns` : hints DLT par colonne (ex: `data_type`, `precision`, `scale`, `nullable`, etc.)

Comportement de `columns` :

- Rétrocompatible : si `columns` est absent, aucun changement de comportement.
- Les noms de colonnes sont normalisés comme le reste de la pipeline.
- Les hints utilisateur sont fusionnés avec les hints internes de nullabilité (`nullable=true` forcé par défaut).
- Si une colonne définie dans `columns` est absente du schéma (typo, colonne exclue, ou non réfléchie), un warning est loggé et la pipeline continue.

#### Commentaires de colonnes (descriptions BigQuery)

Les commentaires de colonnes de la base source sont automatiquement copiés vers les descriptions de colonnes BigQuery. Aucune configuration requise.

- **Oracle** : lus depuis `ALL_COL_COMMENTS` (owner = `DB_SCHEMA`, ou l'utilisateur de session si non défini).
- **PostgreSQL** : lus depuis `pg_description` (commentaires `COMMENT ON COLUMN`), pour le schéma `DB_SCHEMA` (ou `public`).
- Non bloquant : si la récupération échoue, un warning est loggé et la pipeline continue sans descriptions.
- Une `description` explicite définie dans `columns` (voir ci-dessus) est prioritaire sur le commentaire source.
- S'applique uniquement aux tables réfléchies. Les tables avec un SQL personnalisé (`TABLE_QUERIES`) n'ont pas de descriptions auto.

#### Mappings DLT vers BigQuery (utile pour `columns`)

| `data_type` DLT | Type BigQuery | Notes |
| --- | --- | --- |
| `decimal` | `NUMERIC` | Par défaut `(38, 9)`, respecte `precision`/`scale` si fournis (limites `precision - scale <= 29`). |
| `wei` | `BIGNUMERIC` | Jusqu'à `(76, 38)` ; pas de limite `precision - scale`. |
| `bigint` | `INT64` | |
| `double` | `FLOAT64` | |
| `text` | `STRING` | |

#### Exemple Terraform (`TABLE_CONFIGS`) avec hints de colonnes

```hcl
{
  "t_article" = {
    primary_key       = "id"
    incremental       = "updated"
    write_disposition = "merge"
    include           = ["id", "enr_acte_id", "nature_id", "assiette", "updated"]
    columns = {
      assiette = { data_type = "wei" }
    }
  }

  "t_fisc_all_declaration" = {
    primary_key       = "id"
    incremental       = "updated"
    write_disposition = "merge"
    columns = {
      montant_a_payer = { data_type = "wei" }
      montant_paye    = { data_type = "wei" }
    }
  }

  "t_rec_proc_recouvrement" = {
    primary_key       = "id"
    incremental       = "updated"
    write_disposition = "merge"
    columns = {
      pcol_montant_prov = { data_type = "wei" }
      atd_montant_paye  = { data_type = "wei" }
    }
  }
}
```

> [!TIP]
> **Inclusion de colonnes (liste blanche)** : La clé `include` définit les seules colonnes à copier vers BigQuery. Toutes les autres colonnes sont ignorées. C'est l'inverse de `exclude` et elle est prioritaire sur celui-ci. Utile pour les tables avec de nombreuses colonnes sensibles ou inutiles dont on ne veut copier qu'un sous-ensemble précis.
> **Limitation** : `include` n'est pas supporté pour les tables avec un SQL personnalisé (`TABLE_QUERIES`) — dans ce cas, filtrer directement dans la requête SQL.

> [!TIP]
> **Exclusion de colonnes** : Idéal pour les champs `bytea` ou `blob` (pièces jointes) qui alourdissent inutilement le transfert vers BigQuery.

> [!TIP]
> **Partitionnement** : Utiliser la même colonne (ex: `date_maj`) pour `incremental` et `partition` est la configuration optimale pour réduire les coûts sur BigQuery.

### Autres variables optionnelles

| Variable | Description | Défaut |
| --- | --- | --- |
| `GOOGLE_CLOUD_PROJECT` | Projet GCP de destination pour BigQuery. | Projet courant |
| `DB_SCHEMA` | Schéma de la base de données source (ex: `public` pour PG, `MY_SCHEMA` pour Oracle). | Schéma par défaut |
| `BQ_LOCATION` | Localisation du dataset BigQuery. | `EU` |
| `TABLES_INCLUDE` | Liste de tables à inclure séparées par des virgules (si vide, toutes les tables). | (vide) |
| `TABLES_EXCLUDE` | Liste de tables à exclure séparées par des virgules. | (vide) |
| `TABLES_PREFIX` | Préfixe pour filtrer les tables à inclure (ex: `T_`). | (vide) |
| `SQL_CHUNK_SIZE` | Nombre de lignes par chunk d'extraction | `100000` |
| `ENABLE_ORACLE_THICK_MODE` | Activer le mode Thick pour Oracle (requis pour DB < 12.1). | `false` |
| `ORACLE_IC_PATH` | Chemin vers l'Instant Client Oracle (si requis et non dans le PATH). | (vide) |
| `DROP_PENDING_PACKAGES` | Si `true`, supprime les pending packages DLT au démarrage (utile si le state est persisté via PVC et qu'un run précédent a crashé entre extract et load). À ne mettre que ponctuellement. | `false` |
| `LOG_LEVEL` | Niveau de log (`DEBUG`, `INFO`, `WARNING`, `ERROR`). | `INFO` |
| `DB_POOL_SIZE` | Taille du pool SQLAlchemy (connexions maintenues). | `20` |
| `DB_POOL_MAX_OVERFLOW` | Connexions temporaires autorisées au-delà de `DB_POOL_SIZE`. | `30` |
| `DB_POOL_TIMEOUT` | Temps d'attente max (s) pour obtenir une connexion du pool. | `60` |
| `DB_POOL_RECYCLE` | Délai (s) avant recyclage d'une connexion (évite les timeouts Oracle/PG). | `3600` |

> [!NOTE]
> **`DROP_PENDING_PACKAGES`** n'est utile que si le répertoire `~/.dlt` est persisté entre les runs (volume monté). Dans un Job Kubernetes éphémère sans PVC, les pending packages disparaissent avec le pod, l'option est sans effet pratique.

> [!TIP]
> **Pool de connexions et erreur `QueuePool limit reached`** : par défaut, le pool tient ~50 connexions (`DB_POOL_SIZE` + `DB_POOL_MAX_OVERFLOW`). Avec une base à plusieurs centaines de tables, la réflexion du schéma + l'extraction parallèle (`EXTRACT__WORKERS`, `EXTRACT__MAX_PARALLEL_ITEMS`) peuvent saturer le pool. Augmenter `DB_POOL_SIZE`/`DB_POOL_MAX_OVERFLOW` (et la limite `SESSIONS`/`PROCESSES` côté serveur si besoin), ou réduire `EXTRACT__MAX_PARALLEL_ITEMS` à une valeur ≤ `DB_POOL_SIZE`.

## Format des URL de connexion

Les secrets doivent contenir des URL au format SQLAlchemy :

- **PostgreSQL** : `postgresql://user:password@host:port/dbname`
- **Oracle** : `oracle+oracledb://user:password@host:port/?service_name=service` (le driver `python-oracledb` est utilisé en mode thin).

## Déploiement

L'image est conçue pour être exécutée comme un Job Cloud Run ou dans un cluster Kubernetes avec un Service Account ayant les permissions :
- `roles/secretmanager.secretAccessor` sur le secret de la DB.
- `roles/bigquery.dataEditor` et `roles/bigquery.jobUser` sur le projet/dataset de destination.

> [!NOTE]
> L'URL `DB_URL` doit être injectée de manière sécurisée (par exemple via la fonctionnalité de secrets des Jobs Cloud Run ou des Secrets Kubernetes).

## Tests Locaux avec Docker Compose

Pour tester l'image localement :

1. Préparez un fichier `.env` avec vos variables (voir section Configuration).
2. Si vous utilisez BigQuery, placez un fichier `creds.json` à la racine (clé de votre Service Account).
3. Lancez le test :
   ```bash
   docker-compose up --build
   ```