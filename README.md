# db-to-bq-dlt-img

Cette image Docker permet de transférer des données depuis une base de données PostgreSQL ou Oracle vers BigQuery en utilisant [dlt](https://dlthub.com/).

## Configuration

L'image se configure via des variables d'environnement.

### Variables requises

| Variable | Description |
| --- | --- |
| `DB_URL_SECRET` | Nom du secret contenant l'url de connexion |
| `BQ_DATASET_ID` | Nom du dataset BigQuery de destination. |

### Variables optionnelles

| Variable | Description | Défaut |
| --- | --- | --- |
| `GOOGLE_CLOUD_PROJECT` | Projet GCP de destination pour BigQuery. | Projet courant |
| `DB_SCHEMA` | Schéma de la base de données source (ex: `public` pour PG, `MY_SCHEMA` pour Oracle). | Schéma par défaut |
| `BQ_LOCATION` | Localisation du dataset BigQuery. | `EU` |
| `TABLES_INCLUDE` | Liste de tables à inclure séparées par des virgules (si vide, toutes les tables). | (vide) |
| `TABLES_EXCLUDE` | Liste de tables à exclure séparées par des virgules. | (vide) |
| `TABLES_PREFIX` | Préfixe pour filtrer les tables à inclure (ex: `T_`). | (vide) |
| `ENABLE_ORACLE_THICK_MODE` | Activer le mode Thick pour Oracle (requis pour DB < 12.1). | `false` |
| `ORACLE_IC_PATH` | Chemin vers l'Instant Client Oracle (si requis et non dans le PATH). | (vide) |
| `LOG_LEVEL` | Niveau de log (`DEBUG`, `INFO`, `WARNING`, `ERROR`). | `INFO` |

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