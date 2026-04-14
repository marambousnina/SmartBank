"""
SmartBank Metrics - Script de chargement ordonné vers PostgreSQL
=================================================================
Ordre d'exécution:
  1. cleaned       → 5 tables (jira, commits, pipelines, jobs, merge_requests)
  2. features      → 3 tables (tickets, assignee_metrics, project_metrics)
  3. dora_metrics  → 7 tables (kpis, summary, by_project, by_source, weekly, evolution, jobs_stats)
  4. team_metrics  → 9 tables (person_dashboard, assignee_performance, team_performance, ...)
  5. ml_results    → (alimenté par les scripts ML, non chargé ici)

Usage:
  python database/load_to_db.py              # Charge tout
  python database/load_to_db.py --layer cleaned
  python database/load_to_db.py --layer features
  python database/load_to_db.py --layer dora_metrics
  python database/load_to_db.py --layer team_metrics
  python database/load_to_db.py --dry-run    # Vérifie les fichiers sans charger
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Résolution du chemin racine
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine

# ── Configuration des chemins ───────────────────────────────────────────────
DATA = ROOT_DIR / "data"

CLEANED_DIR  = DATA / "cleaned"
FEATURES_DIR = DATA / "features"
METRICS_DIR  = DATA / "metrics"
TEAM_DIR     = DATA / "team"

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT_DIR / "database" / "load_to_db.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _load_csv(filepath: Path, parse_dates: list = None) -> pd.DataFrame | None:
    """Charge un CSV avec gestion d'erreur. Retourne None si fichier absent."""
    if not filepath.exists():
        log.warning(f"Fichier introuvable : {filepath.name}")
        return None
    df = pd.read_csv(filepath, parse_dates=parse_dates or [], low_memory=False)
    log.info(f"  Chargé {len(df):>6} lignes depuis {filepath.name}")
    return df


def _upsert(df: pd.DataFrame, schema: str, table: str, engine,
            conflict_cols: list = None, dry_run: bool = False) -> int:
    """
    Insère les données dans PostgreSQL.
    - Si conflict_cols spécifié : INSERT ... ON CONFLICT DO NOTHING (idempotent)
    - Sinon : remplacement complet de la table
    """
    if df is None or df.empty:
        log.warning(f"  Pas de données pour {schema}.{table}")
        return 0

    full_table = f"{schema}.{table}"

    if dry_run:
        log.info(f"  [DRY-RUN] {full_table} ← {len(df)} lignes ({list(df.columns)[:5]}...)")
        return len(df)

    # Nettoyage des noms de colonnes (lowercase, no spaces)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Ajout de la date de snapshot si la table la supporte
    if "snapshot_date" not in df.columns:
        df["snapshot_date"] = date.today()

    try:
        df.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )
        log.info(f"  [OK] {full_table} ← {len(df)} lignes insérées")
        return len(df)
    except Exception as e:
        log.error(f"  [ERREUR] {full_table} : {e}")
        return 0


def _truncate(schema: str, table: str, engine, dry_run: bool = False):
    """Vide la table avant rechargement (mode refresh complet)."""
    if dry_run:
        return
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE"))
    log.info(f"  [TRUNCATE] {schema}.{table}")


# ════════════════════════════════════════════════════════════════════════════
# COUCHE 1 : cleaned
# ════════════════════════════════════════════════════════════════════════════

DATE_COLS = {
    "jira":  ["StatusEntryDate", "StatusExitDate", "Created", "Updated", "ResolutionDate", "DueDate"],
    "commit": ["date"],
    "pipeline": ["created_at", "updated_at"],
    "job": ["started_at", "finished_at"],
    "mr": ["created_at", "merged_at"],
}

COLUMN_RENAME = {
    "jira": {
        "TicketKey": "ticket_key", "Status": "status",
        "StatusEntryDate": "status_entry_date", "StatusExitDate": "status_exit_date",
        "TimeInStatusHours": "time_in_status_hours", "Priority": "priority",
        "Assignee": "assignee", "AssigneeEmail": "assignee_email",
        "Created": "created", "Updated": "updated",
        "ResolutionDate": "resolution_date", "DueDate": "due_date",
        "SprintState": "sprint_state",
    },
    "commit": {
        "sha": "sha", "author": "author", "email": "email",
        "date": "commit_date", "title": "title", "TicketKey": "ticket_key",
    },
    "pipeline": {
        "pipeline_id": "pipeline_id", "status": "status", "ref": "ref",
        "sha": "sha", "created_at": "created_at", "updated_at": "updated_at",
        "duration_minutes": "duration_minutes", "is_production": "is_production",
    },
    "job": {
        "job_id": "job_id", "pipeline_id": "pipeline_id", "name": "name",
        "status": "status", "started_at": "started_at", "finished_at": "finished_at",
        "sha": "sha", "duration_minutes": "duration_minutes",
    },
    "mr": {
        "mr_id": "mr_id", "title": "title", "author": "author",
        "state": "state", "created_at": "created_at", "merged_at": "merged_at",
        "merge_time_hours": "merge_time_hours", "TicketKey": "ticket_key",
    },
}


def load_cleaned(engine, dry_run: bool = False) -> dict:
    log.info("=" * 60)
    log.info("COUCHE 1 : cleaned")
    log.info("=" * 60)
    stats = {}

    files = {
        "jira_status_history":  (CLEANED_DIR / "jira_cleaned_spark.csv",     "jira",     DATE_COLS["jira"]),
        "commits":              (CLEANED_DIR / "commits_cleaned_spark.csv",   "commit",   DATE_COLS["commit"]),
        "pipelines":            (CLEANED_DIR / "pipelines_cleaned_spark.csv", "pipeline", DATE_COLS["pipeline"]),
        "jobs":                 (CLEANED_DIR / "jobs_cleaned_spark.csv",      "job",      DATE_COLS["job"]),
        "merge_requests":       (CLEANED_DIR / "merge_requests_cleaned_spark.csv", "mr", DATE_COLS["mr"]),
    }

    for table_name, (filepath, rename_key, date_cols) in files.items():
        log.info(f"\n→ {table_name}")
        df = _load_csv(filepath, parse_dates=date_cols)
        if df is None:
            stats[table_name] = 0
            continue

        # Renommage des colonnes
        rename_map = COLUMN_RENAME[rename_key]
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

        # Extraction du project_key depuis ticket_key (ex: DL-123 → DL)
        if "ticket_key" in df.columns and "project_key" not in df.columns:
            df["project_key"] = df["ticket_key"].str.extract(r'^([A-Z]+)-\d+')

        _truncate("cleaned", table_name, engine, dry_run)
        stats[table_name] = _upsert(df, "cleaned", table_name, engine, dry_run=dry_run)

    return stats


# ════════════════════════════════════════════════════════════════════════════
# COUCHE 2 : features
# ════════════════════════════════════════════════════════════════════════════

def load_features(engine, dry_run: bool = False) -> dict:
    log.info("=" * 60)
    log.info("COUCHE 2 : features")
    log.info("=" * 60)
    stats = {}

    # 2.1 tickets_features
    log.info("\n→ tickets")
    df_tickets = _load_csv(
        FEATURES_DIR / "tickets_features_spark.csv",
        parse_dates=["Created", "ResolutionDate", "DueDate"],
    )
    if df_tickets is not None:
        df_tickets.rename(columns={
            "TicketKey": "ticket_key", "ProjectKey": "project_key",
            "Project": "project", "IssueType": "issue_type",
            "CurrentStatus": "current_status", "StatusCategory": "status_category",
            "Priority": "priority", "Assignee": "assignee",
            "AssigneeEmail": "assignee_email", "SprintState": "sprint_state",
            "Created": "created", "ResolutionDate": "resolution_date",
            "DueDate": "due_date",
        }, inplace=True)
        # Harmonisation snake_case
        df_tickets.columns = [c.lower() for c in df_tickets.columns]
        _truncate("features", "tickets", engine, dry_run)
        stats["tickets"] = _upsert(df_tickets, "features", "tickets", engine, dry_run=dry_run)

    # 2.2 assignee_metrics
    log.info("\n→ assignee_metrics")
    df_am = _load_csv(FEATURES_DIR / "assignee_metrics_spark.csv")
    if df_am is not None:
        df_am.columns = [c.lower() for c in df_am.columns]
        _truncate("features", "assignee_metrics", engine, dry_run)
        stats["assignee_metrics"] = _upsert(df_am, "features", "assignee_metrics", engine, dry_run=dry_run)

    # 2.3 project_metrics
    log.info("\n→ project_metrics")
    df_pm = _load_csv(FEATURES_DIR / "project_metrics_spark.csv")
    if df_pm is not None:
        df_pm.columns = [c.lower().replace(" ", "_") for c in df_pm.columns]
        _truncate("features", "project_metrics", engine, dry_run)
        stats["project_metrics"] = _upsert(df_pm, "features", "project_metrics", engine, dry_run=dry_run)

    return stats


# ════════════════════════════════════════════════════════════════════════════
# COUCHE 3 : dora_metrics
# ════════════════════════════════════════════════════════════════════════════

def load_dora_metrics(engine, dry_run: bool = False) -> dict:
    log.info("=" * 60)
    log.info("COUCHE 3 : dora_metrics")
    log.info("=" * 60)
    stats = {}

    dora_files = {
        "kpis":       METRICS_DIR / "dora_kpis_spark.csv",
        "summary":    METRICS_DIR / "dora_metrics_summary_spark.csv",
        "by_project": METRICS_DIR / "dora_by_project_spark.csv",
        "by_source":  METRICS_DIR / "dora_by_source_spark.csv",
        "weekly":     METRICS_DIR / "dora_metrics_weekly_spark.csv",
        "evolution":  METRICS_DIR / "dora_evolution_spark.csv",
        "jobs_stats": METRICS_DIR / "jobs_stats_spark.csv",
    }

    for table_name, filepath in dora_files.items():
        log.info(f"\n→ {table_name}")
        df = _load_csv(filepath)
        if df is None:
            stats[table_name] = 0
            continue

        # Harmonisation snake_case
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Renommage spécifique pour dora_by_project
        if table_name == "by_project":
            df.rename(columns={"projectkey": "project_key"}, inplace=True)

        # NE PAS truncate weekly (accumulation historique) - juste by_source et kpis
        if table_name not in ("weekly",):
            _truncate("dora_metrics", table_name, engine, dry_run)

        stats[table_name] = _upsert(df, "dora_metrics", table_name, engine, dry_run=dry_run)

    return stats


# ════════════════════════════════════════════════════════════════════════════
# COUCHE 4 : team_metrics
# ════════════════════════════════════════════════════════════════════════════

def load_team_metrics(engine, dry_run: bool = False) -> dict:
    log.info("=" * 60)
    log.info("COUCHE 4 : team_metrics")
    log.info("=" * 60)
    stats = {}

    team_files = {
        "person_dashboard":     TEAM_DIR / "person_dashboard_spark.csv",
        "assignee_performance": TEAM_DIR / "assignee_performance_spark.csv",
        "team_performance":     TEAM_DIR / "team_performance_spark.csv",
        "person_vs_team":       TEAM_DIR / "person_vs_team_spark.csv",
        "person_score_detail":  TEAM_DIR / "person_score_detail_spark.csv",
        "sprint_metrics":       TEAM_DIR / "sprint_metrics_spark.csv",
        "monthly_activity":     TEAM_DIR / "monthly_activity_spark.csv",
        "yearly_performance":   TEAM_DIR / "yearly_performance_spark.csv",
        "task_type_distribution": TEAM_DIR / "task_type_distribution_spark.csv",
    }

    for table_name, filepath in team_files.items():
        log.info(f"\n→ {table_name}")
        df = _load_csv(filepath)
        if df is None:
            stats[table_name] = 0
            continue

        # Harmonisation snake_case
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Renommages communs
        rename_map = {"projectkey": "project_key", "assigneeemail": "assignee_email"}
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

        _truncate("team_metrics", table_name, engine, dry_run)
        stats[table_name] = _upsert(df, "team_metrics", table_name, engine, dry_run=dry_run)

    return stats


# ════════════════════════════════════════════════════════════════════════════
# RAPPORT DE CHARGEMENT
# ════════════════════════════════════════════════════════════════════════════

def print_report(all_stats: dict):
    log.info("\n" + "=" * 60)
    log.info("RAPPORT DE CHARGEMENT")
    log.info("=" * 60)
    total = 0
    for layer, stats in all_stats.items():
        log.info(f"\n[{layer.upper()}]")
        for table, count in stats.items():
            status = "OK" if count > 0 else "VIDE/MANQUANT"
            log.info(f"  {table:<35} {count:>6} lignes  [{status}]")
            total += count
    log.info(f"\n  TOTAL INSÉRÉ : {total} lignes")
    log.info("=" * 60)


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

LAYER_FUNCTIONS = {
    "cleaned":      load_cleaned,
    "features":     load_features,
    "dora_metrics": load_dora_metrics,
    "team_metrics": load_team_metrics,
}


def main():
    parser = argparse.ArgumentParser(
        description="Chargement ordonné des données SmartBank vers PostgreSQL"
    )
    parser.add_argument(
        "--layer",
        choices=list(LAYER_FUNCTIONS.keys()),
        default=None,
        help="Charger uniquement cette couche (défaut: toutes)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Vérifie les fichiers et colonnes sans écrire en base",
    )
    args = parser.parse_args()

    if args.dry_run:
        log.info("[DRY-RUN] Aucune écriture en base de données")
        engine = None
    else:
        engine = get_engine()

    # Sélection des couches à charger
    layers_to_run = (
        {args.layer: LAYER_FUNCTIONS[args.layer]}
        if args.layer
        else LAYER_FUNCTIONS
    )

    all_stats = {}
    for layer_name, func in layers_to_run.items():
        all_stats[layer_name] = func(engine, dry_run=args.dry_run)

    print_report(all_stats)
    log.info("Chargement terminé.")


if __name__ == "__main__":
    main()
