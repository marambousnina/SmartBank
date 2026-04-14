"""
=============================================================
 SmartBank Metrics — Script 05 : Enregistrement en base
=============================================================
 Rôle  : Lire les CSV cleaned + features et les enregistrer
         dans PostgreSQL (schémas cleaned et features).

 Usage :
   python scripts/05_save_to_db.py              # tout charger
   python scripts/05_save_to_db.py --layer cleaned
   python scripts/05_save_to_db.py --layer features
   python scripts/05_save_to_db.py --dry-run    # sans écriture DB

 Prérequis :
   - Avoir exécuté 01_cleaning_spark.py  (data/cleaned/*.csv)
   - Avoir exécuté 02_feature_engineering_spark.py (data/features/*.csv)
   - PostgreSQL démarré + base smartbank_db créée
   - database/schema/01_create_schemas.sql exécuté
=============================================================
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import date

import pandas as pd
from sqlalchemy import text, inspect

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine

# ── Chemins ──────────────────────────────────────────────────────────────────
CLEANED_DIR  = ROOT_DIR / "data" / "cleaned"
FEATURES_DIR = ROOT_DIR / "data" / "features"

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def check_prerequisites(dry_run: bool) -> bool:
    """Vérifie que les fichiers CSV source existent."""
    files_required = {
        "cleaned": [
            CLEANED_DIR / "jira_cleaned_spark.csv",
            CLEANED_DIR / "commits_cleaned_spark.csv",
            CLEANED_DIR / "pipelines_cleaned_spark.csv",
            CLEANED_DIR / "jobs_cleaned_spark.csv",
            CLEANED_DIR / "merge_requests_cleaned_spark.csv",
        ],
        "features": [
            FEATURES_DIR / "tickets_features_spark.csv",
            FEATURES_DIR / "assignee_metrics_spark.csv",
            FEATURES_DIR / "project_metrics_spark.csv",
        ],
    }

    log.info("Vérification des fichiers CSV source...")
    all_ok = True
    for layer, files in files_required.items():
        for f in files:
            exists = f.exists()
            status = "OK" if exists else "MANQUANT"
            log.info(f"  [{status}] {f.name}")
            if not exists:
                all_ok = False

    if not all_ok:
        log.error("Des fichiers sont manquants. Lance d'abord :")
        log.error("  python scripts/01_cleaning_spark.py")
        log.error("  python scripts/02_feature_engineering_spark.py")

    return all_ok


def check_db_schemas(engine) -> bool:
    """Vérifie que les schémas PostgreSQL existent."""
    log.info("Vérification des schémas PostgreSQL...")
    required_schemas = ["cleaned", "features"]
    all_ok = True
    with engine.connect() as conn:
        for schema in required_schemas:
            result = conn.execute(text(
                f"SELECT schema_name FROM information_schema.schemata "
                f"WHERE schema_name = '{schema}'"
            ))
            exists = result.fetchone() is not None
            status = "OK" if exists else "ABSENT"
            log.info(f"  [{status}] schéma {schema}")
            if not exists:
                all_ok = False

    if not all_ok:
        log.error("Des schémas manquent. Lance :")
        log.error("  psql -U smartbank_user -d smartbank_db -f database/schema/01_create_schemas.sql")

    return all_ok


def load_csv(filepath: Path, parse_dates: list = None) -> pd.DataFrame:
    """Charge un CSV en DataFrame pandas."""
    df = pd.read_csv(filepath, parse_dates=parse_dates or [], low_memory=False)
    log.info(f"  Chargé  : {len(df):>6} lignes  ({filepath.name})")
    return df


def insert_to_db(df: pd.DataFrame, schema: str, table: str, engine,
                 dry_run: bool = False) -> int:
    """
    Vide la table puis insère le DataFrame.
    Retourne le nombre de lignes insérées.
    """
    if df is None or df.empty:
        log.warning(f"  [VIDE] {schema}.{table} — rien à insérer")
        return 0

    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    if dry_run:
        log.info(f"  [DRY-RUN] {schema}.{table} <- {len(df)} lignes")
        return len(df)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE"))

    df.to_sql(
        name=table,
        schema=schema,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    log.info(f"  [OK]    {schema}.{table} <- {len(df)} lignes inserees")
    return len(df)


# ════════════════════════════════════════════════════════════════════════════
# COUCHE cleaned
# ════════════════════════════════════════════════════════════════════════════

def save_cleaned(engine, dry_run: bool = False) -> dict:
    log.info("")
    log.info("=" * 55)
    log.info("  COUCHE : cleaned")
    log.info("=" * 55)
    stats = {}

    # ── 1. Jira status history ────────────────────────────────────────────
    log.info("\n[1/5] jira_status_history")
    df = load_csv(
        CLEANED_DIR / "jira_cleaned_spark.csv",
        parse_dates=["StatusEntryDate", "StatusExitDate",
                     "Created", "Updated", "ResolutionDate", "DueDate"],
    )
    df.rename(columns={
        "TicketKey":          "ticket_key",
        "Status":             "status",
        "StatusEntryDate":    "status_entry_date",
        "StatusExitDate":     "status_exit_date",
        "TimeInStatusHours":  "time_in_status_hours",
        "Priority":           "priority",
        "Assignee":           "assignee",
        "AssigneeEmail":      "assignee_email",
        "Created":            "created",
        "Updated":            "updated",
        "ResolutionDate":     "resolution_date",
        "DueDate":            "due_date",
        "SprintState":        "sprint_state",
        "ProjectKey":         "project_key",
    }, inplace=True)
    if "project_key" not in df.columns and "ticket_key" in df.columns:
        df["project_key"] = df["ticket_key"].str.extract(r'^([A-Z]+)-\d+')
    stats["jira_status_history"] = insert_to_db(
        df[["ticket_key", "status", "status_entry_date", "status_exit_date",
            "time_in_status_hours", "priority", "assignee", "assignee_email",
            "created", "updated", "resolution_date", "due_date",
            "sprint_state", "project_key"]],
        "cleaned", "jira_status_history", engine, dry_run,
    )

    # ── 2. Commits ────────────────────────────────────────────────────────
    log.info("\n[2/5] commits")
    df = load_csv(CLEANED_DIR / "commits_cleaned_spark.csv", parse_dates=["date"])
    df.rename(columns={
        "date":      "commit_date",
        "TicketKey": "ticket_key",
    }, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    cols_keep = ["sha", "author", "email", "commit_date", "title", "ticket_key"]
    stats["commits"] = insert_to_db(
        df[[c for c in cols_keep if c in df.columns]],
        "cleaned", "commits", engine, dry_run,
    )

    # ── 3. Pipelines ──────────────────────────────────────────────────────
    log.info("\n[3/5] pipelines")
    df = load_csv(
        CLEANED_DIR / "pipelines_cleaned_spark.csv",
        parse_dates=["created_at", "updated_at"],
    )
    df.columns = [c.lower() for c in df.columns]
    if "is_production" in df.columns:
        df["is_production"] = df["is_production"].map(
            {"True": True, "False": False, True: True, False: False}
        ).fillna(False)
    cols_keep = ["pipeline_id", "status", "ref", "sha", "created_at",
                 "updated_at", "duration_minutes", "is_production"]
    stats["pipelines"] = insert_to_db(
        df[[c for c in cols_keep if c in df.columns]],
        "cleaned", "pipelines", engine, dry_run,
    )

    # ── 4. Jobs ───────────────────────────────────────────────────────────
    log.info("\n[4/5] jobs")
    df = load_csv(
        CLEANED_DIR / "jobs_cleaned_spark.csv",
        parse_dates=["started_at", "finished_at"],
    )
    df.columns = [c.lower() for c in df.columns]
    cols_keep = ["job_id", "pipeline_id", "name", "status",
                 "started_at", "finished_at", "sha", "duration_minutes"]
    stats["jobs"] = insert_to_db(
        df[[c for c in cols_keep if c in df.columns]],
        "cleaned", "jobs", engine, dry_run,
    )

    # ── 5. Merge Requests ─────────────────────────────────────────────────
    log.info("\n[5/5] merge_requests")
    df = load_csv(
        CLEANED_DIR / "merge_requests_cleaned_spark.csv",
        parse_dates=["created_at", "merged_at"],
    )
    df.rename(columns={"TicketKey": "ticket_key"}, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    cols_keep = ["mr_id", "title", "author", "state",
                 "created_at", "merged_at", "merge_time_hours", "ticket_key"]
    stats["merge_requests"] = insert_to_db(
        df[[c for c in cols_keep if c in df.columns]],
        "cleaned", "merge_requests", engine, dry_run,
    )

    return stats


# ════════════════════════════════════════════════════════════════════════════
# COUCHE features
# ════════════════════════════════════════════════════════════════════════════

def save_features(engine, dry_run: bool = False) -> dict:
    log.info("")
    log.info("=" * 55)
    log.info("  COUCHE : features")
    log.info("=" * 55)
    stats = {}

    # ── 1. Tickets features ───────────────────────────────────────────────
    log.info("\n[1/3] tickets")
    df = load_csv(
        FEATURES_DIR / "tickets_features_spark.csv",
        parse_dates=["Created", "ResolutionDate", "DueDate"],
    )
    df.rename(columns={
        "TicketKey":     "ticket_key",
        "ProjectKey":    "project_key",
        "Project":       "project",
        "IssueType":     "issue_type",
        "CurrentStatus": "current_status",
        "StatusCategory":"status_category",
        "Priority":      "priority",
        "Assignee":      "assignee",
        "AssigneeEmail": "assignee_email",
        "SprintState":   "sprint_state",
        "Created":       "created",
        "ResolutionDate":"resolution_date",
        "DueDate":       "due_date",
    }, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    # Supprimer les colonnes dupliquees (garder la premiere occurrence)
    df = df.loc[:, ~df.columns.duplicated()]
    stats["tickets"] = insert_to_db(df, "features", "tickets", engine, dry_run)

    # ── 2. Assignee metrics ───────────────────────────────────────────────
    log.info("\n[2/3] assignee_metrics")
    df = load_csv(FEATURES_DIR / "assignee_metrics_spark.csv")
    df.columns = [c.lower() for c in df.columns]
    df.rename(columns={
        "nb_tickets_deployed_person": "nb_tickets_deployed",
        "avg_lead_time_person":       "avg_lead_time_hours",
    }, inplace=True)
    df.dropna(subset=["assignee"], inplace=True)
    stats["assignee_metrics"] = insert_to_db(
        df, "features", "assignee_metrics", engine, dry_run
    )

    # ── 3. Project metrics ────────────────────────────────────────────────
    log.info("\n[3/3] project_metrics")
    df = load_csv(FEATURES_DIR / "project_metrics_spark.csv")
    df.columns = [c.lower() for c in df.columns]
    df.rename(columns={"projectkey": "project_key"}, inplace=True)
    stats["project_metrics"] = insert_to_db(
        df, "features", "project_metrics", engine, dry_run
    )

    return stats


# ════════════════════════════════════════════════════════════════════════════
# RAPPORT
# ════════════════════════════════════════════════════════════════════════════

def print_report(all_stats: dict, dry_run: bool):
    mode = "DRY-RUN" if dry_run else "RÉEL"
    log.info("")
    log.info("=" * 55)
    log.info(f"  RAPPORT [{mode}]")
    log.info("=" * 55)
    total = 0
    for layer, stats in all_stats.items():
        log.info(f"\n  {layer.upper()}")
        for table, count in stats.items():
            ok = "OK" if count > 0 else "VIDE"
            log.info(f"    {table:<30} {count:>6} lignes  [{ok}]")
            total += count
    log.info(f"\n  TOTAL : {total} lignes")
    log.info("=" * 55)


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Enregistre cleaned + features dans PostgreSQL"
    )
    parser.add_argument(
        "--layer",
        choices=["cleaned", "features"],
        default=None,
        help="Charger uniquement cette couche (défaut: les deux)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Vérifie sans écrire en base",
    )
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("  SmartBank — Enregistrement vers PostgreSQL")
    log.info("=" * 55)

    # Vérification des fichiers CSV
    if not check_prerequisites(args.dry_run):
        sys.exit(1)

    # Connexion DB (sauf dry-run)
    engine = None
    if not args.dry_run:
        from database.db_connection import get_engine, test_connection
        if not test_connection():
            sys.exit(1)
        engine = get_engine()
        if not check_db_schemas(engine):
            sys.exit(1)

    # Chargement
    all_stats = {}
    if args.layer in (None, "cleaned"):
        all_stats["cleaned"] = save_cleaned(engine, dry_run=args.dry_run)

    if args.layer in (None, "features"):
        all_stats["features"] = save_features(engine, dry_run=args.dry_run)

    print_report(all_stats, args.dry_run)


if __name__ == "__main__":
    main()
