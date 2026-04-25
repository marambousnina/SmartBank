# -*- coding: utf-8 -*-
"""
SmartBank Metrics — Pipeline complet de chargement en base
===========================================================
Exécute dans l'ordre :

  ÉTAPE 1 — cleaned        : jira, commits, pipelines, jobs, merge_requests
  ÉTAPE 2 — features       : tickets, assignee_metrics, project_metrics, personnel
  ÉTAPE 3 — dora_metrics   : kpis, summary, by_project, weekly, jobs_stats
                             + project_monthly, project_weekly (script 03_dora_metrics_04)
                             + person_monthly, person_weekly   (script 03_dora_metrics_04)
  ÉTAPE 4 — team_metrics   : person_dashboard, team_performance, sprint_metrics, ...
  ÉTAPE 5 — ml_results     : predictions (Random Forest délai projet)
  ÉTAPE 6 — dwh (star)     : dim_* + fact_* (07_populate_dwh.py)
  ÉTAPE 7 — dwh (dora proj): fact_dora_project_monthly + fact_dora_project_weekly

Usage :
  python database/run_all.py              # tout charger
  python database/run_all.py --dry-run    # sans écriture DB
  python database/run_all.py --from 3     # à partir de l'étape 3
  python database/run_all.py --only 6     # une seule étape
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine
from database.load_to_db import load_cleaned, load_features, load_dora_metrics, load_team_metrics

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_FILE = ROOT_DIR / "database" / "run_all.log"
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.stream = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        _stream_handler,
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

DATA         = ROOT_DIR / "data"
METRICS_DIR  = DATA / "metrics"
PREDICTIONS_DIR = DATA / "predictions"


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _banner(step: int, title: str):
    log.info("")
    log.info("=" * 65)
    log.info(f"  ETAPE {step} -- {title}")
    log.info("=" * 65)


def _load_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        log.warning(f"  Fichier introuvable : {path.name}")
        return None
    df = pd.read_csv(path, low_memory=False)
    # Nettoyage BOM éventuel sur les noms de colonnes
    df.columns = [c.lstrip('\ufeff') for c in df.columns]
    log.info(f"  {len(df):>6} lignes  ← {path.name}")
    return df


def _ensure_table(conn, schema: str, table: str, ddl: str):
    """Crée la table si elle n'existe pas."""
    conn.execute(text(ddl))
    log.info(f"  Table {schema}.{table} prête")


def _load_df_to_table(engine, df: pd.DataFrame, schema: str, table: str,
                      truncate: bool = True) -> int:
    if df is None or df.empty:
        log.warning(f"  Pas de données pour {schema}.{table}")
        return 0

    df = df.copy()
    df.columns = [c.lower().replace(" ", "_").lstrip('\ufeff') for c in df.columns]

    # Ajouter snapshot_date si absente
    if "snapshot_date" not in df.columns:
        df["snapshot_date"] = date.today()

    # Récupérer les colonnes réelles de la table
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t
        """), {"s": schema, "t": table})
        db_cols = {row[0] for row in result.fetchall()}

    if db_cols:
        auto = {"id", "loaded_at"}
        valid = [c for c in df.columns if c in db_cols and c not in auto]
        ignored = [c for c in df.columns if c not in db_cols and c not in auto]
        if ignored:
            log.warning(f"    Colonnes ignorées : {ignored[:5]}")
        df = df[valid] if valid else df

    if df.empty or len(df.columns) == 0:
        log.warning(f"  Aucune colonne valide — {schema}.{table} ignoré")
        return 0

    try:
        with engine.begin() as conn:
            if truncate:
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE"))
            df.to_sql(table, schema=schema, con=conn, if_exists="append",
                      index=False, method="multi", chunksize=500)
        log.info(f"  [OK] {schema}.{table} ← {len(df)} lignes")
        return len(df)
    except Exception as e:
        log.error(f"  [ERREUR] {schema}.{table} : {e}")
        return 0


def _run_script(script_path: Path) -> bool:
    """Exécute un script Python externe et retourne True si succès."""
    log.info(f"  Exécution : {script_path.name}")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if result.stdout:
        for line in result.stdout.strip().split("\n")[-20:]:
            log.info(f"    {line}")
    if result.returncode != 0:
        log.error(f"  [ÉCHEC] {script_path.name} (code {result.returncode})")
        if result.stderr:
            for line in result.stderr.strip().split("\n")[-10:]:
                log.error(f"    ERR: {line}")
        return False
    log.info(f"  [OK] {script_path.name}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 3 EXTRA — dora_metrics.project_monthly / project_weekly / person_*
# ══════════════════════════════════════════════════════════════════════════════

DDL_DORA_PROJECT_MONTHLY = """
CREATE TABLE IF NOT EXISTS dora_metrics.project_monthly (
    id                    VARCHAR(100) PRIMARY KEY,
    project_id            VARCHAR(50),
    year_month            VARCHAR(10),
    metrics_period_start  DATE,
    metrics_period_end    DATE,
    lead_time             DOUBLE PRECISION,
    deployment_frequency  DOUBLE PRECISION,
    change_failure_rate   DOUBLE PRECISION,
    mttr                  DOUBLE PRECISION,
    cycle_time            DOUBLE PRECISION,
    throughput            INTEGER,
    wip                   INTEGER,
    bug_rate              DOUBLE PRECISION,
    velocity              INTEGER,
    code_coverage         DOUBLE PRECISION,
    data_source           VARCHAR(50),
    project_phase         VARCHAR(50),
    git_coverage_pct      DOUBLE PRECISION,
    calculation_timestamp TIMESTAMP,
    snapshot_date         DATE,
    loaded_at             TIMESTAMP DEFAULT NOW()
)
"""

DDL_DORA_PROJECT_WEEKLY = """
CREATE TABLE IF NOT EXISTS dora_metrics.project_weekly (
    id                    VARCHAR(100) PRIMARY KEY,
    project_id            VARCHAR(50),
    year_week             VARCHAR(12),
    metrics_period_start  DATE,
    metrics_period_end    DATE,
    lead_time             DOUBLE PRECISION,
    deployment_frequency  DOUBLE PRECISION,
    change_failure_rate   DOUBLE PRECISION,
    mttr                  DOUBLE PRECISION,
    cycle_time            DOUBLE PRECISION,
    throughput            INTEGER,
    wip                   INTEGER,
    bug_rate              DOUBLE PRECISION,
    velocity              INTEGER,
    code_coverage         DOUBLE PRECISION,
    data_source           VARCHAR(50),
    project_phase         VARCHAR(50),
    git_coverage_pct      DOUBLE PRECISION,
    calculation_timestamp TIMESTAMP,
    snapshot_date         DATE,
    loaded_at             TIMESTAMP DEFAULT NOW()
)
"""

DDL_DORA_PERSON_MONTHLY = """
CREATE TABLE IF NOT EXISTS dora_metrics.person_monthly (
    id                    VARCHAR(200) PRIMARY KEY,
    person_id             VARCHAR(200),
    person_name           VARCHAR(200),
    project_id            VARCHAR(50),
    year_month            VARCHAR(10),
    metrics_period_start  DATE,
    metrics_period_end    DATE,
    lead_time             DOUBLE PRECISION,
    change_failure_rate   DOUBLE PRECISION,
    mttr                  DOUBLE PRECISION,
    tasks_completed       INTEGER,
    bugs_created          DOUBLE PRECISION,
    bugs_fixed            INTEGER,
    commit_count          INTEGER,
    prs_merged            INTEGER,
    cycle_time            DOUBLE PRECISION,
    productivity_score    DOUBLE PRECISION,
    data_source           VARCHAR(50),
    project_phase         VARCHAR(50),
    git_coverage_pct      DOUBLE PRECISION,
    calculation_timestamp TIMESTAMP,
    snapshot_date         DATE,
    loaded_at             TIMESTAMP DEFAULT NOW()
)
"""

DDL_DORA_PERSON_WEEKLY = """
CREATE TABLE IF NOT EXISTS dora_metrics.person_weekly (
    id                    VARCHAR(200) PRIMARY KEY,
    person_id             VARCHAR(200),
    person_name           VARCHAR(200),
    project_id            VARCHAR(50),
    year_week             VARCHAR(12),
    metrics_period_start  DATE,
    metrics_period_end    DATE,
    lead_time             DOUBLE PRECISION,
    change_failure_rate   DOUBLE PRECISION,
    mttr                  DOUBLE PRECISION,
    tasks_completed       INTEGER,
    bugs_created          DOUBLE PRECISION,
    bugs_fixed            INTEGER,
    commit_count          INTEGER,
    prs_merged            INTEGER,
    cycle_time            DOUBLE PRECISION,
    productivity_score    DOUBLE PRECISION,
    data_source           VARCHAR(50),
    project_phase         VARCHAR(50),
    git_coverage_pct      DOUBLE PRECISION,
    calculation_timestamp TIMESTAMP,
    snapshot_date         DATE,
    loaded_at             TIMESTAMP DEFAULT NOW()
)
"""

DDL_ML_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS ml_results.project_predictions (
    project_key              VARCHAR(50) PRIMARY KEY,
    nb_tickets               INTEGER,
    nb_delayed_actual        INTEGER,
    nb_delayed_predicted     INTEGER,
    avg_delay_proba          DOUBLE PRECISION,
    max_delay_proba          DOUBLE PRECISION,
    delay_rate_actual_pct    DOUBLE PRECISION,
    delay_rate_predicted_pct DOUBLE PRECISION,
    risk_level               VARCHAR(50),
    project_will_delay       INTEGER,
    snapshot_date            DATE,
    loaded_at                TIMESTAMP DEFAULT NOW()
)
"""


def load_dora_extras(engine) -> dict:
    """Charge les 4 tables DORA par projet/personne × mensuel/hebdomadaire."""
    stats = {}
    today = date.today()

    with engine.begin() as conn:
        for ddl in [DDL_DORA_PROJECT_MONTHLY, DDL_DORA_PROJECT_WEEKLY,
                    DDL_DORA_PERSON_MONTHLY,  DDL_DORA_PERSON_WEEKLY]:
            conn.execute(text(ddl))
    log.info("  Tables dora_metrics créées / vérifiées")

    files = {
        ("project_monthly", "dora_project_monthly.csv"): ("project_id", "year_month"),
        ("project_weekly",  "dora_project_weekly.csv"):  ("project_id", "year_week"),
        ("person_monthly",  "dora_person_monthly.csv"):  ("person_id",  "year_month"),
        ("person_weekly",   "dora_person_weekly.csv"):   ("person_id",  "year_week"),
    }

    for (table, filename), (pk_col, period_col) in files.items():
        log.info(f"\n→ dora_metrics.{table}")
        df = _load_csv(METRICS_DIR / filename)
        if df is None:
            stats[table] = 0
            continue

        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Renommages depuis le CSV
        renames = {
            "projectkey": "project_id", "project_key": "project_id",
            "personkey":  "person_id",  "person_key":  "person_id",
        }
        df.rename(columns={k: v for k, v in renames.items() if k in df.columns}, inplace=True)

        # Normaliser period_col
        if "year_month" not in df.columns and "year-month" in df.columns:
            df.rename(columns={"year-month": "year_month"}, inplace=True)
        if "year_week" not in df.columns and "year-week" in df.columns:
            df.rename(columns={"year-week": "year_week"}, inplace=True)

        df["snapshot_date"] = today

        # Upsert par PRIMARY KEY (id)
        if "id" in df.columns:
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE dora_metrics.{table} RESTART IDENTITY CASCADE"))
                    df.to_sql(table, schema="dora_metrics", con=conn,
                              if_exists="append", index=False, method="multi", chunksize=500)
                log.info(f"  [OK] dora_metrics.{table} ← {len(df)} lignes")
                stats[table] = len(df)
            except Exception as e:
                log.error(f"  [ERREUR] dora_metrics.{table} : {e}")
                stats[table] = 0
        else:
            stats[table] = _load_df_to_table(engine, df, "dora_metrics", table, truncate=True)

    return stats


def load_ml_predictions(engine) -> dict:
    """Charge les prédictions ML dans ml_results.project_predictions."""
    stats = {}

    with engine.begin() as conn:
        conn.execute(text(DDL_ML_PREDICTIONS))
    log.info("  Table ml_results.project_predictions créée / vérifiée")

    log.info("\n→ ml_results.project_predictions")
    df = _load_csv(PREDICTIONS_DIR / "project_delay_predictions.csv")
    if df is None:
        return {"project_predictions": 0}

    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df.rename(columns={"projectkey": "project_key"}, inplace=True)
    df["snapshot_date"] = date.today()

    stats["project_predictions"] = _load_df_to_table(
        engine, df, "ml_results", "project_predictions", truncate=True
    )
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 6 — DWH Star Schema
# ══════════════════════════════════════════════════════════════════════════════

def run_dwh_populate() -> bool:
    script = ROOT_DIR / "scripts" / "07_populate_dwh.py"
    if not script.exists():
        log.error(f"  Script introuvable : {script}")
        return False
    engine = get_engine()
    # Single atomic TRUNCATE of all DWH tables with CASCADE (handles FK order automatically)
    dwh_tables = ", ".join([
        "dwh.fact_dora_project_weekly",
        "dwh.fact_dora_project_monthly",
        "dwh.fact_dora_snapshot",
        "dwh.fact_commits",
        "dwh.fact_deployments",
        "dwh.fact_tickets",
        "dwh.fact_team_performance",
        "dwh.dim_assignee",
        "dwh.dim_dora_level",
        "dwh.dim_risk_level",
        "dwh.dim_sprint",
        "dwh.dim_status",
        "dwh.dim_project",
        "dwh.dim_personnel",
        "dwh.dim_date",
    ])
    with engine.begin() as conn:
        try:
            conn.execute(text(f"TRUNCATE TABLE {dwh_tables} CASCADE"))
            log.info("  DWH tables truncated (CASCADE)")
        except Exception as e:
            log.warning(f"  TRUNCATE partiel (tables peut-etre absentes) : {e}")
    return _run_script(script)


# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 7 — DWH DORA Projet
# ══════════════════════════════════════════════════════════════════════════════

def run_dora_project() -> bool:
    from database.load_dora_project import run as dora_run
    try:
        dora_run()
        return True
    except Exception as e:
        log.error(f"  [ÉCHEC] load_dora_project : {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# RAPPORT FINAL
# ══════════════════════════════════════════════════════════════════════════════

def print_final_report(results: dict, t0: float):
    elapsed = time.time() - t0
    log.info("")
    log.info("=" * 65)
    log.info("  RAPPORT FINAL")
    log.info("=" * 65)

    total_rows = 0
    for step, data in results.items():
        status = "OK" if data.get("ok", True) else "FAIL"
        log.info(f"\n  [{status}] {step}")
        if isinstance(data.get("tables"), dict):
            for table, n in data["tables"].items():
                icon = "OK" if n > 0 else "--"
                log.info(f"      [{icon}] {table:<40} {n:>7} lignes")
                total_rows += n

    log.info("")
    log.info(f"  TOTAL LIGNES INSEREES : {total_rows:,}")
    log.info(f"  DUREE                 : {elapsed:.1f}s")
    log.info("=" * 65)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

STEPS = {
    1: "cleaned        (jira, commits, pipelines, jobs, merge_requests)",
    2: "features       (tickets, assignee_metrics, project_metrics, personnel)",
    3: "dora_metrics   (kpis, summary, by_project, weekly + project/person monthly/weekly)",
    4: "team_metrics   (person_dashboard, team_performance, sprint_metrics, ...)",
    5: "ml_results     (project_delay_predictions)",
    6: "dwh star       (dim_* + fact_* via 07_populate_dwh.py)",
    7: "dwh dora_proj  (fact_dora_project_monthly + fact_dora_project_weekly)",
}


def main():
    parser = argparse.ArgumentParser(description="Pipeline complet SmartBank → PostgreSQL")
    parser.add_argument("--dry-run", action="store_true", help="Vérifie sans écrire en DB")
    parser.add_argument("--from",    dest="from_step", type=int, default=1,
                        help="Démarrer à l'étape N (défaut: 1)")
    parser.add_argument("--only",    dest="only_step", type=int, default=None,
                        help="Exécuter uniquement l'étape N")
    args = parser.parse_args()

    t0 = time.time()
    log.info("=" * 65)
    log.info(f"  SmartBank — Pipeline complet  ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    log.info("=" * 65)

    if args.dry_run:
        log.info("  [DRY-RUN] Aucune écriture en base de données")

    engine = None if args.dry_run else get_engine()

    # Sélection des étapes à exécuter
    if args.only_step:
        steps_to_run = [args.only_step]
    else:
        steps_to_run = [s for s in STEPS if s >= args.from_step]

    results = {}

    for step in steps_to_run:
        _banner(step, STEPS[step])

        if step == 1:
            tables = {} if args.dry_run else load_cleaned(engine)
            results["1 — cleaned"] = {"ok": True, "tables": tables}

        elif step == 2:
            tables = {} if args.dry_run else load_features(engine)
            results["2 — features"] = {"ok": True, "tables": tables}

        elif step == 3:
            if args.dry_run:
                results["3 — dora_metrics"] = {"ok": True, "tables": {}}
            else:
                tables_std   = load_dora_metrics(engine)
                tables_extra = load_dora_extras(engine)
                results["3 — dora_metrics"] = {"ok": True, "tables": {**tables_std, **tables_extra}}

        elif step == 4:
            tables = {} if args.dry_run else load_team_metrics(engine)
            results["4 — team_metrics"] = {"ok": True, "tables": tables}

        elif step == 5:
            tables = {} if args.dry_run else load_ml_predictions(engine)
            results["5 — ml_results"] = {"ok": True, "tables": tables}

        elif step == 6:
            if args.dry_run:
                log.info("  [DRY-RUN] 07_populate_dwh.py ignoré")
                results["6 — dwh star"] = {"ok": True, "tables": {}}
            else:
                ok = run_dwh_populate()
                results["6 — dwh star"] = {"ok": ok, "tables": {"07_populate_dwh": 0 if not ok else 1}}

        elif step == 7:
            if args.dry_run:
                log.info("  [DRY-RUN] load_dora_project ignoré")
                results["7 — dwh dora_proj"] = {"ok": True, "tables": {}}
            else:
                ok = run_dora_project()
                results["7 — dwh dora_proj"] = {"ok": ok, "tables": {"dora_project_monthly": 0 if not ok else 1,
                                                                       "dora_project_weekly":  0 if not ok else 1}}

    print_final_report(results, t0)


if __name__ == "__main__":
    main()
