"""
=============================================================
 SmartBank Metrics — Tests de vérification base de données
=============================================================
 Vérifie que cleaned + features sont correctement chargés :
   - Tables non vides
   - Comptes cohérents avec les CSV sources
   - Valeurs clés présentes (tickets connus, dates, etc.)
   - Pas de doublons sur les clés primaires

 Usage :
   python database/test_db_load.py
   python database/test_db_load.py --verbose
=============================================================
"""

import argparse
import sys
from pathlib import Path

# Force UTF-8 sur le terminal Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine

CLEANED_DIR  = ROOT_DIR / "data" / "cleaned"
FEATURES_DIR = ROOT_DIR / "data" / "features"

# ── Couleurs terminal (simple) ────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

passed = 0
failed = 0
warnings = 0


def ok(msg: str):
    global passed
    passed += 1
    print(f"  {GREEN}[PASS]{RESET} {msg}")


def fail(msg: str):
    global failed
    failed += 1
    print(f"  {RED}[FAIL]{RESET} {msg}")


def warn(msg: str):
    global warnings
    warnings += 1
    print(f"  {YELLOW}[WARN]{RESET} {msg}")


def query(engine, sql: str):
    with engine.connect() as conn:
        return conn.execute(text(sql)).fetchall()


def scalar(engine, sql: str):
    rows = query(engine, sql)
    return rows[0][0] if rows else None


# ════════════════════════════════════════════════════════════════════════════
# TEST 1 — Connexion
# ════════════════════════════════════════════════════════════════════════════

def test_connection(engine):
    print("\n[1] Connexion à PostgreSQL")
    try:
        version = scalar(engine, "SELECT version()")
        ok(f"Connexion OK — {version[:50]}...")
    except Exception as e:
        fail(f"Connexion impossible : {e}")
        sys.exit(1)


# ════════════════════════════════════════════════════════════════════════════
# TEST 2 — Schémas existent
# ════════════════════════════════════════════════════════════════════════════

def test_schemas(engine):
    print("\n[2] Existence des schémas")
    for schema in ["cleaned", "features", "dora_metrics", "team_metrics", "ml_results"]:
        exists = scalar(engine,
            f"SELECT COUNT(*) FROM information_schema.schemata "
            f"WHERE schema_name = '{schema}'"
        )
        if exists:
            ok(f"Schéma '{schema}' existe")
        else:
            fail(f"Schéma '{schema}' MANQUANT — lance 01_create_schemas.sql")


# ════════════════════════════════════════════════════════════════════════════
# TEST 3 — Tables non vides (cleaned)
# ════════════════════════════════════════════════════════════════════════════

def test_cleaned_counts(engine, verbose: bool):
    print("\n[3] Comptes tables cleaned vs CSV")

    mapping = {
        "jira_status_history": CLEANED_DIR / "jira_cleaned_spark.csv",
        "commits":             CLEANED_DIR / "commits_cleaned_spark.csv",
        "pipelines":           CLEANED_DIR / "pipelines_cleaned_spark.csv",
        "jobs":                CLEANED_DIR / "jobs_cleaned_spark.csv",
        "merge_requests":      CLEANED_DIR / "merge_requests_cleaned_spark.csv",
    }

    for table, csv_path in mapping.items():
        db_count = scalar(engine, f"SELECT COUNT(*) FROM cleaned.{table}")

        if not csv_path.exists():
            warn(f"cleaned.{table} — CSV source introuvable, ne peut pas comparer")
            continue

        csv_count = sum(1 for _ in open(csv_path, encoding="utf-8-sig")) - 1  # -1 header

        if db_count == 0:
            fail(f"cleaned.{table} — VIDE (0 lignes)")
        elif db_count == csv_count:
            ok(f"cleaned.{table} — {db_count} lignes = CSV ({csv_count})")
        elif abs(db_count - csv_count) <= 5:
            warn(f"cleaned.{table} — {db_count} DB / {csv_count} CSV (diff <= 5, acceptable)")
        else:
            fail(f"cleaned.{table} — {db_count} DB vs {csv_count} CSV (écart : {abs(db_count - csv_count)})")

        if verbose:
            sample = query(engine, f"SELECT * FROM cleaned.{table} LIMIT 2")
            for row in sample:
                print(f"       Sample: {dict(zip([c for c in row._fields], row))}")


# ════════════════════════════════════════════════════════════════════════════
# TEST 4 — Tables non vides (features)
# ════════════════════════════════════════════════════════════════════════════

def test_features_counts(engine, verbose: bool):
    print("\n[4] Comptes tables features vs CSV")

    mapping = {
        "tickets":          FEATURES_DIR / "tickets_features_spark.csv",
        "assignee_metrics": FEATURES_DIR / "assignee_metrics_spark.csv",
        "project_metrics":  FEATURES_DIR / "project_metrics_spark.csv",
    }

    for table, csv_path in mapping.items():
        db_count = scalar(engine, f"SELECT COUNT(*) FROM features.{table}")

        if not csv_path.exists():
            warn(f"features.{table} — CSV source introuvable")
            continue

        csv_count = sum(1 for _ in open(csv_path, encoding="utf-8-sig")) - 1

        if db_count == 0:
            fail(f"features.{table} — VIDE (0 lignes)")
        elif db_count == csv_count:
            ok(f"features.{table} — {db_count} lignes = CSV ({csv_count})")
        elif abs(db_count - csv_count) <= 5:
            warn(f"features.{table} — {db_count} DB / {csv_count} CSV (diff <= 5, acceptable)")
        else:
            fail(f"features.{table} — {db_count} DB vs {csv_count} CSV")


# ════════════════════════════════════════════════════════════════════════════
# TEST 5 — Clés primaires uniques (pas de doublons)
# ════════════════════════════════════════════════════════════════════════════

def test_no_duplicates(engine):
    print("\n[5] Unicité des clés primaires")

    checks = [
        ("cleaned.commits",        "sha"),
        ("cleaned.pipelines",      "pipeline_id"),
        ("cleaned.jobs",           "job_id"),
        ("cleaned.merge_requests", "mr_id"),
    ]

    for table, key_col in checks:
        total   = scalar(engine, f"SELECT COUNT(*) FROM {table}")
        distinct = scalar(engine, f"SELECT COUNT(DISTINCT {key_col}) FROM {table}")
        if total == distinct:
            ok(f"{table}.{key_col} — aucun doublon ({total} lignes)")
        else:
            fail(f"{table}.{key_col} — {total - distinct} doublons détectés !")


# ════════════════════════════════════════════════════════════════════════════
# TEST 6 — Valeurs clés dans les données
# ════════════════════════════════════════════════════════════════════════════

def test_data_quality(engine):
    print("\n[6] Qualité des données")

    # Tickets : lead_time_hours non nuls pour les tickets finaux
    lt_final = scalar(engine, """
        SELECT COUNT(*) FROM features.tickets
        WHERE lead_time_is_final = 1 AND lead_time_hours IS NULL
    """)
    if lt_final == 0:
        ok("features.tickets — pas de lead_time NULL sur tickets finaux")
    else:
        fail(f"features.tickets — {lt_final} tickets finaux avec lead_time NULL")

    # Projets connus présents
    projects = ["DL", "MBC", "CSCCP25", "RIDIDP25"]
    for proj in projects:
        count = scalar(engine, f"""
            SELECT COUNT(*) FROM features.tickets WHERE project_key = '{proj}'
        """)
        if count and count > 0:
            ok(f"features.tickets — projet {proj} présent ({count} tickets)")
        else:
            warn(f"features.tickets — projet {proj} absent ou 0 tickets")

    # Pipelines : production vs total
    total_pipes   = scalar(engine, "SELECT COUNT(*) FROM cleaned.pipelines")
    prod_pipes    = scalar(engine, "SELECT COUNT(*) FROM cleaned.pipelines WHERE is_production = true")
    if total_pipes and total_pipes > 0:
        pct = round(prod_pipes / total_pipes * 100, 1)
        ok(f"cleaned.pipelines — {prod_pipes}/{total_pipes} production ({pct}%)")
    else:
        fail("cleaned.pipelines — table vide")

    # Jobs : statuts présents
    statuses = query(engine, """
        SELECT status, COUNT(*) as nb
        FROM cleaned.jobs
        GROUP BY status ORDER BY nb DESC LIMIT 5
    """)
    ok(f"cleaned.jobs — statuts: {[(r[0], r[1]) for r in statuses]}")

    # Risk levels dans features.tickets
    risk_levels = query(engine, """
        SELECT risk_level, COUNT(*) as nb
        FROM features.tickets
        GROUP BY risk_level ORDER BY nb DESC
    """)
    ok(f"features.tickets — risk levels: {[(r[0], r[1]) for r in risk_levels]}")

    # Score de risque : min/max
    risk_stats = query(engine, """
        SELECT MIN(risk_score), MAX(risk_score), ROUND(AVG(risk_score)::numeric, 1)
        FROM features.tickets
    """)
    if risk_stats:
        ok(f"features.tickets — risk_score min={risk_stats[0][0]} max={risk_stats[0][1]} avg={risk_stats[0][2]}")


# ════════════════════════════════════════════════════════════════════════════
# TEST 7 — Cohérence Jira ↔ Features
# ════════════════════════════════════════════════════════════════════════════

def test_jira_features_consistency(engine):
    print("\n[7] Cohérence Jira (cleaned) ↔ tickets (features)")

    jira_tickets = scalar(engine, """
        SELECT COUNT(DISTINCT ticket_key) FROM cleaned.jira_status_history
    """)
    feat_tickets = scalar(engine, """
        SELECT COUNT(DISTINCT ticket_key) FROM features.tickets
    """)

    if jira_tickets and feat_tickets:
        ok(f"Tickets uniques — Jira cleaned: {jira_tickets} | Features: {feat_tickets}")
        if feat_tickets <= jira_tickets:
            ok("features.tickets <= jira cleaned (normal : on garde 1 ligne par ticket)")
        else:
            fail(f"features.tickets ({feat_tickets}) > jira cleaned ({jira_tickets}) — anomalie")
    else:
        warn("Impossible de comparer (tables vides)")

    # Commits liés vs non liés
    linked = scalar(engine, """
        SELECT COUNT(*) FROM cleaned.commits WHERE ticket_key IS NOT NULL
    """)
    total  = scalar(engine, "SELECT COUNT(*) FROM cleaned.commits")
    if total:
        pct = round(linked / total * 100, 1)
        ok(f"cleaned.commits — {linked}/{total} liés à un ticket Jira ({pct}%)")


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Tests de vérification DB SmartBank")
    parser.add_argument("--verbose", action="store_true",
                        help="Affiche des lignes d'exemple pour chaque table")
    args = parser.parse_args()

    print("=" * 55)
    print("  SmartBank — Tests de vérification PostgreSQL")
    print("=" * 55)

    engine = get_engine()

    test_connection(engine)
    test_schemas(engine)
    test_cleaned_counts(engine, args.verbose)
    test_features_counts(engine, args.verbose)
    test_no_duplicates(engine)
    test_data_quality(engine)
    test_jira_features_consistency(engine)

    # ── Rapport final ─────────────────────────────────────────────────────
    total_tests = passed + failed + warnings
    print()
    print("=" * 55)
    print(f"  RÉSULTAT : {total_tests} tests")
    print(f"  {GREEN}PASS{RESET}  : {passed}")
    print(f"  {RED}FAIL{RESET}  : {failed}")
    print(f"  {YELLOW}WARN{RESET}  : {warnings}")
    print("=" * 55)

    if failed > 0:
        print(f"\n  {RED}Des tests ont échoué. Consulte les messages ci-dessus.{RESET}")
        sys.exit(1)
    else:
        print(f"\n  {GREEN}Tous les tests passent.{RESET}")


if __name__ == "__main__":
    main()
