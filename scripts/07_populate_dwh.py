"""
=============================================================
 SmartBank Metrics -- ETL : Populate Data Warehouse (DWH)
 Script  : 07_populate_dwh.py
 Etape   : 7 -- Alimenter le Star Schema DWH pour Power BI

 Flux :
   cleaned.* + features.*  -->  dwh.dim_* + dwh.fact_*

 Ordre d execution :
   1. dim_date          (calendrier 2020-2026)
   2. dim_assignee      (depuis features.tickets)
   3. dim_project       (depuis features.project_metrics)
   4. dim_status        (depuis cleaned.jira_status_history)
   5. dim_sprint        (depuis cleaned.jira_status_history)
   6. dim_risk_level    (statique)
   7. dim_dora_level    (statique)
   8. fact_tickets      (depuis features.tickets)
   9. fact_deployments  (depuis cleaned.pipelines)
  10. fact_commits      (depuis cleaned.commits)
  11. fact_dora_snapshot (depuis dora_metrics.weekly + kpis)
  12. fact_team_performance (depuis features.assignee_metrics)

 Usage :
   python scripts/07_populate_dwh.py
   python scripts/07_populate_dwh.py --reset   # vide et recharge
=============================================================
"""

import sys
import argparse
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
from datetime import date, timedelta
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine

# ---- helpers couleurs -------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}[OK]{RESET}   {msg}")
def fail(msg): print(f"  {RED}[ERR]{RESET}  {msg}")
def info(msg): print(f"  {YELLOW}[INFO]{RESET} {msg}")


def run_sql(engine, sql: str):
    with engine.begin() as conn:
        conn.execute(text(sql))


def load_df(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def upsert(engine, df: pd.DataFrame, table: str, if_exists: str = "append"):
    """Insere un DataFrame dans une table DWH."""
    if df.empty:
        info(f"{table} -- DataFrame vide, rien a inserer")
        return
    df.to_sql(
        name=table.split(".")[1],
        schema=table.split(".")[0],
        con=engine,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=500,
    )
    ok(f"{table} -- {len(df)} lignes inserees")


# =============================================================================
# 1. DIM_DATE
# =============================================================================
def populate_dim_date(engine, start_year: int = 2020, end_year: int = 2027):
    print("\n[1] dim_date")

    MONTHS_FR = ["Janvier","Fevrier","Mars","Avril","Mai","Juin",
                 "Juillet","Aout","Septembre","Octobre","Novembre","Decembre"]
    DAYS_FR   = ["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi","Dimanche"]

    rows = []
    d = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    while d <= end:
        iso_year, iso_week, iso_dow = d.isocalendar()
        rows.append({
            "date_key":     int(d.strftime("%Y%m%d")),
            "full_date":    d,
            "year":         d.year,
            "quarter":      (d.month - 1) // 3 + 1,
            "month":        d.month,
            "month_name":   MONTHS_FR[d.month - 1],
            "month_year":   d.strftime("%Y-%m"),
            "week_of_year": iso_week,
            "week_label":   f"{iso_year}-W{iso_week:02d}",
            "day_of_month": d.day,
            "day_of_week":  iso_dow,
            "day_name":     DAYS_FR[iso_dow - 1],
            "is_weekend":   iso_dow >= 6,
            "semester":     1 if d.month <= 6 else 2,
        })
        d += timedelta(days=1)

    df = pd.DataFrame(rows)
    upsert(engine, df, "dwh.dim_date", if_exists="append")


# =============================================================================
# 2. DIM_ASSIGNEE
# =============================================================================
def populate_dim_assignee(engine):
    print("\n[2] dim_assignee")

    df = load_df(engine, """
        SELECT DISTINCT
            assignee      AS assignee_name,
            assignee_email
        FROM features.tickets
        WHERE assignee IS NOT NULL
        ORDER BY assignee
    """)
    df["team"] = None
    df["role"] = None
    upsert(engine, df, "dwh.dim_assignee")


# =============================================================================
# 3. DIM_PROJECT
# =============================================================================
def populate_dim_project(engine):
    print("\n[3] dim_project")

    df = load_df(engine, """
        SELECT DISTINCT
            project_key  AS project_code,
            project      AS project_name
        FROM features.tickets
        WHERE project_key IS NOT NULL
        ORDER BY project_key
    """)
    df["domain"] = None
    upsert(engine, df, "dwh.dim_project")


# =============================================================================
# 4. DIM_STATUS
# =============================================================================
def populate_dim_status(engine):
    print("\n[4] dim_status")

    # Statuts connus avec leurs categories
    STATUS_MAP = {
        "In Analysis":           ("In Progress", False),
        "To Do":                 ("To Do",       False),
        "In Process":            ("In Progress", False),
        "Blocked":               ("In Progress", False),
        "In Review":             ("In Progress", False),
        "Correction Post Review":("In Progress", False),
        "Ready For Test":        ("In Progress", False),
        "QA In Process":         ("In Progress", False),
        "Correction Post QA":    ("In Progress", False),
        "To Be Deployed":        ("Done",        True),
        "Done":                  ("Done",        True),
        "Closed":                ("Done",        True),
        "Unknown":               ("Unknown",     False),
    }

    # Enrichit avec les statuts trouves dans la DB
    df_db = load_df(engine, """
        SELECT DISTINCT status FROM cleaned.jira_status_history
        WHERE status IS NOT NULL
    """)
    rows = []
    seen = set()
    for s in list(STATUS_MAP.keys()) + df_db["status"].tolist():
        if s in seen:
            continue
        seen.add(s)
        cat, is_final = STATUS_MAP.get(s, ("Unknown", False))
        rows.append({
            "status_name":     s,
            "status_category": cat,
            "is_final":        is_final,
        })
    df = pd.DataFrame(rows)
    upsert(engine, df, "dwh.dim_status")


# =============================================================================
# 5. DIM_SPRINT
# =============================================================================
def populate_dim_sprint(engine):
    print("\n[5] dim_sprint")

    # jira_status_history n a pas de colonne sprint_name explicite
    # on utilise sprint_state + project_key
    df = load_df(engine, """
        SELECT DISTINCT
            sprint_state  AS sprint_name,
            sprint_state,
            project_key   AS project_code
        FROM cleaned.jira_status_history
        WHERE sprint_state IS NOT NULL
    """)
    if df.empty:
        info("Aucun sprint trouve dans jira_status_history")
        return
    upsert(engine, df, "dwh.dim_sprint")


# =============================================================================
# 6. DIM_RISK_LEVEL (statique)
# =============================================================================
def populate_dim_risk_level(engine):
    print("\n[6] dim_risk_level")

    df = pd.DataFrame([
        {"risk_level": "Low",      "risk_min_score": 0.0,  "risk_max_score": 2.9,  "risk_color": "#00B050"},
        {"risk_level": "Medium",   "risk_min_score": 3.0,  "risk_max_score": 5.9,  "risk_color": "#FFC000"},
        {"risk_level": "High",     "risk_min_score": 6.0,  "risk_max_score": 7.9,  "risk_color": "#FF6600"},
        {"risk_level": "Critical", "risk_min_score": 8.0,  "risk_max_score": 10.0, "risk_color": "#FF0000"},
    ])
    upsert(engine, df, "dwh.dim_risk_level")


# =============================================================================
# 7. DIM_DORA_LEVEL (statique)
# =============================================================================
def populate_dim_dora_level(engine):
    print("\n[7] dim_dora_level")

    df = pd.DataFrame([
        {"dora_level": "Elite",  "dora_score": 4, "dora_color": "#00B050"},
        {"dora_level": "High",   "dora_score": 3, "dora_color": "#92D050"},
        {"dora_level": "Medium", "dora_score": 2, "dora_color": "#FFC000"},
        {"dora_level": "Low",    "dora_score": 1, "dora_color": "#FF0000"},
    ])
    upsert(engine, df, "dwh.dim_dora_level")


# =============================================================================
# 8. FACT_TICKETS
# =============================================================================
def populate_fact_tickets(engine):
    print("\n[8] fact_tickets")

    df = load_df(engine, """
        SELECT
            t.ticket_key,
            -- date_keys
            CASE WHEN t.created IS NOT NULL
                 THEN CAST(TO_CHAR(t.created, 'YYYYMMDD') AS INTEGER) END   AS date_key_created,
            CASE WHEN t.resolution_date IS NOT NULL
                 THEN CAST(TO_CHAR(t.resolution_date, 'YYYYMMDD') AS INTEGER) END AS date_key_resolved,
            CASE WHEN t.due_date IS NOT NULL
                 THEN CAST(TO_CHAR(t.due_date, 'YYYYMMDD') AS INTEGER) END  AS date_key_due,
            -- dim keys
            da.assignee_key,
            dp.project_key,
            ds.status_key,
            dsp.sprint_key,
            dr.risk_key,
            -- mesures
            t.lead_time_hours,
            t.cycle_time_hours,
            t.time_blocked_hours,
            t.time_in_review_hours,
            t.time_in_qa_hours,
            t.nb_status_changes,
            t.nb_commits,
            t.nb_mrs,
            t.avg_merge_time_hours,
            t.risk_score,
            -- indicateurs
            t.is_delayed,
            t.is_bug,
            t.was_blocked,
            t.lead_time_is_final,
            -- attributs degrades
            t.issue_type,
            t.priority,
            t.data_source
        FROM features.tickets t
        LEFT JOIN dwh.dim_assignee da ON da.assignee_name = t.assignee
        LEFT JOIN dwh.dim_project  dp ON dp.project_code  = t.project_key
        LEFT JOIN dwh.dim_status   ds ON ds.status_name   = t.current_status
        LEFT JOIN dwh.dim_sprint   dsp ON dsp.sprint_name = t.sprint_state
                                      AND dsp.project_code = t.project_key
        LEFT JOIN dwh.dim_risk_level dr ON dr.risk_level  = t.risk_level
    """)
    upsert(engine, df, "dwh.fact_tickets")


# =============================================================================
# 9. FACT_DEPLOYMENTS
# =============================================================================
def populate_fact_deployments(engine):
    print("\n[9] fact_deployments")

    df = load_df(engine, """
        SELECT
            p.pipeline_id,
            CASE WHEN p.created_at IS NOT NULL
                 THEN CAST(TO_CHAR(p.created_at, 'YYYYMMDD') AS INTEGER) END AS date_key,
            p.duration_minutes,
            p.is_production,
            (p.status = 'failed') AS is_failed,
            p.status,
            p.ref,
            p.sha
        FROM cleaned.pipelines p
    """)
    upsert(engine, df, "dwh.fact_deployments")


# =============================================================================
# 10. FACT_COMMITS
# =============================================================================
def populate_fact_commits(engine):
    print("\n[10] fact_commits")

    df = load_df(engine, """
        SELECT
            c.sha,
            CASE WHEN c.commit_date IS NOT NULL
                 THEN CAST(TO_CHAR(c.commit_date, 'YYYYMMDD') AS INTEGER) END AS date_key,
            da.assignee_key,
            dp.project_key,
            c.ticket_key,
            CASE WHEN c.ticket_key IS NOT NULL THEN 'jira+git' ELSE 'git_only' END AS data_source,
            c.title
        FROM cleaned.commits c
        LEFT JOIN dwh.dim_assignee da ON da.assignee_name  = c.author
        LEFT JOIN features.tickets t  ON t.ticket_key      = c.ticket_key
        LEFT JOIN dwh.dim_project  dp ON dp.project_code   = t.project_key
    """)
    upsert(engine, df, "dwh.fact_commits")


# =============================================================================
# 11. FACT_DORA_SNAPSHOT  (depuis dora_metrics.weekly si disponible)
# =============================================================================
def populate_fact_dora_snapshot(engine):
    print("\n[11] fact_dora_snapshot")

    # Verifie si dora_metrics.weekly est peuple
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dora_metrics.weekly")).scalar()

    if not count or count == 0:
        info("dora_metrics.weekly vide -- utilise cleaned.pipelines pour approximation")
        df = load_df(engine, """
            SELECT
                TO_CHAR(DATE_TRUNC('week', created_at), 'YYYY-"W"IW') AS week_label,
                CAST(TO_CHAR(MIN(created_at), 'YYYYMMDD') AS INTEGER)  AS date_key,
                COUNT(*)                                                AS deployment_count,
                COUNT(*) / 7.0                                          AS deploy_freq_per_week,
                COUNT(*)                                                AS total_pipelines,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)     AS failed_pipelines,
                ROUND(
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                )                                                       AS cfr_pct,
                NULL::DOUBLE PRECISION                                  AS lead_time_jira_hours,
                NULL::DOUBLE PRECISION                                  AS lead_time_git_hours,
                NULL::DOUBLE PRECISION                                  AS lead_time_global_hours,
                NULL::DOUBLE PRECISION                                  AS mttr_global_hours
            FROM cleaned.pipelines
            WHERE created_at IS NOT NULL
            GROUP BY DATE_TRUNC('week', created_at)
            ORDER BY DATE_TRUNC('week', created_at)
        """)
    else:
        df = load_df(engine, """
            SELECT
                w.week                                                  AS week_label,
                CAST(REPLACE(w.week, '-W', '') || '01' AS INTEGER)     AS date_key,
                w.deployment_count,
                w.deployment_count / 7.0                               AS deploy_freq_per_week,
                w.total_pipelines,
                w.failed_pipelines,
                w.change_failure_rate                                   AS cfr_pct,
                NULL::DOUBLE PRECISION                                  AS lead_time_jira_hours,
                NULL::DOUBLE PRECISION                                  AS lead_time_git_hours,
                NULL::DOUBLE PRECISION                                  AS lead_time_global_hours,
                NULL::DOUBLE PRECISION                                  AS mttr_global_hours
            FROM dora_metrics.weekly w
        """)

    # Associe les niveaux DORA
    dora_levels = load_df(engine, "SELECT dora_key, dora_level FROM dwh.dim_dora_level")
    level_map = dict(zip(dora_levels["dora_level"], dora_levels["dora_key"]))

    def deploy_level_key(freq):
        if freq is None: return level_map.get("Low")
        if freq >= 1:    return level_map.get("Elite")
        if freq >= 1/7:  return level_map.get("High")
        if freq >= 1/30: return level_map.get("Medium")
        return level_map.get("Low")

    def cfr_level_key(pct):
        if pct is None: return level_map.get("Low")
        if pct <= 5:    return level_map.get("Elite")
        if pct <= 10:   return level_map.get("High")
        if pct <= 15:   return level_map.get("Medium")
        return level_map.get("Low")

    df["deploy_freq_key"] = df["deploy_freq_per_week"].apply(deploy_level_key)
    df["cfr_key"]         = df["cfr_pct"].apply(cfr_level_key)
    df["lead_time_key"]   = level_map.get("Low")   # sera mis a jour apres script 03
    df["mttr_key"]        = level_map.get("Elite")  # idem
    df["snapshot_date"]   = date.today()

    upsert(engine, df, "dwh.fact_dora_snapshot")


# =============================================================================
# 12. FACT_TEAM_PERFORMANCE
# =============================================================================
def populate_fact_team_performance(engine):
    print("\n[12] fact_team_performance")

    df = load_df(engine, """
        SELECT
            CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INTEGER) AS date_key,
            da.assignee_key,
            NULL::INTEGER                                        AS project_key,
            am.nb_tickets_assigned,
            am.nb_tickets_deployed                              AS nb_tickets_done,
            NULL::INTEGER                                        AS nb_bugs_assigned,
            NULL::INTEGER                                        AS nb_delayed,
            NULL::INTEGER                                        AS nb_commits_total,
            NULL::INTEGER                                        AS nb_mrs_total,
            am.avg_lead_time_hours,
            NULL::DOUBLE PRECISION                               AS avg_cycle_time_hours,
            NULL::DOUBLE PRECISION                               AS avg_time_blocked_hours,
            am.on_time_rate,
            CASE WHEN am.nb_tickets_assigned > 0
                 THEN ROUND(am.nb_tickets_deployed * 100.0 / am.nb_tickets_assigned, 2)
                 ELSE 0 END                                      AS completion_rate,
            NULL::DOUBLE PRECISION                               AS performance_score,
            CURRENT_DATE                                         AS snapshot_date
        FROM features.assignee_metrics am
        JOIN dwh.dim_assignee da ON da.assignee_name = am.assignee
        WHERE am.assignee IS NOT NULL
    """)
    # Cast colonnes nullable INTEGER (pandas object -> Int64)
    int_cols = ["project_key", "nb_bugs_assigned", "nb_delayed",
                "nb_commits_total", "nb_mrs_total"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.array(df[c], dtype=pd.Int64Dtype())

    upsert(engine, df, "dwh.fact_team_performance")


# =============================================================================
# MAIN
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="ETL DWH SmartBank pour Power BI")
    parser.add_argument("--reset", action="store_true",
                        help="Vide toutes les tables DWH avant de recharger")
    args = parser.parse_args()

    print("=" * 60)
    print("  SmartBank -- Alimentation Data Warehouse (Power BI)")
    print("=" * 60)

    engine = get_engine()

    # -- Verification schema DWH existe
    with engine.connect() as conn:
        schema_exists = conn.execute(text(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'dwh'"
        )).scalar()
    if not schema_exists:
        fail("Schema 'dwh' introuvable -- lance d'abord: psql -f database/schema/02_star_schema_dwh.sql")
        sys.exit(1)

    # -- Reset si demande
    if args.reset:
        info("Reset du DWH...")
        TABLES_ORDER = [
            "dwh.fact_team_performance",
            "dwh.fact_dora_snapshot",
            "dwh.fact_commits",
            "dwh.fact_deployments",
            "dwh.fact_tickets",
            "dwh.dim_dora_level",
            "dwh.dim_risk_level",
            "dwh.dim_sprint",
            "dwh.dim_status",
            "dwh.dim_project",
            "dwh.dim_assignee",
            "dwh.dim_date",
        ]
        with engine.begin() as conn:
            for t in TABLES_ORDER:
                conn.execute(text(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE"))
        ok("Tables DWH videes")

    # -- Alimentation dans l ordre (dimensions en premier)
    populate_dim_date(engine)
    populate_dim_assignee(engine)
    populate_dim_project(engine)
    populate_dim_status(engine)
    populate_dim_sprint(engine)
    populate_dim_risk_level(engine)
    populate_dim_dora_level(engine)

    populate_fact_tickets(engine)
    populate_fact_deployments(engine)
    populate_fact_commits(engine)
    populate_fact_dora_snapshot(engine)
    populate_fact_team_performance(engine)

    print()
    print("=" * 60)
    print(f"  {GREEN}DWH alimente avec succes !{RESET}")
    print("=" * 60)
    print()
    print("  Prochaines etapes Power BI :")
    print("  1. Ouvrir Power BI Desktop")
    print("  2. Obtenir les donnees > PostgreSQL")
    print("  3. Serveur : localhost:5432  Base : smartbank_db")
    print("  4. Importer les tables dwh.*")
    print("  5. Dans le modele, verifier les relations automatiques")
    print("  6. Les date_key (INTEGER YYYYMMDD) relient toutes les faits a dim_date")
    print()


if __name__ == "__main__":
    main()
