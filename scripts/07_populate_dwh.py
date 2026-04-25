"""
=============================================================
 SmartBank Metrics -- ETL : Populate Data Warehouse (DWH)
 Script  : 07_populate_dwh.py
 Etape   : 7 -- Alimenter le Star Schema DWH pour Power BI

 Flux :
   cleaned.* + features.* + personnel.csv  -->  dwh.dim_* + dwh.fact_*

 Ordre d execution :
   1.  dim_date           (calendrier 2020-2027)
   2.  dim_personnel      (depuis features/personnel.csv)
   3.  dim_assignee       (depuis personnel.csv — team + departement remplis)
   4.  dim_project        (depuis features.tickets + domain mapping)
   5.  dim_status         (depuis cleaned.jira_status_history)
   6.  dim_sprint         (depuis cleaned.jira_status_history)
   7.  dim_risk_level     (statique)
   8.  dim_dora_level     (statique)
   9.  fact_tickets       (depuis features.tickets)
  10.  fact_deployments   (depuis cleaned.pipelines)
  11.  fact_commits       (depuis cleaned.commits + email_person_map)
  12.  fact_dora_snapshot (depuis dora_metrics.weekly + kpis)
  13.  fact_team_performance (depuis features.tickets — colonnes NULL remplies)

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

FEATURES_DIR = ROOT_DIR / "data" / "features"

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


def _load_personnel() -> pd.DataFrame:
    path = FEATURES_DIR / "personnel.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def _load_email_map() -> pd.DataFrame:
    path = FEATURES_DIR / "email_person_map.csv"
    if not path.exists():
        return pd.DataFrame(columns=["alias_email", "canonical_email"])
    return pd.read_csv(path, encoding="utf-8-sig")


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
# 2. DIM_PERSONNEL  (nouvelle dimension — source : personnel.csv)
# =============================================================================
def populate_dim_personnel(engine):
    print("\n[2] dim_personnel")

    personnel = _load_personnel()
    if personnel.empty:
        info("personnel.csv introuvable — etape ignoree")
        return

    df = personnel[["id", "nom", "email", "departement", "equipe", "source"]].copy()
    df = df.rename(columns={"id": "person_id"})
    df = df.dropna(subset=["person_id"])

    upsert(engine, df, "dwh.dim_personnel")


# =============================================================================
# 3. DIM_ASSIGNEE  (enrichi depuis personnel.csv : team + role remplis)
# =============================================================================
def populate_dim_assignee(engine):
    print("\n[3] dim_assignee")

    personnel = _load_personnel()

    if not personnel.empty:
        # team = equipe, role = source (git/jira/git+jira)
        df = personnel[["nom", "email", "equipe", "source"]].copy()
        df = df.rename(columns={
            "nom":    "assignee_name",
            "email":  "assignee_email",
            "equipe": "team",
            "source": "role",
        })
    else:
        info("personnel.csv absent — repli sur features.tickets")
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
# 4. DIM_PROJECT  (domain derive du nom du projet)
# =============================================================================
DOMAIN_MAP = {
    "cross sell":     "Banking Products",
    "credit":         "Banking Products",
    "data lake":      "Data & BI",
    "data":           "Data & BI",
    "mobile":         "Digital Banking",
    "neo bank":       "Digital Banking",
    "onboarding":     "Digital Banking",
    "api gateway":    "Architecture",
    "payment":        "Payments",
    "fraud":          "AI / Fraud",
    "cloud":          "Infrastructure",
    "refonte":        "Data & BI",
    "indicateur":     "Data & BI",
}

def _derive_domain(project_name: str) -> str:
    if not isinstance(project_name, str):
        return "Autre"
    name_lower = project_name.lower()
    for keyword, domain in DOMAIN_MAP.items():
        if keyword in name_lower:
            return domain
    return "Autre"


def populate_dim_project(engine):
    print("\n[4] dim_project")

    # Priorité 1 : project_metrics (a le vrai nom via project_name)
    try:
        df = load_df(engine, """
            SELECT DISTINCT project_key AS project_code, project_name
            FROM features.project_metrics
            WHERE project_key IS NOT NULL AND project_name IS NOT NULL
            ORDER BY project_key
        """)
    except Exception:
        df = pd.DataFrame()

    # Priorité 2 : tickets CSV si DB a des codes à la place des noms
    if df.empty or df["project_name"].isna().all():
        tickets_csv = FEATURES_DIR / "tickets_features_spark.csv"
        if tickets_csv.exists():
            raw = pd.read_csv(tickets_csv, usecols=["ProjectKey", "Project"])
            df = (raw.dropna(subset=["Project"])
                     .drop_duplicates(subset="ProjectKey")
                     .rename(columns={"ProjectKey": "project_code", "Project": "project_name"}))

    df["domain"] = df["project_name"].apply(_derive_domain)
    upsert(engine, df, "dwh.dim_project")

    # -- Ajouter dead_line et variance_budget si absents
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE dwh.dim_project ADD COLUMN IF NOT EXISTS dead_line DATE"))
        conn.execute(text("ALTER TABLE dwh.dim_project ADD COLUMN IF NOT EXISTS variance_budget NUMERIC(12,2)"))

    # -- Calculer variance_budget uniquement pour les projets Actifs
    # Budget = somme des jours-personne travailles x 200 DT
    try:
        budget_df = load_df(engine, """
            SELECT
                UPPER(sub.project_key)                           AS project_code,
                ROUND(SUM(sub.person_days * 200.0)::numeric, 2) AS variance_budget
            FROM (
                SELECT
                    c.project_key,
                    c.author_email,
                    COUNT(DISTINCT c.created_at::date) AS person_days
                FROM cleaned.commits c
                WHERE c.project_key IS NOT NULL
                  AND c.author_email IS NOT NULL
                  AND c.created_at   IS NOT NULL
                GROUP BY c.project_key, c.author_email
            ) sub
            GROUP BY UPPER(sub.project_key)
        """)
        if not budget_df.empty:
            with engine.begin() as conn:
                for _, row in budget_df.iterrows():
                    conn.execute(text("""
                        UPDATE dwh.dim_project
                        SET variance_budget = :budget
                        WHERE UPPER(project_code) = :code
                          AND COALESCE(status, 'Actif') = 'Actif'
                    """), {"budget": float(row["variance_budget"]), "code": str(row["project_code"])})
        ok("dim_project.variance_budget mis a jour (projets Actifs)")
    except Exception as exc:
        info(f"variance_budget calcul ignore : {exc}")


# =============================================================================
# 5. DIM_STATUS
# =============================================================================
def populate_dim_status(engine):
    print("\n[5] dim_status")

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
# 6. DIM_SPRINT
# =============================================================================
def populate_dim_sprint(engine):
    print("\n[6] dim_sprint")

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
# 7. DIM_RISK_LEVEL (statique)
# =============================================================================
def populate_dim_risk_level(engine):
    print("\n[7] dim_risk_level")

    df = pd.DataFrame([
        {"risk_level": "Low",      "risk_min_score": 0.0,  "risk_max_score": 2.9,  "risk_color": "#00B050"},
        {"risk_level": "Medium",   "risk_min_score": 3.0,  "risk_max_score": 5.9,  "risk_color": "#FFC000"},
        {"risk_level": "High",     "risk_min_score": 6.0,  "risk_max_score": 7.9,  "risk_color": "#FF6600"},
        {"risk_level": "Critical", "risk_min_score": 8.0,  "risk_max_score": 10.0, "risk_color": "#FF0000"},
    ])
    upsert(engine, df, "dwh.dim_risk_level")


# =============================================================================
# 8. DIM_DORA_LEVEL (statique)
# =============================================================================
def populate_dim_dora_level(engine):
    print("\n[8] dim_dora_level")

    df = pd.DataFrame([
        {"dora_level": "Elite",  "dora_score": 4, "dora_color": "#00B050"},
        {"dora_level": "High",   "dora_score": 3, "dora_color": "#92D050"},
        {"dora_level": "Medium", "dora_score": 2, "dora_color": "#FFC000"},
        {"dora_level": "Low",    "dora_score": 1, "dora_color": "#FF0000"},
    ])
    upsert(engine, df, "dwh.dim_dora_level")


# =============================================================================
# 9. FACT_TICKETS
# =============================================================================
def populate_fact_tickets(engine):
    print("\n[9] fact_tickets")

    df = load_df(engine, """
        SELECT
            t.ticket_key,
            CASE WHEN t.created IS NOT NULL
                 THEN CAST(TO_CHAR(t.created, 'YYYYMMDD') AS INTEGER) END         AS date_key_created,
            CASE WHEN t.resolution_date IS NOT NULL
                 THEN CAST(TO_CHAR(t.resolution_date, 'YYYYMMDD') AS INTEGER) END AS date_key_resolved,
            CASE WHEN t.due_date IS NOT NULL
                 THEN CAST(TO_CHAR(t.due_date, 'YYYYMMDD') AS INTEGER) END        AS date_key_due,
            da.assignee_key,
            dp.project_key,
            ds.status_key,
            dsp.sprint_key,
            dr.risk_key,
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
            t.is_delayed,
            t.is_bug,
            t.was_blocked,
            t.lead_time_is_final,
            t.issue_type,
            t.priority,
            t.data_source
        FROM features.tickets t
        LEFT JOIN dwh.dim_assignee da  ON da.assignee_name = t.assignee
        LEFT JOIN dwh.dim_project  dp  ON dp.project_code  = t.project_key
        LEFT JOIN dwh.dim_status   ds  ON ds.status_name   = t.current_status
        LEFT JOIN dwh.dim_sprint   dsp ON dsp.sprint_name  = t.sprint_state
                                      AND dsp.project_code = t.project_key
        LEFT JOIN dwh.dim_risk_level dr ON dr.risk_level   = t.risk_level
    """)
    upsert(engine, df, "dwh.fact_tickets")


# =============================================================================
# 10. FACT_DEPLOYMENTS
# =============================================================================
def populate_fact_deployments(engine):
    print("\n[10] fact_deployments")

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
# 11. FACT_COMMITS  (jointure corrigee via email au lieu du nom hache)
# =============================================================================
def populate_fact_commits(engine):
    print("\n[11] fact_commits")

    email_map = _load_email_map()

    # Construire mapping email → assignee_key dans le DWH
    dim_assignee = load_df(engine, """
        SELECT assignee_key, assignee_email
        FROM dwh.dim_assignee
        WHERE assignee_email IS NOT NULL
    """)

    # Normaliser les emails
    dim_assignee["assignee_email"] = dim_assignee["assignee_email"].str.strip().str.lower()

    if not email_map.empty:
        email_map["alias_email"]     = email_map["alias_email"].str.strip().str.lower()
        email_map["canonical_email"] = email_map["canonical_email"].str.strip().str.lower()
        # alias → canonical → assignee_key
        email_to_key = (
            email_map
            .merge(dim_assignee.rename(columns={"assignee_email": "canonical_email"}),
                   on="canonical_email", how="left")
            .set_index("alias_email")["assignee_key"]
            .to_dict()
        )
    else:
        email_to_key = dict(zip(dim_assignee["assignee_email"], dim_assignee["assignee_key"]))

    commits = load_df(engine, """
        SELECT
            c.sha,
            CASE WHEN c.commit_date IS NOT NULL
                 THEN CAST(TO_CHAR(c.commit_date, 'YYYYMMDD') AS INTEGER) END AS date_key,
            LOWER(TRIM(c.email))  AS commit_email,
            dp.project_key,
            c.ticket_key,
            CASE WHEN c.ticket_key IS NOT NULL THEN 'jira+git' ELSE 'git_only' END AS data_source,
            c.title
        FROM cleaned.commits c
        LEFT JOIN features.tickets t  ON t.ticket_key    = c.ticket_key
        LEFT JOIN dwh.dim_project  dp ON dp.project_code = t.project_key
    """)

    # Résoudre assignee_key via email (plus fiable que le nom hachéauteur)
    commits["assignee_key"] = commits["commit_email"].map(email_to_key)
    commits = commits.drop(columns=["commit_email"])

    upsert(engine, commits, "dwh.fact_commits")


# =============================================================================
# 12. FACT_DORA_SNAPSHOT  (lead_time_key calculé depuis données réelles)
# =============================================================================
def populate_fact_dora_snapshot(engine):
    print("\n[12] fact_dora_snapshot")

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dora_metrics.weekly")).scalar()

    if not count or count == 0:
        info("dora_metrics.weekly vide -- approximation depuis cleaned.pipelines")
        df = load_df(engine, """
            SELECT
                TO_CHAR(DATE_TRUNC('week', created_at), 'YYYY-"W"IW') AS week_label,
                CAST(TO_CHAR(DATE_TRUNC('week', MIN(created_at)), 'YYYYMMDD') AS INTEGER) AS date_key,
                COUNT(*)                                                AS deployment_count,
                COUNT(*) / 7.0                                          AS deploy_freq_per_week,
                COUNT(*)                                                AS total_pipelines,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)     AS failed_pipelines,
                ROUND(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                                        AS cfr_pct,
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
                w.week                              AS week_label,
                CAST(TO_CHAR(TO_DATE(w.week || '-1', 'IYYY-"W"IW-ID'), 'YYYYMMDD') AS INTEGER) AS date_key,
                w.deployment_count,
                w.deployment_count / 7.0            AS deploy_freq_per_week,
                w.total_pipelines,
                w.failed_pipelines,
                w.change_failure_rate               AS cfr_pct,
                NULL::DOUBLE PRECISION              AS lead_time_jira_hours,
                NULL::DOUBLE PRECISION              AS lead_time_git_hours,
                NULL::DOUBLE PRECISION              AS lead_time_global_hours,
                NULL::DOUBLE PRECISION              AS mttr_global_hours
            FROM dora_metrics.weekly w
        """)

    # Enrichir avec le lead_time réel depuis features.tickets (par semaine)
    lt_weekly = load_df(engine, """
        SELECT
            TO_CHAR(DATE_TRUNC('week', created), 'YYYY-"W"IW') AS week_label,
            AVG(lead_time_hours)                                 AS lead_time_jira_hours
        FROM features.tickets
        WHERE created IS NOT NULL AND lead_time_hours IS NOT NULL
        GROUP BY DATE_TRUNC('week', created)
    """)
    if not lt_weekly.empty:
        df = df.merge(lt_weekly, on="week_label", how="left", suffixes=("", "_computed"))
        if "lead_time_jira_hours_computed" in df.columns:
            df["lead_time_jira_hours"] = df["lead_time_jira_hours"].combine_first(
                df["lead_time_jira_hours_computed"]
            )
            df = df.drop(columns=["lead_time_jira_hours_computed"])
        df["lead_time_global_hours"] = df["lead_time_jira_hours"]

    # Associe les niveaux DORA
    dora_levels = load_df(engine, "SELECT dora_key, dora_level FROM dwh.dim_dora_level")
    level_map = dict(zip(dora_levels["dora_level"], dora_levels["dora_key"]))

    def deploy_level_key(freq):
        if pd.isna(freq):        return level_map.get("Low")
        if freq >= 1:            return level_map.get("Elite")
        if freq >= 1 / 7:        return level_map.get("High")
        if freq >= 1 / 30:       return level_map.get("Medium")
        return level_map.get("Low")

    def cfr_level_key(pct):
        if pd.isna(pct):  return level_map.get("Low")
        if pct <= 5:      return level_map.get("Elite")
        if pct <= 10:     return level_map.get("High")
        if pct <= 15:     return level_map.get("Medium")
        return level_map.get("Low")

    # Lead Time DORA : < 1h Elite, < 1j High, < 1 semaine Medium, sinon Low
    def lt_level_key(hours):
        if pd.isna(hours):      return level_map.get("Low")
        if hours < 1:           return level_map.get("Elite")
        if hours < 24:          return level_map.get("High")
        if hours < 168:         return level_map.get("Medium")
        return level_map.get("Low")

    df["deploy_freq_key"] = df["deploy_freq_per_week"].apply(deploy_level_key)
    df["cfr_key"]         = df["cfr_pct"].apply(cfr_level_key)
    df["lead_time_key"]   = df["lead_time_global_hours"].apply(lt_level_key)
    df["mttr_key"]        = level_map.get("Elite")
    df["snapshot_date"]   = date.today()

    upsert(engine, df, "dwh.fact_dora_snapshot")


# =============================================================================
# 13. FACT_TEAM_PERFORMANCE  (toutes les colonnes NULL maintenant remplies)
# =============================================================================
def populate_fact_team_performance(engine):
    print("\n[13] fact_team_performance")

    # Métriques agrégées par assignee ET projet depuis features.tickets
    df = load_df(engine, """
        SELECT
            CAST(TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS INTEGER)    AS date_key,
            da.assignee_key,
            dp.project_key,
            -- Volumes
            COUNT(t.ticket_key)                                     AS nb_tickets_assigned,
            SUM(CASE WHEN t.current_status IN
                ('To Be Deployed','Done','Closed','Resolved') THEN 1 ELSE 0 END)
                                                                    AS nb_tickets_done,
            SUM(CASE WHEN t.is_bug = 1 THEN 1 ELSE 0 END)         AS nb_bugs_assigned,
            SUM(CASE WHEN t.is_delayed = 1 THEN 1 ELSE 0 END)     AS nb_delayed,
            SUM(COALESCE(t.nb_commits, 0))                         AS nb_commits_total,
            SUM(COALESCE(t.nb_mrs, 0))                             AS nb_mrs_total,
            -- Timing
            ROUND(AVG(t.lead_time_hours)::NUMERIC, 2)              AS avg_lead_time_hours,
            ROUND(AVG(t.cycle_time_hours)::NUMERIC, 2)             AS avg_cycle_time_hours,
            ROUND(AVG(t.time_blocked_hours)::NUMERIC, 2)           AS avg_time_blocked_hours,
            -- Taux
            ROUND(
                SUM(CASE WHEN t.is_delayed = 0 THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(t.ticket_key), 0), 1
            )                                                       AS on_time_rate,
            ROUND(
                SUM(CASE WHEN t.current_status IN
                    ('To Be Deployed','Done','Closed','Resolved') THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(t.ticket_key), 0), 2
            )                                                       AS completion_rate,
            CURRENT_DATE                                            AS snapshot_date
        FROM features.tickets t
        JOIN dwh.dim_assignee da ON da.assignee_name = t.assignee
        JOIN dwh.dim_project  dp ON dp.project_code  = t.project_key
        WHERE t.assignee IS NOT NULL AND t.project_key IS NOT NULL
        GROUP BY da.assignee_key, dp.project_key
    """)

    if df.empty:
        info("Aucune donnee pour fact_team_performance")
        return

    # Performance score composite : pondération on_time (40%) + completion (30%) + lead_time (30%)
    # Lead time score : normalise sur 2000h max (inverse : moins = mieux)
    df["lead_time_score"] = (
        1 - (df["avg_lead_time_hours"].clip(upper=2000) / 2000)
    ).clip(lower=0) * 100

    df["performance_score"] = (
        df["on_time_rate"].fillna(0)     * 0.40 +
        df["completion_rate"].fillna(0)  * 0.30 +
        df["lead_time_score"].fillna(0)  * 0.30
    ).round(2)
    df = df.drop(columns=["lead_time_score"])

    # Cast colonnes integer nullable
    int_cols = ["nb_bugs_assigned", "nb_delayed", "nb_commits_total",
                "nb_mrs_total", "project_key"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.array(df[c], dtype=pd.Int64Dtype())

    upsert(engine, df, "dwh.fact_team_performance")


# =============================================================================
# 14. FACT_DORA_METRICS_PROJET  (table de cache pour le calcul a la demande)
# =============================================================================
def create_fact_dora_metrics_projet(engine):
    print("\n[14] fact_dora_metrics_projet")
    run_sql(engine, """
        CREATE TABLE IF NOT EXISTS dwh.fact_dora_metrics_projet (
            id                    SERIAL PRIMARY KEY,
            project_key           INTEGER REFERENCES dwh.dim_project(project_key),
            date_debut            DATE NOT NULL,
            date_fin              DATE NOT NULL,
            mttr                  DOUBLE PRECISION,
            change_failure_rate   DOUBLE PRECISION,
            deployment_frequency  DOUBLE PRECISION,
            lead_time_mean        DOUBLE PRECISION,
            risk_score            DOUBLE PRECISION,
            failed_jobs           INTEGER,
            tickets_total         INTEGER,
            tickets_done          INTEGER,
            commits               INTEGER,
            data_source           VARCHAR(20) DEFAULT 'jira_only',
            computed_at           TIMESTAMP DEFAULT NOW(),
            UNIQUE (project_key, date_debut, date_fin)
        )
    """)
    ok("fact_dora_metrics_projet prete")


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

    with engine.connect() as conn:
        schema_exists = conn.execute(text(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'dwh'"
        )).scalar()
    if not schema_exists:
        fail("Schema 'dwh' introuvable -- lance d'abord: psql -f database/schema/02_star_schema_dwh.sql")
        sys.exit(1)

    # Verifier que dim_personnel existe (schema mis a jour)
    with engine.connect() as conn:
        personnel_table_exists = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'dwh' AND table_name = 'dim_personnel'
        """)).scalar()
    if not personnel_table_exists:
        info("dim_personnel absente -- applique: psql -f database/schema/02_star_schema_dwh.sql")

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
        if personnel_table_exists:
            TABLES_ORDER.insert(0, "dwh.dim_personnel")
        with engine.begin() as conn:
            for t in TABLES_ORDER:
                conn.execute(text(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE"))
        ok("Tables DWH videes")

    # -- Alimentation dans l ordre (dimensions en premier)
    populate_dim_date(engine)
    if personnel_table_exists:
        populate_dim_personnel(engine)
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
    create_fact_dora_metrics_projet(engine)

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
    print("  5. Ajouter dim_personnel comme slicer Equipe / Departement")
    print("  6. Les date_key (INTEGER YYYYMMDD) relient toutes les facts a dim_date")
    print()


if __name__ == "__main__":
    main()
