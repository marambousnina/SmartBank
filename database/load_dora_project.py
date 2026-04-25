# -*- coding: utf-8 -*-
"""
Crée et peuple deux tables DORA par projet :
  - dwh.fact_dora_project_monthly  (granularité mensuelle)
  - dwh.fact_dora_project_weekly   (granularité hebdomadaire)

Sources :
  - fact_deployments  : deploy_freq + CFR (via ref LIKE %project_code%)
  - fact_tickets      : lead_time, on_time_rate (via project_key)
  - dim_date          : axe temporel
  - dim_dora_level    : classification Elite/High/Medium/Low
"""

import sys
from pathlib import Path
from datetime import date

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine
from sqlalchemy import text

engine = get_engine()


# ── classification DORA level ────────────────────────────────────────────────

def deploy_freq_key(freq_per_week: float) -> int:
    """Elite≥7/sem, High≥1/sem, Medium≥1/mois(0.25), Low sinon."""
    if freq_per_week >= 7:    return 1   # Elite
    if freq_per_week >= 1:    return 2   # High
    if freq_per_week >= 0.25: return 3   # Medium
    return 4                             # Low


def lead_time_key(lt_hours: float | None) -> int:
    """Elite<24h, High<168h(7j), Medium<720h(30j), Low sinon."""
    if lt_hours is None: return 4
    if lt_hours < 24:    return 1
    if lt_hours < 168:   return 2
    if lt_hours < 720:   return 3
    return 4


def cfr_key(cfr: float | None) -> int:
    """Elite<5%, High<10%, Medium<15%, Low sinon."""
    if cfr is None:  return 1
    if cfr < 5:      return 1
    if cfr < 10:     return 2
    if cfr < 15:     return 3
    return 4


# ── DDL ─────────────────────────────────────────────────────────────────────

DDL_MONTHLY = """
CREATE TABLE IF NOT EXISTS dwh.fact_dora_project_monthly (
    dpm_key              SERIAL PRIMARY KEY,
    project_key          INTEGER NOT NULL,
    date_key             INTEGER NOT NULL,
    month_year           VARCHAR(10) NOT NULL,
    year                 SMALLINT NOT NULL,
    month                SMALLINT NOT NULL,
    -- Déploiements
    deployment_count     INTEGER   DEFAULT 0,
    deploy_freq_per_week FLOAT     DEFAULT 0,
    deploy_freq_key      INTEGER   DEFAULT 4,
    -- Lead time (tickets résolus ce mois)
    avg_lead_time_hours  FLOAT,
    avg_lead_time_days   FLOAT,
    lead_time_key        INTEGER   DEFAULT 4,
    -- CFR
    total_pipelines      INTEGER   DEFAULT 0,
    failed_pipelines     INTEGER   DEFAULT 0,
    cfr_pct              FLOAT     DEFAULT 0,
    cfr_key              INTEGER   DEFAULT 1,
    -- Tickets
    tickets_created      INTEGER   DEFAULT 0,
    tickets_resolved     INTEGER   DEFAULT 0,
    tickets_delayed      INTEGER   DEFAULT 0,
    on_time_rate         FLOAT     DEFAULT 0,
    -- Meta
    snapshot_date        DATE      NOT NULL,
    loaded_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE (project_key, date_key)
);
"""

DDL_WEEKLY = """
CREATE TABLE IF NOT EXISTS dwh.fact_dora_project_weekly (
    dpw_key              SERIAL PRIMARY KEY,
    project_key          INTEGER NOT NULL,
    date_key             INTEGER NOT NULL,
    week_label           VARCHAR(12) NOT NULL,
    year                 SMALLINT NOT NULL,
    week_of_year         SMALLINT NOT NULL,
    -- Déploiements
    deployment_count     INTEGER   DEFAULT 0,
    deploy_freq_per_week FLOAT     DEFAULT 0,
    deploy_freq_key      INTEGER   DEFAULT 4,
    -- Lead time
    avg_lead_time_hours  FLOAT,
    avg_lead_time_days   FLOAT,
    lead_time_key        INTEGER   DEFAULT 4,
    -- CFR
    total_pipelines      INTEGER   DEFAULT 0,
    failed_pipelines     INTEGER   DEFAULT 0,
    cfr_pct              FLOAT     DEFAULT 0,
    cfr_key              INTEGER   DEFAULT 1,
    -- Tickets
    tickets_created      INTEGER   DEFAULT 0,
    tickets_resolved     INTEGER   DEFAULT 0,
    tickets_delayed      INTEGER   DEFAULT 0,
    on_time_rate         FLOAT     DEFAULT 0,
    -- Meta
    snapshot_date        DATE      NOT NULL,
    loaded_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE (project_key, date_key)
);
"""


# ── helpers ──────────────────────────────────────────────────────────────────

def q(conn, sql, params=None):
    result = conn.execute(text(sql), params or {})
    cols = list(result.keys())
    return [dict(zip(cols, row)) for row in result.fetchall()]


def build_monthly_rows(conn, projects, today):
    """Compute monthly DORA for each project."""
    rows = []

    # Time grid: all months in dim_date up to today
    months = q(conn, """
        SELECT DISTINCT date_key, month_year, year, month
        FROM dwh.dim_date
        WHERE full_date <= :today
          AND full_date >= '2024-01-01'
          AND day_of_month = 1
        ORDER BY date_key
    """, {"today": today})

    for proj in projects:
        pk   = proj["project_key"]
        code = proj["project_code"]

        # Deployments by month (via ref)
        dep_by_month = {r["month_year"]: r for r in q(conn, """
            SELECT
                dd.month_year,
                COUNT(*)                                       AS total,
                COUNT(CASE WHEN fd.is_failed THEN 1 END)      AS failed,
                COUNT(CASE WHEN NOT fd.is_failed THEN 1 END)  AS success
            FROM dwh.fact_deployments fd
            JOIN dwh.dim_date dd ON fd.date_key = dd.date_key
            WHERE UPPER(fd.ref) LIKE :pattern
              AND dd.full_date <= :today
            GROUP BY dd.month_year
        """, {"pattern": f"%{code}%", "today": today})}

        # Lead time by resolution month
        lt_by_month = {r["month_year"]: r for r in q(conn, """
            SELECT
                dd.month_year,
                AVG(ft.lead_time_hours)                                    AS avg_lt_h,
                COUNT(ft.ticket_fact_key)                                  AS resolved,
                COUNT(CASE WHEN ft.is_delayed = 0 THEN 1 END) * 100.0
                    / NULLIF(COUNT(ft.ticket_fact_key), 0)                 AS on_time
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_date dd ON ft.date_key_resolved = dd.date_key
            WHERE ft.project_key = :pk
              AND ft.lead_time_is_final = 1
              AND dd.full_date <= :today
            GROUP BY dd.month_year
        """, {"pk": pk, "today": today})}

        # Tickets created by month
        created_by_month = {r["month_year"]: r["cnt"] for r in q(conn, """
            SELECT dd.month_year, COUNT(*) AS cnt
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_date dd ON ft.date_key_created = dd.date_key
            WHERE ft.project_key = :pk AND dd.full_date <= :today
            GROUP BY dd.month_year
        """, {"pk": pk, "today": today})}

        delayed_by_month = {r["month_year"]: r["cnt"] for r in q(conn, """
            SELECT dd.month_year, COUNT(*) AS cnt
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_date dd ON ft.date_key_resolved = dd.date_key
            WHERE ft.project_key = :pk AND ft.is_delayed = 1 AND dd.full_date <= :today
            GROUP BY dd.month_year
        """, {"pk": pk, "today": today})}

        for m in months:
            my = m["month_year"]
            dep   = dep_by_month.get(my, {})
            lt    = lt_by_month.get(my, {})
            dep_count  = int(dep.get("total") or 0)
            fail_count = int(dep.get("failed") or 0)
            total_pip  = dep_count
            freq       = round(dep_count / 4.33, 2)   # approx weeks/month
            cfr        = round(fail_count * 100.0 / dep_count, 1) if dep_count else 0.0
            lt_h       = float(lt.get("avg_lt_h") or 0) or None
            lt_days    = round(lt_h / 24, 1) if lt_h else None
            resolved   = int(lt.get("resolved") or 0)
            on_time    = round(float(lt.get("on_time") or 0), 1)
            created    = int(created_by_month.get(my) or 0)
            delayed    = int(delayed_by_month.get(my) or 0)

            rows.append({
                "project_key": pk, "date_key": m["date_key"],
                "month_year": my, "year": m["year"], "month": m["month"],
                "deployment_count": dep_count, "deploy_freq_per_week": freq,
                "deploy_freq_key": deploy_freq_key(freq),
                "avg_lead_time_hours": lt_h, "avg_lead_time_days": lt_days,
                "lead_time_key": lead_time_key(lt_h),
                "total_pipelines": total_pip, "failed_pipelines": fail_count,
                "cfr_pct": cfr, "cfr_key": cfr_key(cfr),
                "tickets_created": created, "tickets_resolved": resolved,
                "tickets_delayed": delayed, "on_time_rate": on_time,
                "snapshot_date": today,
            })
    return rows


def build_weekly_rows(conn, projects, today):
    """Compute weekly DORA for each project."""
    rows = []

    weeks = q(conn, """
        SELECT DISTINCT date_key, week_label, year, week_of_year
        FROM dwh.dim_date
        WHERE full_date <= :today
          AND full_date >= '2024-01-01'
          AND day_of_week = 1
        ORDER BY date_key
    """, {"today": today})

    for proj in projects:
        pk   = proj["project_key"]
        code = proj["project_code"]

        dep_by_week = {r["week_label"]: r for r in q(conn, """
            SELECT
                dd.week_label,
                COUNT(*)                                      AS total,
                COUNT(CASE WHEN fd.is_failed THEN 1 END)     AS failed
            FROM dwh.fact_deployments fd
            JOIN dwh.dim_date dd ON fd.date_key = dd.date_key
            WHERE UPPER(fd.ref) LIKE :pattern AND dd.full_date <= :today
            GROUP BY dd.week_label
        """, {"pattern": f"%{code}%", "today": today})}

        lt_by_week = {r["week_label"]: r for r in q(conn, """
            SELECT
                dd.week_label,
                AVG(ft.lead_time_hours)                                AS avg_lt_h,
                COUNT(ft.ticket_fact_key)                              AS resolved,
                COUNT(CASE WHEN ft.is_delayed=0 THEN 1 END)*100.0
                    / NULLIF(COUNT(ft.ticket_fact_key), 0)             AS on_time
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_date dd ON ft.date_key_resolved = dd.date_key
            WHERE ft.project_key = :pk AND ft.lead_time_is_final = 1 AND dd.full_date <= :today
            GROUP BY dd.week_label
        """, {"pk": pk, "today": today})}

        created_by_week = {r["week_label"]: r["cnt"] for r in q(conn, """
            SELECT dd.week_label, COUNT(*) AS cnt
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_date dd ON ft.date_key_created = dd.date_key
            WHERE ft.project_key = :pk AND dd.full_date <= :today
            GROUP BY dd.week_label
        """, {"pk": pk, "today": today})}

        delayed_by_week = {r["week_label"]: r["cnt"] for r in q(conn, """
            SELECT dd.week_label, COUNT(*) AS cnt
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_date dd ON ft.date_key_resolved = dd.date_key
            WHERE ft.project_key = :pk AND ft.is_delayed = 1 AND dd.full_date <= :today
            GROUP BY dd.week_label
        """, {"pk": pk, "today": today})}

        for w in weeks:
            wl = w["week_label"]
            dep   = dep_by_week.get(wl, {})
            lt    = lt_by_week.get(wl, {})
            dep_count  = int(dep.get("total") or 0)
            fail_count = int(dep.get("failed") or 0)
            freq       = float(dep_count)   # par définition c'est déjà par semaine
            cfr        = round(fail_count * 100.0 / dep_count, 1) if dep_count else 0.0
            lt_h       = float(lt.get("avg_lt_h") or 0) or None
            lt_days    = round(lt_h / 24, 1) if lt_h else None
            resolved   = int(lt.get("resolved") or 0)
            on_time    = round(float(lt.get("on_time") or 0), 1)
            created    = int(created_by_week.get(wl) or 0)
            delayed    = int(delayed_by_week.get(wl) or 0)

            rows.append({
                "project_key": pk, "date_key": w["date_key"],
                "week_label": wl, "year": w["year"], "week_of_year": w["week_of_year"],
                "deployment_count": dep_count, "deploy_freq_per_week": freq,
                "deploy_freq_key": deploy_freq_key(freq),
                "avg_lead_time_hours": lt_h, "avg_lead_time_days": lt_days,
                "lead_time_key": lead_time_key(lt_h),
                "total_pipelines": dep_count, "failed_pipelines": fail_count,
                "cfr_pct": cfr, "cfr_key": cfr_key(cfr),
                "tickets_created": created, "tickets_resolved": resolved,
                "tickets_delayed": delayed, "on_time_rate": on_time,
                "snapshot_date": today,
            })
    return rows


# ── upsert ───────────────────────────────────────────────────────────────────

def upsert_monthly(conn, rows):
    sql = text("""
        INSERT INTO dwh.fact_dora_project_monthly (
            project_key, date_key, month_year, year, month,
            deployment_count, deploy_freq_per_week, deploy_freq_key,
            avg_lead_time_hours, avg_lead_time_days, lead_time_key,
            total_pipelines, failed_pipelines, cfr_pct, cfr_key,
            tickets_created, tickets_resolved, tickets_delayed, on_time_rate,
            snapshot_date, loaded_at
        ) VALUES (
            :project_key, :date_key, :month_year, :year, :month,
            :deployment_count, :deploy_freq_per_week, :deploy_freq_key,
            :avg_lead_time_hours, :avg_lead_time_days, :lead_time_key,
            :total_pipelines, :failed_pipelines, :cfr_pct, :cfr_key,
            :tickets_created, :tickets_resolved, :tickets_delayed, :on_time_rate,
            :snapshot_date, NOW()
        )
        ON CONFLICT (project_key, date_key) DO UPDATE SET
            deployment_count     = EXCLUDED.deployment_count,
            deploy_freq_per_week = EXCLUDED.deploy_freq_per_week,
            deploy_freq_key      = EXCLUDED.deploy_freq_key,
            avg_lead_time_hours  = EXCLUDED.avg_lead_time_hours,
            avg_lead_time_days   = EXCLUDED.avg_lead_time_days,
            lead_time_key        = EXCLUDED.lead_time_key,
            total_pipelines      = EXCLUDED.total_pipelines,
            failed_pipelines     = EXCLUDED.failed_pipelines,
            cfr_pct              = EXCLUDED.cfr_pct,
            cfr_key              = EXCLUDED.cfr_key,
            tickets_created      = EXCLUDED.tickets_created,
            tickets_resolved     = EXCLUDED.tickets_resolved,
            tickets_delayed      = EXCLUDED.tickets_delayed,
            on_time_rate         = EXCLUDED.on_time_rate,
            snapshot_date        = EXCLUDED.snapshot_date,
            loaded_at            = NOW()
    """)
    conn.execute(sql, rows)


def upsert_weekly(conn, rows):
    sql = text("""
        INSERT INTO dwh.fact_dora_project_weekly (
            project_key, date_key, week_label, year, week_of_year,
            deployment_count, deploy_freq_per_week, deploy_freq_key,
            avg_lead_time_hours, avg_lead_time_days, lead_time_key,
            total_pipelines, failed_pipelines, cfr_pct, cfr_key,
            tickets_created, tickets_resolved, tickets_delayed, on_time_rate,
            snapshot_date, loaded_at
        ) VALUES (
            :project_key, :date_key, :week_label, :year, :week_of_year,
            :deployment_count, :deploy_freq_per_week, :deploy_freq_key,
            :avg_lead_time_hours, :avg_lead_time_days, :lead_time_key,
            :total_pipelines, :failed_pipelines, :cfr_pct, :cfr_key,
            :tickets_created, :tickets_resolved, :tickets_delayed, :on_time_rate,
            :snapshot_date, NOW()
        )
        ON CONFLICT (project_key, date_key) DO UPDATE SET
            deployment_count     = EXCLUDED.deployment_count,
            deploy_freq_per_week = EXCLUDED.deploy_freq_per_week,
            deploy_freq_key      = EXCLUDED.deploy_freq_key,
            avg_lead_time_hours  = EXCLUDED.avg_lead_time_hours,
            avg_lead_time_days   = EXCLUDED.avg_lead_time_days,
            lead_time_key        = EXCLUDED.lead_time_key,
            total_pipelines      = EXCLUDED.total_pipelines,
            failed_pipelines     = EXCLUDED.failed_pipelines,
            cfr_pct              = EXCLUDED.cfr_pct,
            cfr_key              = EXCLUDED.cfr_key,
            tickets_created      = EXCLUDED.tickets_created,
            tickets_resolved     = EXCLUDED.tickets_resolved,
            tickets_delayed      = EXCLUDED.tickets_delayed,
            on_time_rate         = EXCLUDED.on_time_rate,
            snapshot_date        = EXCLUDED.snapshot_date,
            loaded_at            = NOW()
    """)
    conn.execute(sql, rows)


# ── main ─────────────────────────────────────────────────────────────────────

def run():
    today = date.today()
    print(f"[DORA Project] Chargement au {today}")

    with engine.begin() as conn:
        # DDL
        conn.execute(text(DDL_MONTHLY))
        conn.execute(text(DDL_WEEKLY))
        print("  Tables créées / vérifiées")

        # Projets
        projects = q(conn, "SELECT project_key, project_code FROM dwh.dim_project ORDER BY project_key")
        print(f"  {len(projects)} projets trouvés")

        # Monthly
        monthly_rows = build_monthly_rows(conn, projects, today)
        non_empty_monthly = [r for r in monthly_rows if r["deployment_count"] > 0 or r["tickets_created"] > 0 or r["tickets_resolved"] > 0]
        print(f"  Monthly : {len(monthly_rows)} combinaisons projet×mois ({len(non_empty_monthly)} non vides)")
        if monthly_rows:
            upsert_monthly(conn, monthly_rows)

        # Weekly
        weekly_rows = build_weekly_rows(conn, projects, today)
        non_empty_weekly = [r for r in weekly_rows if r["deployment_count"] > 0 or r["tickets_created"] > 0 or r["tickets_resolved"] > 0]
        print(f"  Weekly  : {len(weekly_rows)} combinaisons projet×semaine ({len(non_empty_weekly)} non vides)")
        if weekly_rows:
            upsert_weekly(conn, weekly_rows)

    print("[DORA Project] Chargement terminé")

    # Verification
    with engine.connect() as conn:
        nm = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_dora_project_monthly")).scalar()
        nw = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_dora_project_weekly")).scalar()
        print(f"  fact_dora_project_monthly : {nm} lignes")
        print(f"  fact_dora_project_weekly  : {nw} lignes")


if __name__ == "__main__":
    run()
