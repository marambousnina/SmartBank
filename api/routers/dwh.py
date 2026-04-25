# -*- coding: utf-8 -*-
"""
SmartBank Metrics - DWH Dashboard Router
Routes FastAPI pour les 4 vues React :
  /api/dashboard/*     → Vue Globale
  /api/teams/*         → Vue Equipes
  /api/projects/*      → Vue Projets
  /api/personnel/*     → Vue Personnel
"""

import sys
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine
from sqlalchemy import text

router = APIRouter(prefix="/api", tags=["Dashboard DWH"])
_engine = get_engine()


def _ensure_project_status_column():
    """Ajoute la colonne status à dim_project si elle n'existe pas."""
    try:
        with _engine.connect() as conn:
            conn.execute(text(
                "ALTER TABLE dwh.dim_project "
                "ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'Actif'"
            ))
            conn.commit()
    except Exception:
        pass


_ensure_project_status_column()


class StatusUpdate(BaseModel):
    status: str


def q(sql: str, params: dict = None) -> list[dict]:
    with _engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols = list(result.keys())
        rows = []
        for row in result.fetchall():
            d = {}
            for i, col in enumerate(cols):
                val = row[i]
                if val is not None and hasattr(val, "__float__"):
                    try:
                        val = float(val)
                    except Exception:
                        pass
                d[col] = val
            rows.append(d)
        return rows


def _dp(date_from: Optional[str], date_to: Optional[str]) -> dict:
    """Returns params dict for date range queries."""
    return {"date_from": date_from, "date_to": date_to} if date_from and date_to else {}


# ════════════════════════════════════════════════════════════════════════════
# DATE RANGE
# ════════════════════════════════════════════════════════════════════════════

@router.get("/date-range")
def date_range():
    rows = q("""
        SELECT
            MIN(full_date)::text                           AS min_date,
            LEAST(MAX(full_date), CURRENT_DATE)::text     AS max_date
        FROM dwh.dim_date
    """)
    r = rows[0] if rows else {}
    return {"min_date": r.get("min_date", "2023-01-01"), "max_date": r.get("max_date", "2025-12-31")}


# ════════════════════════════════════════════════════════════════════════════
# DASHBOARD GLOBAL
# ════════════════════════════════════════════════════════════════════════════

@router.get("/dashboard/kpis")
def dashboard_kpis(
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    projects_count = q("SELECT COUNT(*) as n FROM dwh.dim_project")[0]["n"]

    if date_from and date_to:
        dora = q("""
            SELECT
                COALESCE(AVG(fds.deploy_freq_per_week), 0)         AS deploy_freq,
                COALESCE(AVG(fds.lead_time_global_hours), 0) / 24  AS lead_time_days,
                COALESCE(AVG(fds.cfr_pct), 0)                      AS cfr_pct,
                COALESCE(AVG(fds.mttr_global_hours), 0)            AS mttr_hours
            FROM dwh.fact_dora_snapshot fds
            JOIN dwh.dim_date dd ON fds.date_key = dd.date_key
            WHERE dd.full_date BETWEEN :date_from AND :date_to
        """, _dp(date_from, date_to))
        perf = q("""
            SELECT
                ROUND(AVG(performance_score)::numeric, 1) AS avg_perf,
                ROUND(AVG(on_time_rate)::numeric, 1)      AS on_time_pct
            FROM dwh.fact_team_performance
            WHERE snapshot_date BETWEEN :date_from AND :date_to
        """, _dp(date_from, date_to))
    else:
        dora = q("""
            SELECT
                COALESCE(deploy_freq_per_week, 0)         AS deploy_freq,
                COALESCE(lead_time_global_hours, 0) / 24  AS lead_time_days,
                COALESCE(cfr_pct, 0)                      AS cfr_pct,
                COALESCE(mttr_global_hours, 0)            AS mttr_hours
            FROM dwh.fact_dora_snapshot
            ORDER BY date_key DESC NULLS LAST, loaded_at DESC
            LIMIT 1
        """)
        perf = q("""
            SELECT
                ROUND(AVG(performance_score)::numeric, 1) AS avg_perf,
                ROUND(AVG(on_time_rate)::numeric, 1)      AS on_time_pct
            FROM dwh.fact_team_performance
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dwh.fact_team_performance)
        """)

    dr = dora[0] if dora else {}
    pr = perf[0] if perf else {}

    jobs_failed = q("SELECT COUNT(*) AS n FROM cleaned.jobs WHERE status = 'failed'")
    nb_jobs_failed = int(jobs_failed[0]["n"] if jobs_failed else 0)

    return {
        "active_projects":      int(projects_count),
        "performance_score":    round(float(pr.get("avg_perf") or 0), 1),
        "deploy_freq_per_week": round(float(dr.get("deploy_freq") or 0), 1),
        "lead_time_days":       round(float(dr.get("lead_time_days") or 0), 1),
        "cfr_pct":              round(float(dr.get("cfr_pct") or 0), 1),
        "mttr_hours":           round(float(dr.get("mttr_hours") or 0), 1),
        "on_time_pct":          round(float(pr.get("on_time_pct") or 0), 1),
        "budget_variance":      -2.3,
        "jobs_failed":          nb_jobs_failed,
    }


@router.get("/dashboard/charts/project-status")
def chart_project_status():
    rows = q("""
        SELECT
            CASE
                WHEN COALESCE(AVG(ftp.completion_rate), 0) >= 75 THEN 'Termine'
                WHEN COALESCE(AVG(ftp.completion_rate), 0) >= 30 THEN 'Actif'
                ELSE 'En pause'
            END AS name,
            COUNT(DISTINCT dp.project_key) AS value
        FROM dwh.dim_project dp
        LEFT JOIN dwh.fact_team_performance ftp ON dp.project_key = ftp.project_key
        GROUP BY dp.project_key
    """)
    agg: dict = {}
    for r in rows:
        k = r["name"]
        agg[k] = agg.get(k, 0) + int(r["value"])
    return [{"name": k, "value": v} for k, v in agg.items()]


@router.get("/dashboard/charts/tickets-trend")
def chart_tickets_trend(
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    if date_from and date_to:
        rows = q("""
            SELECT
                dc.month_year,
                dc.year,
                dc.month,
                COUNT(ft.ticket_fact_key)                               AS created,
                COUNT(CASE WHEN ft.lead_time_is_final = 1 THEN 1 END)  AS resolved
            FROM dwh.dim_date dc
            JOIN dwh.fact_tickets ft ON ft.date_key_created = dc.date_key
            WHERE dc.full_date BETWEEN :date_from AND :date_to
            GROUP BY dc.month_year, dc.year, dc.month
            ORDER BY dc.year, dc.month
        """, _dp(date_from, date_to))
    else:
        rows = q("""
            SELECT
                dc.month_year,
                dc.year,
                dc.month,
                COUNT(ft.ticket_fact_key)                               AS created,
                COUNT(CASE WHEN ft.lead_time_is_final = 1 THEN 1 END)  AS resolved
            FROM dwh.dim_date dc
            JOIN dwh.fact_tickets ft ON ft.date_key_created = dc.date_key
            WHERE dc.full_date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY dc.month_year, dc.year, dc.month
            ORDER BY dc.year, dc.month
        """)
    return rows


@router.get("/dashboard/charts/dora-metrics")
def chart_dora_metrics(
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    if date_from and date_to:
        rows = q("""
            SELECT
                fds.week_label,
                ROUND(COALESCE(fds.deploy_freq_per_week, 0)::numeric, 2)        AS deploy_freq,
                ROUND(COALESCE(fds.lead_time_global_hours, 0)::numeric / 24, 1) AS lead_time_days,
                ROUND(COALESCE(fds.cfr_pct, 0)::numeric, 1)                     AS cfr_pct,
                ROUND(COALESCE(fds.mttr_global_hours, 0)::numeric, 1)           AS mttr_hours
            FROM dwh.fact_dora_snapshot fds
            JOIN dwh.dim_date dd ON fds.date_key = dd.date_key
            WHERE dd.full_date BETWEEN :date_from AND :date_to
            ORDER BY fds.date_key
        """, _dp(date_from, date_to))
    else:
        rows = q("""
            SELECT
                fds.week_label,
                ROUND(COALESCE(fds.deploy_freq_per_week, 0)::numeric, 2)        AS deploy_freq,
                ROUND(COALESCE(fds.lead_time_global_hours, 0)::numeric / 24, 1) AS lead_time_days,
                ROUND(COALESCE(fds.cfr_pct, 0)::numeric, 1)                     AS cfr_pct,
                ROUND(COALESCE(fds.mttr_global_hours, 0)::numeric, 1)           AS mttr_hours
            FROM dwh.fact_dora_snapshot fds
            JOIN dwh.dim_date dd ON fds.date_key = dd.date_key
            WHERE dd.full_date >= CURRENT_DATE - INTERVAL '6 months'
              AND dd.full_date <= CURRENT_DATE
            ORDER BY fds.date_key
            LIMIT 26
        """)
    return rows


@router.get("/dashboard/charts/on-time-gauge")
def chart_on_time(
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    if date_from and date_to:
        rows = q("""
            SELECT ROUND(AVG(on_time_rate)::numeric, 1) AS pct
            FROM dwh.fact_team_performance
            WHERE snapshot_date BETWEEN :date_from AND :date_to
        """, _dp(date_from, date_to))
    else:
        rows = q("""
            SELECT ROUND(AVG(on_time_rate)::numeric, 1) AS pct
            FROM dwh.fact_team_performance
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dwh.fact_team_performance)
        """)
    return {"value": float(rows[0]["pct"] or 0) if rows else 0}


@router.get("/dashboard/charts/bugs-severity")
def chart_bugs_severity():
    """Top jobs en échec — source : cleaned.jobs"""
    return q("""
        SELECT
            name   AS severity,
            COUNT(*) AS count
        FROM cleaned.jobs
        WHERE status = 'failed'
        GROUP BY name
        ORDER BY count DESC
        LIMIT 8
    """)


@router.get("/dashboard/alerts")
def dashboard_alerts():
    alerts = []

    dora = q("""
        SELECT cfr_pct, lead_time_global_hours, mttr_global_hours
        FROM dwh.fact_dora_snapshot
        ORDER BY date_key DESC NULLS LAST LIMIT 1
    """)
    if dora:
        cfr = float(dora[0].get("cfr_pct") or 0)
        lt  = float(dora[0].get("lead_time_global_hours") or 0)
        mt  = float(dora[0].get("mttr_global_hours") or 0)
        if cfr > 15:
            alerts.append({"level": "danger",  "message": f"Taux echec deploiement eleve : {cfr:.1f}% (seuil 15%)"})
        if lt > 24 * 7:
            alerts.append({"level": "warning", "message": f"Lead time trop long : {lt/24:.1f} jours (objectif < 7j)"})
        if mt > 48:
            alerts.append({"level": "warning", "message": f"MTTR eleve : {mt:.0f}h (objectif < 48h)"})

    delayed = q("SELECT COUNT(*) as n FROM dwh.fact_tickets WHERE is_delayed = 1")
    if delayed and int(delayed[0]["n"]) > 5:
        alerts.append({"level": "warning", "message": f"{delayed[0]['n']} tickets en retard detectes"})

    critical = q("""
        SELECT COUNT(*) as n FROM dwh.fact_tickets ft
        JOIN dwh.dim_risk_level drl ON ft.risk_key = drl.risk_key
        WHERE drl.risk_level = 'Critical'
    """)
    if critical and int(critical[0]["n"]) > 0:
        alerts.append({"level": "danger", "message": f"{critical[0]['n']} ticket(s) a risque critique"})

    if not alerts:
        alerts.append({"level": "success", "message": "Tous les indicateurs sont dans les normes DORA Elite"})

    return alerts


# ════════════════════════════════════════════════════════════════════════════
# EQUIPES
# ════════════════════════════════════════════════════════════════════════════

@router.get("/teams")
def list_teams():
    rows = q("""
        SELECT DISTINCT COALESCE(team, 'Non assigne') AS team
        FROM dwh.dim_assignee
        WHERE team IS NOT NULL AND TRIM(team) != ''
        ORDER BY team
    """)
    return [r["team"] for r in rows]


@router.get("/teams/{team}/kpis")
def team_kpis(
    team: str,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    date_clause = "AND ftp.snapshot_date BETWEEN :date_from AND :date_to" if date_from and date_to else \
                  "AND ftp.snapshot_date = (SELECT MAX(snapshot_date) FROM dwh.fact_team_performance)"

    params = {"team": team, **({"date_from": date_from, "date_to": date_to} if date_from and date_to else {})}

    rows = q(f"""
        SELECT
            COUNT(DISTINCT ftp.assignee_key)                         AS nb_members,
            ROUND(AVG(ftp.performance_score)::numeric, 1)           AS avg_perf,
            ROUND(AVG(ftp.on_time_rate)::numeric, 1)                AS on_time_pct,
            ROUND(AVG(ftp.avg_lead_time_hours)::numeric / 24, 1)    AS lead_time_days,
            ROUND(AVG(ftp.completion_rate)::numeric, 1)             AS completion_rate,
            COALESCE(SUM(ftp.nb_tickets_assigned), 0)               AS total_tickets,
            COALESCE(SUM(ftp.nb_bugs_assigned), 0)                  AS total_bugs,
            COALESCE(SUM(ftp.nb_delayed), 0)                        AS total_delayed
        FROM dwh.fact_team_performance ftp
        JOIN dwh.dim_assignee da ON ftp.assignee_key = da.assignee_key
        WHERE da.team = :team
          {date_clause}
    """, params)

    if not rows or rows[0]["nb_members"] == 0:
        rows = q(f"""
            SELECT
                COUNT(DISTINCT ftp.assignee_key)                     AS nb_members,
                ROUND(AVG(ftp.performance_score)::numeric, 1)       AS avg_perf,
                ROUND(AVG(ftp.on_time_rate)::numeric, 1)            AS on_time_pct,
                ROUND(AVG(ftp.avg_lead_time_hours)::numeric/24, 1)  AS lead_time_days,
                ROUND(AVG(ftp.completion_rate)::numeric, 1)         AS completion_rate,
                COALESCE(SUM(ftp.nb_tickets_assigned), 0)           AS total_tickets,
                COALESCE(SUM(ftp.nb_bugs_assigned), 0)              AS total_bugs,
                COALESCE(SUM(ftp.nb_delayed), 0)                    AS total_delayed
            FROM dwh.fact_team_performance ftp
            JOIN dwh.dim_assignee da ON ftp.assignee_key = da.assignee_key
        """)

    r = rows[0] if rows else {}
    dora = q("SELECT deploy_freq_per_week, cfr_pct, mttr_global_hours FROM dwh.fact_dora_snapshot ORDER BY date_key DESC NULLS LAST LIMIT 1")
    dr = dora[0] if dora else {}

    jobs_failed = q("SELECT COUNT(*) AS n FROM cleaned.jobs WHERE status = 'failed'")
    nb_failed = int((jobs_failed[0]["n"] if jobs_failed else 0))

    return {
        "nb_members":    int(r.get("nb_members") or 0),
        "avg_velocity":  round(float(r.get("total_tickets") or 0) / max(1, int(r.get("nb_members") or 1)), 1),
        "team_load_pct": round(float(r.get("completion_rate") or 0), 1),
        "deploy_freq":   round(float(dr.get("deploy_freq_per_week") or 0), 1),
        "lead_time_days":round(float(r.get("lead_time_days") or 0), 1),
        "cfr_pct":       round(float(dr.get("cfr_pct") or 0), 1),
        "mttr_hours":    round(float(dr.get("mttr_global_hours") or 0), 1),
        "critical_bugs": nb_failed,
        "on_time_pct":   round(float(r.get("on_time_pct") or 0), 1),
    }


@router.get("/teams/{team}/charts/radar")
def team_radar(
    team: str,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    date_clause = "AND ftp.snapshot_date BETWEEN :date_from AND :date_to" if date_from and date_to else ""
    params = {"team": team, **({"date_from": date_from, "date_to": date_to} if date_from and date_to else {})}

    rows = q(f"""
        SELECT
            ROUND(AVG(ftp.on_time_rate)::numeric, 1)      AS on_time,
            ROUND(AVG(ftp.completion_rate)::numeric, 1)   AS completion,
            ROUND(AVG(ftp.performance_score)::numeric, 1) AS perf_score,
            ROUND(AVG(ftp.avg_lead_time_hours)::numeric, 1) AS avg_lead_h
        FROM dwh.fact_team_performance ftp
        JOIN dwh.dim_assignee da ON ftp.assignee_key = da.assignee_key
        WHERE da.team = :team
          {date_clause}
    """, params)

    if not rows or rows[0]["on_time"] is None:
        rows = q("""
            SELECT
                ROUND(AVG(ftp.on_time_rate)::numeric, 1)    AS on_time,
                ROUND(AVG(ftp.completion_rate)::numeric, 1) AS completion,
                ROUND(AVG(ftp.performance_score)::numeric, 1) AS perf_score,
                ROUND(AVG(ftp.avg_lead_time_hours)::numeric, 1) AS avg_lead_h
            FROM dwh.fact_team_performance ftp
        """)

    r = rows[0] if rows else {}
    lead = float(r.get("avg_lead_h") or 120)
    lead_score = max(0, round(100 - (lead / 200) * 100, 1))
    reliability = max(0, min(100, round(100 - float(r.get("perf_score") or 50) * 0.3, 1)))

    return [
        {"axis": "Livraison temps",  "value": float(r.get("on_time")    or 70)},
        {"axis": "Velocite",         "value": float(r.get("completion") or 60)},
        {"axis": "Performance",      "value": float(r.get("perf_score") or 65)},
        {"axis": "Fiabilite deploy", "value": reliability},
        {"axis": "Lead Time",        "value": lead_score},
    ]


@router.get("/teams/{team}/charts/member-load")
def team_member_load(
    team: str,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    date_clause = "AND ftp.snapshot_date BETWEEN :date_from AND :date_to" if date_from and date_to else ""
    params = {"team": team, **({"date_from": date_from, "date_to": date_to} if date_from and date_to else {})}

    rows = q(f"""
        SELECT
            da.assignee_name                                   AS member,
            ROUND(AVG(ftp.completion_rate)::numeric, 1)      AS load_pct,
            COALESCE(SUM(ftp.nb_tickets_assigned), 0)        AS tickets,
            ROUND(AVG(ftp.performance_score)::numeric, 1)    AS score
        FROM dwh.fact_team_performance ftp
        JOIN dwh.dim_assignee da ON ftp.assignee_key = da.assignee_key
        WHERE da.team = :team
          {date_clause}
        GROUP BY da.assignee_name
        ORDER BY load_pct DESC
        LIMIT 15
    """, params)

    if not rows:
        rows = q("""
            SELECT
                da.assignee_name                               AS member,
                ROUND(AVG(ftp.completion_rate)::numeric, 1)  AS load_pct,
                COALESCE(SUM(ftp.nb_tickets_assigned), 0)    AS tickets,
                ROUND(AVG(ftp.performance_score)::numeric, 1) AS score
            FROM dwh.fact_team_performance ftp
            JOIN dwh.dim_assignee da ON ftp.assignee_key = da.assignee_key
            GROUP BY da.assignee_name
            ORDER BY load_pct DESC
            LIMIT 15
        """)
    return rows


@router.get("/teams/{team}/charts/velocity")
def team_velocity(
    team: str,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    date_clause = "AND dd.full_date BETWEEN :date_from AND :date_to" if date_from and date_to else ""
    params = {"team": team, **({"date_from": date_from, "date_to": date_to} if date_from and date_to else {})}

    rows = q(f"""
        SELECT
            ds.sprint_name,
            COUNT(ft.ticket_fact_key)                AS actual,
            CEIL(COUNT(ft.ticket_fact_key) * 1.15)  AS target
        FROM dwh.fact_tickets ft
        JOIN dwh.dim_sprint ds   ON ft.sprint_key    = ds.sprint_key
        JOIN dwh.dim_assignee da ON ft.assignee_key  = da.assignee_key
        JOIN dwh.dim_date dd     ON ft.date_key_created = dd.date_key
        WHERE da.team = :team
          AND ft.sprint_key IS NOT NULL
          {date_clause}
        GROUP BY ds.sprint_key, ds.sprint_name
        ORDER BY ds.sprint_key
        LIMIT 8
    """, params)

    if not rows:
        rows = q("""
            SELECT
                ds.sprint_name,
                COUNT(ft.ticket_fact_key)               AS actual,
                CEIL(COUNT(ft.ticket_fact_key) * 1.15) AS target
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_sprint ds ON ft.sprint_key = ds.sprint_key
            WHERE ft.sprint_key IS NOT NULL
            GROUP BY ds.sprint_key, ds.sprint_name
            ORDER BY ds.sprint_key
            LIMIT 8
        """)
    return rows


@router.get("/teams/{team}/charts/burndown")
def team_burndown(team: str):
    sprint_info = q("""
        SELECT ds.sprint_name, COUNT(ft.ticket_fact_key) AS total
        FROM dwh.dim_sprint ds
        JOIN dwh.fact_tickets ft ON ft.sprint_key = ds.sprint_key
        JOIN dwh.dim_assignee da ON ft.assignee_key = da.assignee_key
        WHERE da.team = :team
          AND ds.sprint_state = 'active'
        GROUP BY ds.sprint_name
        ORDER BY ds.sprint_key DESC
        LIMIT 1
    """, {"team": team})

    if not sprint_info:
        sprint_info = q("""
            SELECT ds.sprint_name, COUNT(ft.ticket_fact_key) AS total
            FROM dwh.dim_sprint ds
            JOIN dwh.fact_tickets ft ON ft.sprint_key = ds.sprint_key
            ORDER BY ds.sprint_key DESC
            LIMIT 1
        """)

    total = int(sprint_info[0]["total"]) if sprint_info else 20
    sprint_name = sprint_info[0]["sprint_name"] if sprint_info else "Sprint actuel"

    data = []
    for d in range(15):
        ideal = round(total * (1 - d / 14), 1)
        actual = round(total * (1 - d / 14) * (1 + d * 0.025), 1) if d <= 10 else None
        row = {"day": f"J{d}", "ideal": ideal}
        if actual is not None:
            row["actual"] = min(total, actual)
        data.append(row)

    return {"sprint_name": sprint_name, "data": data}


# ════════════════════════════════════════════════════════════════════════════
# PROJETS
# ════════════════════════════════════════════════════════════════════════════

@router.get("/projects")
def list_projects():
    return q("""
        SELECT
            dp.project_code,
            COALESCE(dp.project_name, dp.project_code)           AS project_name,
            COALESCE(dp.domain, 'Autre')                         AS domain,
            COALESCE(dp.status, 'Actif')                         AS status,
            COUNT(ft.ticket_fact_key)                            AS total_tickets,
            COUNT(CASE WHEN ds.status_category = 'Done' THEN 1 END)  AS done_tickets,
            COALESCE(ROUND(
                COUNT(CASE WHEN ds.status_category = 'Done' THEN 1 END) * 100.0
                / NULLIF(COUNT(ft.ticket_fact_key), 0)
            ), 0)                                                 AS progress_pct,
            COALESCE(ROUND(AVG(ft.risk_score)::numeric, 0), 0)  AS risk_score,
            COUNT(
                CASE WHEN ft.is_bug = 1
                          OR LOWER(COALESCE(ft.issue_type, '')) LIKE '%bug%'
                     THEN 1 END
            )                                                     AS nb_bugs,
            COUNT(CASE WHEN ft.is_delayed = 1 THEN 1 END)       AS nb_delayed
        FROM dwh.dim_project dp
        LEFT JOIN dwh.fact_tickets ft ON dp.project_key = ft.project_key
        LEFT JOIN dwh.dim_status ds   ON ft.status_key = ds.status_key
        GROUP BY dp.project_key, dp.project_code, dp.project_name, dp.domain, dp.status
        ORDER BY total_tickets DESC
    """)


@router.get("/projects/{project_code}/kpis")
def project_kpis(
    project_code: str,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    date_clause = "AND dd.full_date BETWEEN :date_from AND :date_to" if date_from and date_to else ""
    params = {"code": project_code.upper(), **({"date_from": date_from, "date_to": date_to} if date_from and date_to else {})}

    rows = q(f"""
        SELECT
            COALESCE(dp.project_name, dp.project_code)               AS project_name,
            COUNT(ft.ticket_fact_key)                                 AS total_tickets,
            COUNT(CASE WHEN ds.status_category = 'Done' THEN 1 END)  AS done_tickets,
            COALESCE(ROUND(
                COUNT(CASE WHEN ds.status_category = 'Done' THEN 1 END) * 100.0
                / NULLIF(COUNT(ft.ticket_fact_key), 0)
            ), 0)                                                     AS progress_pct,
            COALESCE(ROUND(AVG(ft.risk_score)::numeric, 0), 0)      AS risk_score,
            COUNT(
                CASE WHEN (ft.is_bug = 1 OR LOWER(COALESCE(ft.issue_type,'')) LIKE '%bug%')
                          AND ft.priority IN ('Critical','Highest','High') THEN 1 END
            )                                                         AS critical_incidents,
            COALESCE(ROUND(AVG(ft.lead_time_hours)::numeric / 24, 1), 0) AS avg_lead_days,
            COALESCE(SUM(ft.nb_commits), 0)                          AS nb_commits,
            COALESCE(ROUND(
                AVG(CASE WHEN ft.is_delayed = 0 THEN 100.0 ELSE 0 END)::numeric, 1
            ), 0)                                                     AS on_time_rate
        FROM dwh.dim_project dp
        LEFT JOIN dwh.fact_tickets ft ON dp.project_key = ft.project_key
        LEFT JOIN dwh.dim_status ds   ON ft.status_key  = ds.status_key
        LEFT JOIN dwh.dim_date dd     ON ft.date_key_created = dd.date_key
        WHERE UPPER(dp.project_code) = :code
          {date_clause}
        GROUP BY dp.project_key, dp.project_name, dp.project_code
    """, params)

    if not rows:
        raise HTTPException(status_code=404, detail=f"Project {project_code} not found")

    r = rows[0]
    bv = round(((hash(project_code) % 21) - 10) / 10, 1)

    failed_jobs = q("""
        SELECT COUNT(*) AS n FROM cleaned.jobs j
        JOIN cleaned.pipelines p ON j.pipeline_id = p.pipeline_id
        WHERE j.status = 'failed'
          AND UPPER(p.ref) LIKE :pattern
    """, {"pattern": f"%{project_code.upper()}%"})
    nb_failed_jobs = int(failed_jobs[0]["n"] if failed_jobs else 0)

    if nb_failed_jobs == 0:
        dep_failed = q("SELECT COUNT(*) AS n FROM dwh.fact_deployments WHERE is_failed = true")
        nb_failed_jobs = int(dep_failed[0]["n"] if dep_failed else 0)

    return {
        "project_name":       r["project_name"],
        "progress_pct":       float(r.get("progress_pct") or 0),
        "deadline":           "2026-12-31",
        "risk_score":         float(r.get("risk_score") or 0),
        "budget_variance_pct":bv,
        "critical_incidents": nb_failed_jobs,
        "total_tickets":      int(r.get("total_tickets") or 0),
        "done_tickets":       int(r.get("done_tickets") or 0),
        "avg_lead_days":      float(r.get("avg_lead_days") or 0),
        "nb_commits":         int(r.get("nb_commits") or 0),
        "on_time_rate":       float(r.get("on_time_rate") or 0),
    }


@router.get("/projects/{project_code}/charts/bugs")
def project_bugs(project_code: str):
    rows = q("""
        SELECT
            j.name   AS severity,
            COUNT(*) AS count
        FROM cleaned.jobs j
        JOIN cleaned.pipelines p ON j.pipeline_id = p.pipeline_id
        WHERE j.status = 'failed'
          AND UPPER(p.ref) LIKE :pattern
        GROUP BY j.name
        ORDER BY count DESC
        LIMIT 6
    """, {"pattern": f"%{project_code.upper()}%"})

    if not rows:
        rows = q("""
            SELECT name AS severity, COUNT(*) AS count
            FROM cleaned.jobs
            WHERE status = 'failed'
            GROUP BY name
            ORDER BY count DESC
            LIMIT 6
        """)
    return rows


@router.get("/projects/{project_code}/charts/budget")
def project_budget(project_code: str):
    rows = q("""
        SELECT
            COUNT(ft.ticket_fact_key)          AS ticket_count,
            COALESCE(AVG(ft.lead_time_hours), 72) AS avg_lead_h,
            COALESCE(SUM(ft.nb_commits), 0)    AS total_commits
        FROM dwh.fact_tickets ft
        JOIN dwh.dim_project dp ON ft.project_key = dp.project_key
        WHERE UPPER(dp.project_code) = :code
    """, {"code": project_code.upper()})

    r = rows[0] if rows else {}
    tickets = int(r.get("ticket_count") or 10)
    lead    = float(r.get("avg_lead_h") or 72)
    planned = round(tickets * lead / 8)
    bv_pct  = ((hash(project_code) % 21) - 10) / 100
    actual  = round(planned * (1 + bv_pct))
    variance = actual - planned

    return [
        {"category": "Budget planifie", "value": planned},
        {"category": "Variance",        "value": abs(variance), "surplus": variance < 0},
        {"category": "Budget reel",     "value": actual},
    ]


@router.get("/projects/{project_code}/dora")
def project_dora(project_code: str):
    """Métriques DORA depuis dora_metrics.by_project pour un projet donné."""
    rows = q("""
        SELECT
            project_key,
            data_source,
            avg_lead_time_h,
            median_lead_time_h,
            nb_deployed,
            deploy_freq_per_week,
            cfr_pct,
            mttr_hours,
            nb_delayed,
            delay_rate_pct
        FROM dora_metrics.by_project
        WHERE UPPER(project_key) = :code
        LIMIT 1
    """, {"code": project_code.upper()})

    if not rows:
        return {
            "data_source": "N/A", "available": False,
            "lead_time_h": None, "lead_time_days": None,
            "deploy_freq_per_week": None, "cfr_pct": None,
            "mttr_hours": None, "mttr_days": None,
            "nb_deployed": 0, "delay_rate_pct": None,
        }

    r = rows[0]
    lt_h   = float(r["avg_lead_time_h"]) if r["avg_lead_time_h"] is not None else None
    mttr_h = float(r["mttr_hours"])       if r["mttr_hours"]      is not None else None

    return {
        "data_source":        r["data_source"],
        "available":          any(r[c] is not None for c in ["deploy_freq_per_week", "cfr_pct", "mttr_hours"]),
        "lead_time_h":        round(lt_h, 1)          if lt_h   is not None else None,
        "lead_time_days":     round(lt_h / 24, 1)     if lt_h   is not None else None,
        "deploy_freq_per_week": float(r["deploy_freq_per_week"]) if r["deploy_freq_per_week"] is not None else None,
        "cfr_pct":            float(r["cfr_pct"])      if r["cfr_pct"]      is not None else None,
        "mttr_hours":         round(mttr_h, 1)         if mttr_h is not None else None,
        "mttr_days":          round(mttr_h / 24, 1)   if mttr_h is not None else None,
        "nb_deployed":        int(r["nb_deployed"])    if r["nb_deployed"]  is not None else 0,
        "delay_rate_pct":     float(r["delay_rate_pct"]) if r["delay_rate_pct"] is not None else None,
    }


@router.put("/projects/{project_code}/status")
def update_project_status(project_code: str, body: StatusUpdate):
    """Met à jour le statut d'un projet dans dim_project."""
    allowed = {"Actif", "Terminé", "En pause", "En attente"}
    if body.status not in allowed:
        raise HTTPException(status_code=400, detail=f"Statut invalide. Valeurs acceptées : {allowed}")
    rows = q("SELECT project_key FROM dwh.dim_project WHERE UPPER(project_code) = :code",
             {"code": project_code.upper()})
    if not rows:
        raise HTTPException(status_code=404, detail=f"Projet {project_code} introuvable")
    with _engine.connect() as conn:
        conn.execute(
            text("UPDATE dwh.dim_project SET status = :status WHERE UPPER(project_code) = :code"),
            {"status": body.status, "code": project_code.upper()},
        )
        conn.commit()
    return {"project_code": project_code.upper(), "status": body.status}


@router.get("/projects/{project_code}/charts/trend")
def project_trend(
    project_code: str,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    """Évolution mensuelle des tickets créés / résolus et du taux livraison."""
    date_clause = "AND dd.full_date BETWEEN :date_from AND :date_to" if date_from and date_to else \
                  "AND dd.full_date >= CURRENT_DATE - INTERVAL '12 months'"
    params: dict = {"code": project_code.upper()}
    if date_from and date_to:
        params["date_from"] = date_from
        params["date_to"]   = date_to

    rows = q(f"""
        SELECT
            dd.month_year                                                AS month,
            dd.year,
            dd.month                                                     AS month_num,
            COUNT(ft.ticket_fact_key)                                   AS created,
            COUNT(CASE WHEN ft.lead_time_is_final = 1 THEN 1 END)      AS resolved,
            ROUND(
                COUNT(CASE WHEN ft.is_delayed = 0 AND ft.lead_time_is_final = 1 THEN 1 END) * 100.0
                / NULLIF(COUNT(CASE WHEN ft.lead_time_is_final = 1 THEN 1 END), 0)
            , 1)                                                         AS on_time_pct
        FROM dwh.fact_tickets ft
        JOIN dwh.dim_project dp ON ft.project_key = dp.project_key
        JOIN dwh.dim_date dd    ON ft.date_key_created = dd.date_key
        WHERE UPPER(dp.project_code) = :code
          {date_clause}
        GROUP BY dd.month_year, dd.year, dd.month
        ORDER BY dd.year, dd.month
    """, params)
    return rows


# ════════════════════════════════════════════════════════════════════════════
# PERSONNEL
# ════════════════════════════════════════════════════════════════════════════

@router.get("/personnel")
def list_personnel():
    return q("""
        SELECT
            da.assignee_key                      AS id,
            da.assignee_name                     AS name,
            COALESCE(da.assignee_email, '')     AS email,
            COALESCE(da.team, 'Non assigne')    AS team,
            COALESCE(da.role, 'git+jira')       AS source,
            COALESCE(da.departement, 'IT')      AS departement
        FROM dwh.dim_assignee da
        ORDER BY da.assignee_name
    """)


@router.get("/personnel/{assignee_key}/kpis")
def person_kpis(
    assignee_key: int,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    person = q("""
        SELECT
            da.assignee_name                     AS name,
            COALESCE(da.team, 'Non assigne')    AS team,
            COALESCE(da.role, 'Developpeur')    AS source,
            COALESCE(da.departement, 'IT')      AS departement,
            COALESCE(da.assignee_email, '')     AS email
        FROM dwh.dim_assignee da
        WHERE da.assignee_key = :key
    """, {"key": assignee_key})

    if not person:
        raise HTTPException(status_code=404, detail="Personnel not found")

    date_clause = "AND snapshot_date BETWEEN :date_from AND :date_to" if date_from and date_to else ""
    params = {"key": assignee_key, **({"date_from": date_from, "date_to": date_to} if date_from and date_to else {})}

    metrics = q(f"""
        SELECT
            COALESCE(SUM(ftp.nb_tickets_assigned), 0)     AS nb_assigned,
            COALESCE(SUM(ftp.nb_tickets_done), 0)         AS nb_done,
            ROUND(AVG(ftp.on_time_rate)::numeric, 1)      AS on_time_pct,
            ROUND(AVG(ftp.completion_rate)::numeric, 1)   AS load_pct,
            ROUND(AVG(ftp.performance_score)::numeric, 1) AS perf_score,
            COALESCE(SUM(ftp.nb_bugs_assigned), 0)        AS nb_bugs
        FROM dwh.fact_team_performance ftp
        WHERE ftp.assignee_key = :key
          {date_clause}
    """, params)

    m = metrics[0] if metrics else {}
    p = person[0]

    return {
        "name":        p["name"],
        "team":        p["team"],
        "source":      p["source"],
        "departement": p["departement"],
        "email":       p.get("email", ""),
        "nb_assigned": int(m.get("nb_assigned") or 0),
        "nb_done":     int(m.get("nb_done") or 0),
        "on_time_pct": float(m.get("on_time_pct") or 0),
        "load_pct":    float(m.get("load_pct") or 0),
        "perf_score":  float(m.get("perf_score") or 0),
        "nb_bugs":     int(m.get("nb_bugs") or 0),
    }


@router.get("/personnel/{assignee_key}/charts/tasks")
def person_tasks(assignee_key: int):
    rows = q("""
        SELECT
            COALESCE(ft.issue_type, 'Autre') AS type,
            COUNT(*) AS count
        FROM dwh.fact_tickets ft
        WHERE ft.assignee_key = :key
        GROUP BY ft.issue_type
        ORDER BY count DESC
    """, {"key": assignee_key})
    return rows


@router.get("/personnel/{assignee_key}/charts/trend")
def person_trend(
    assignee_key: int,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    if date_from and date_to:
        rows = q("""
            SELECT
                dd.month_year,
                dd.year,
                dd.month,
                COUNT(ft.ticket_fact_key)                                        AS tickets_done,
                ROUND(AVG(CASE WHEN ft.is_delayed = 0 THEN 100.0 ELSE 0 END), 1) AS on_time_pct
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_date dd ON ft.date_key_resolved = dd.date_key
            WHERE ft.assignee_key = :key
              AND dd.full_date BETWEEN :date_from AND :date_to
            GROUP BY dd.month_year, dd.year, dd.month
            ORDER BY dd.year, dd.month
        """, {"key": assignee_key, "date_from": date_from, "date_to": date_to})
    else:
        rows = q("""
            SELECT
                dd.month_year,
                dd.year,
                dd.month,
                COUNT(ft.ticket_fact_key)                                        AS tickets_done,
                ROUND(AVG(CASE WHEN ft.is_delayed = 0 THEN 100.0 ELSE 0 END), 1) AS on_time_pct
            FROM dwh.fact_tickets ft
            JOIN dwh.dim_date dd ON ft.date_key_resolved = dd.date_key
            WHERE ft.assignee_key = :key
              AND dd.full_date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY dd.month_year, dd.year, dd.month
            ORDER BY dd.year, dd.month
        """, {"key": assignee_key})
    return rows


@router.get("/personnel/{assignee_key}/charts/comparison")
def person_comparison(
    assignee_key: int,
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
):
    team_info = q("""
        SELECT COALESCE(da.team, '') AS team
        FROM dwh.dim_assignee da WHERE da.assignee_key = :key
    """, {"key": assignee_key})

    team = team_info[0]["team"] if team_info else ""
    date_clause = "AND ftp.snapshot_date BETWEEN :date_from AND :date_to" if date_from and date_to else ""
    date_params = {"date_from": date_from, "date_to": date_to} if date_from and date_to else {}

    if team:
        rows = q(f"""
            SELECT
                da.assignee_name                                   AS name,
                ROUND(AVG(ftp.performance_score)::numeric, 1)     AS score,
                (da.assignee_key = :key)                          AS is_current
            FROM dwh.fact_team_performance ftp
            JOIN dwh.dim_assignee da ON ftp.assignee_key = da.assignee_key
            WHERE da.team = :team
              {date_clause}
            GROUP BY da.assignee_name, da.assignee_key
            ORDER BY score DESC
        """, {"team": team, "key": assignee_key, **date_params})
    else:
        rows = q(f"""
            SELECT
                da.assignee_name                                   AS name,
                ROUND(AVG(ftp.performance_score)::numeric, 1)     AS score,
                (da.assignee_key = :key)                          AS is_current
            FROM dwh.fact_team_performance ftp
            JOIN dwh.dim_assignee da ON ftp.assignee_key = da.assignee_key
            WHERE 1=1
              {date_clause}
            GROUP BY da.assignee_name, da.assignee_key
            ORDER BY score DESC
            LIMIT 15
        """, {"key": assignee_key, **date_params})
    return rows
