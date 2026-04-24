# -*- coding: utf-8 -*-
"""
SmartBank Metrics - REST API
=============================
Expose les donnees PostgreSQL via FastAPI.

Lancement :
    uvicorn api.main:app --reload --port 8000

Swagger UI :
    http://localhost:8000/docs
"""

import sys
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine
from api.routers.dwh import router as dwh_router
from api.routers.predictions import router as predictions_router

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="SmartBank Metrics API",
    description="API REST pour visualiser les donnees DORA et performance equipe",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = get_engine()
app.include_router(dwh_router)
app.include_router(predictions_router)


def run_query(sql: str, params: dict = None) -> list[dict]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]


# ════════════════════════════════════════════════════════════════════════════
# HEALTH
# ════════════════════════════════════════════════════════════════════════════

@app.get("/health", tags=["System"])
def health_check():
    """Verifie que l'API et la base de donnees sont operationnelles."""
    try:
        rows = run_query("SELECT COUNT(*) as total FROM cleaned.commits")
        return {
            "status": "ok",
            "database": "connected",
            "commits_in_db": rows[0]["total"],
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/stats", tags=["System"])
def global_stats():
    """Nombre de lignes dans chaque table chargee."""
    tables = {
        "cleaned.jira_status_history": "jira_rows",
        "cleaned.commits":             "commits_rows",
        "cleaned.pipelines":           "pipelines_rows",
        "cleaned.jobs":                "jobs_rows",
        "cleaned.merge_requests":      "mr_rows",
        "features.tickets":            "tickets_rows",
        "features.assignee_metrics":   "assignee_rows",
        "features.project_metrics":    "project_rows",
    }
    stats = {}
    for table, key in tables.items():
        rows = run_query(f"SELECT COUNT(*) as n FROM {table}")
        stats[key] = rows[0]["n"]
    return stats


# ════════════════════════════════════════════════════════════════════════════
# CLEANED - Jira
# ════════════════════════════════════════════════════════════════════════════

@app.get("/cleaned/jira", tags=["Cleaned"])
def get_jira(
    project: Optional[str] = Query(None, description="Filtrer par projet (ex: DL, MBC)"),
    assignee: Optional[str] = Query(None, description="Filtrer par assignee"),
    status: Optional[str] = Query(None, description="Filtrer par statut"),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
):
    """Retourne l'historique des statuts Jira nettoyes."""
    where = ["1=1"]
    params = {"limit": limit, "offset": offset}

    if project:
        where.append("project_key = :project")
        params["project"] = project.upper()
    if assignee:
        where.append("LOWER(assignee) LIKE :assignee")
        params["assignee"] = f"%{assignee.lower()}%"
    if status:
        where.append("LOWER(status) = :status")
        params["status"] = status.lower()

    sql = f"""
        SELECT ticket_key, project_key, status, assignee,
               status_entry_date, time_in_status_hours, priority
        FROM cleaned.jira_status_history
        WHERE {' AND '.join(where)}
        ORDER BY ticket_key, status_entry_date
        LIMIT :limit OFFSET :offset
    """
    rows = run_query(sql, params)
    total = run_query(
        f"SELECT COUNT(*) as n FROM cleaned.jira_status_history WHERE {' AND '.join(where)}",
        {k: v for k, v in params.items() if k not in ("limit", "offset")}
    )[0]["n"]
    return {"total": total, "limit": limit, "offset": offset, "data": rows}


# ════════════════════════════════════════════════════════════════════════════
# CLEANED - Commits
# ════════════════════════════════════════════════════════════════════════════

@app.get("/cleaned/commits", tags=["Cleaned"])
def get_commits(
    ticket_key: Optional[str] = Query(None, description="Filtrer par ticket Jira (ex: DL-123)"),
    author: Optional[str] = Query(None, description="Filtrer par auteur"),
    linked_only: bool = Query(False, description="Seulement les commits lies a Jira"),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
):
    """Retourne les commits GitLab nettoyes."""
    where = ["1=1"]
    params = {"limit": limit, "offset": offset}

    if ticket_key:
        where.append("ticket_key = :ticket_key")
        params["ticket_key"] = ticket_key.upper()
    if author:
        where.append("LOWER(author) LIKE :author")
        params["author"] = f"%{author.lower()}%"
    if linked_only:
        where.append("ticket_key IS NOT NULL")

    sql = f"""
        SELECT sha, author, email, commit_date, title, ticket_key
        FROM cleaned.commits
        WHERE {' AND '.join(where)}
        ORDER BY commit_date DESC
        LIMIT :limit OFFSET :offset
    """
    rows = run_query(sql, params)
    total = run_query(
        f"SELECT COUNT(*) as n FROM cleaned.commits WHERE {' AND '.join(where)}",
        {k: v for k, v in params.items() if k not in ("limit", "offset")}
    )[0]["n"]
    return {"total": total, "limit": limit, "offset": offset, "data": rows}


# ════════════════════════════════════════════════════════════════════════════
# CLEANED - Pipelines
# ════════════════════════════════════════════════════════════════════════════

@app.get("/cleaned/pipelines", tags=["Cleaned"])
def get_pipelines(
    status: Optional[str] = Query(None, description="success | failed | canceled"),
    production_only: bool = Query(False, description="Seulement les pipelines de production"),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
):
    """Retourne les pipelines GitLab CI/CD nettoyes."""
    where = ["1=1"]
    params = {"limit": limit, "offset": offset}

    if status:
        where.append("status = :status")
        params["status"] = status.lower()
    if production_only:
        where.append("is_production = true")

    sql = f"""
        SELECT pipeline_id, status, ref, created_at, duration_minutes, is_production
        FROM cleaned.pipelines
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """
    rows = run_query(sql, params)
    total = run_query(
        f"SELECT COUNT(*) as n FROM cleaned.pipelines WHERE {' AND '.join(where)}",
        {k: v for k, v in params.items() if k not in ("limit", "offset")}
    )[0]["n"]
    return {"total": total, "limit": limit, "offset": offset, "data": rows}


@app.get("/cleaned/pipelines/summary", tags=["Cleaned"])
def get_pipelines_summary():
    """Repartition des pipelines par statut."""
    sql = """
        SELECT
            status,
            COUNT(*) as nb,
            ROUND(AVG(duration_minutes)::numeric, 2) as avg_duration_min,
            COUNT(*) FILTER (WHERE is_production) as nb_production
        FROM cleaned.pipelines
        GROUP BY status
        ORDER BY nb DESC
    """
    return run_query(sql)


# ════════════════════════════════════════════════════════════════════════════
# CLEANED - Jobs
# ════════════════════════════════════════════════════════════════════════════

@app.get("/cleaned/jobs/summary", tags=["Cleaned"])
def get_jobs_summary():
    """Top 10 jobs par nombre d'echecs."""
    sql = """
        SELECT
            name,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status = 'failed') as nb_failed,
            ROUND(AVG(duration_minutes)::numeric, 2) as avg_duration_min
        FROM cleaned.jobs
        GROUP BY name
        ORDER BY nb_failed DESC
        LIMIT 10
    """
    return run_query(sql)


# ════════════════════════════════════════════════════════════════════════════
# FEATURES - Tickets
# ════════════════════════════════════════════════════════════════════════════

@app.get("/features/tickets", tags=["Features"])
def get_tickets(
    project: Optional[str] = Query(None, description="Filtrer par projet"),
    risk_level: Optional[str] = Query(None, description="Low | Medium | High"),
    assignee: Optional[str] = Query(None, description="Filtrer par assignee"),
    delayed_only: bool = Query(False, description="Seulement les tickets en retard"),
    blocked_only: bool = Query(False, description="Seulement les tickets bloques"),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
):
    """Retourne les tickets Jira enrichis avec toutes les features calculees."""
    where = ["1=1"]
    params = {"limit": limit, "offset": offset}

    if project:
        where.append("project_key = :project")
        params["project"] = project.upper()
    if risk_level:
        where.append("risk_level = :risk_level")
        params["risk_level"] = risk_level.capitalize()
    if assignee:
        where.append("LOWER(assignee) LIKE :assignee")
        params["assignee"] = f"%{assignee.lower()}%"
    if delayed_only:
        where.append("is_delayed = 1")
    if blocked_only:
        where.append("was_blocked = 1")

    sql = f"""
        SELECT
            ticket_key, project_key, issue_type, current_status,
            assignee, priority, lead_time_hours, cycle_time_hours,
            is_delayed, was_blocked, nb_commits, nb_mrs,
            risk_score, risk_level, data_source, month_year
        FROM features.tickets
        WHERE {' AND '.join(where)}
        ORDER BY risk_score DESC, lead_time_hours DESC
        LIMIT :limit OFFSET :offset
    """
    rows = run_query(sql, params)
    total = run_query(
        f"SELECT COUNT(*) as n FROM features.tickets WHERE {' AND '.join(where)}",
        {k: v for k, v in params.items() if k not in ("limit", "offset")}
    )[0]["n"]
    return {"total": total, "limit": limit, "offset": offset, "data": rows}


@app.get("/features/tickets/{ticket_key}", tags=["Features"])
def get_ticket_detail(ticket_key: str):
    """Retourne le detail complet d'un ticket specifique."""
    sql = "SELECT * FROM features.tickets WHERE UPPER(ticket_key) = :key"
    rows = run_query(sql, {"key": ticket_key.upper()})
    if not rows:
        raise HTTPException(status_code=404, detail=f"Ticket {ticket_key} non trouve")
    return rows[0]


@app.get("/features/tickets/project/{project_key}", tags=["Features"])
def get_tickets_by_project(project_key: str):
    """Statistiques agregees par projet."""
    sql = """
        SELECT
            project_key,
            COUNT(*) as nb_tickets,
            COUNT(*) FILTER (WHERE is_delayed = 1) as nb_delayed,
            COUNT(*) FILTER (WHERE was_blocked = 1) as nb_blocked,
            ROUND(AVG(lead_time_hours)::numeric, 1) as avg_lead_time_h,
            ROUND(AVG(cycle_time_hours)::numeric, 1) as avg_cycle_time_h,
            ROUND(AVG(risk_score)::numeric, 1) as avg_risk_score,
            MAX(completion_rate) as completion_rate
        FROM features.tickets
        WHERE UPPER(project_key) = :project_key
        GROUP BY project_key
    """
    rows = run_query(sql, {"project_key": project_key.upper()})
    if not rows:
        raise HTTPException(status_code=404, detail=f"Projet {project_key} non trouve")
    return rows[0]


# ════════════════════════════════════════════════════════════════════════════
# FEATURES - Assignees
# ════════════════════════════════════════════════════════════════════════════

@app.get("/features/assignees", tags=["Features"])
def get_assignees(
    sort_by: str = Query("avg_lead_time_hours", description="Colonne de tri"),
    order: str = Query("asc", description="asc | desc"),
):
    """Performance par developpeur - triee et classee."""
    allowed_sort = {"avg_lead_time_hours", "nb_tickets_assigned", "on_time_rate", "nb_tickets_deployed"}
    if sort_by not in allowed_sort:
        sort_by = "avg_lead_time_hours"
    direction = "DESC" if order.lower() == "desc" else "ASC"

    sql = f"""
        SELECT
            assignee,
            nb_tickets_assigned,
            nb_tickets_deployed,
            ROUND(avg_lead_time_hours::numeric, 1) as avg_lead_time_hours,
            ROUND(on_time_rate::numeric, 1) as on_time_rate,
            ROUND(nb_tickets_deployed::numeric / NULLIF(nb_tickets_assigned, 0) * 100, 1) as completion_rate
        FROM features.assignee_metrics
        ORDER BY {sort_by} {direction}
    """
    return run_query(sql)


@app.get("/features/assignees/{assignee_name}", tags=["Features"])
def get_assignee_detail(assignee_name: str):
    """Detail d'un developpeur + ses tickets."""
    metrics = run_query(
        "SELECT * FROM features.assignee_metrics WHERE LOWER(assignee) = :name",
        {"name": assignee_name.lower()}
    )
    if not metrics:
        raise HTTPException(status_code=404, detail=f"Assignee '{assignee_name}' non trouve")

    tickets = run_query(
        """SELECT ticket_key, project_key, current_status, issue_type,
                  lead_time_hours, cycle_time_hours, is_delayed, was_blocked,
                  risk_score, risk_level
           FROM features.tickets
           WHERE LOWER(assignee) = :name
           ORDER BY lead_time_hours DESC""",
        {"name": assignee_name.lower()}
    )
    return {"metrics": metrics[0], "tickets": tickets}


# ════════════════════════════════════════════════════════════════════════════
# FEATURES - Projets
# ════════════════════════════════════════════════════════════════════════════

@app.get("/features/projects", tags=["Features"])
def get_projects():
    """Vue d'ensemble des 4 projets avec taux de completion."""
    sql = """
        SELECT
            project_key,
            nb_tickets_total,
            nb_tickets_done,
            ROUND(completion_rate::numeric, 1) as completion_rate
        FROM features.project_metrics
        ORDER BY completion_rate DESC
    """
    return run_query(sql)


# ════════════════════════════════════════════════════════════════════════════
# ANALYTICS - Vues transverses
# ════════════════════════════════════════════════════════════════════════════

@app.get("/analytics/risk-distribution", tags=["Analytics"])
def get_risk_distribution():
    """Repartition des niveaux de risque par projet."""
    sql = """
        SELECT
            project_key,
            risk_level,
            COUNT(*) as nb_tickets,
            ROUND(AVG(risk_score)::numeric, 1) as avg_risk_score
        FROM features.tickets
        GROUP BY project_key, risk_level
        ORDER BY project_key, risk_level
    """
    return run_query(sql)


@app.get("/analytics/lead-time-by-project", tags=["Analytics"])
def get_lead_time_by_project():
    """Distribution du lead time par projet (percentiles)."""
    sql = """
        SELECT
            project_key,
            COUNT(*) as nb_tickets,
            ROUND(MIN(lead_time_hours)::numeric, 1) as min_h,
            ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY lead_time_hours)::numeric, 1) as p25_h,
            ROUND(PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY lead_time_hours)::numeric, 1) as median_h,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY lead_time_hours)::numeric, 1) as p75_h,
            ROUND(MAX(lead_time_hours)::numeric, 1) as max_h,
            ROUND(AVG(lead_time_hours)::numeric, 1) as avg_h
        FROM features.tickets
        WHERE lead_time_hours IS NOT NULL
        GROUP BY project_key
        ORDER BY median_h
    """
    return run_query(sql)


@app.get("/analytics/monthly-activity", tags=["Analytics"])
def get_monthly_activity():
    """Evolution mensuelle du nombre de tickets par projet."""
    sql = """
        SELECT
            month_year,
            project_key,
            COUNT(*) as nb_tickets,
            COUNT(*) FILTER (WHERE is_delayed = 1) as nb_delayed,
            ROUND(AVG(lead_time_hours)::numeric, 1) as avg_lead_time_h
        FROM features.tickets
        WHERE month_year IS NOT NULL
        GROUP BY month_year, project_key
        ORDER BY month_year, project_key
    """
    return run_query(sql)


@app.get("/analytics/top-blocked-tickets", tags=["Analytics"])
def get_top_blocked(limit: int = Query(10, le=50)):
    """Tickets avec le plus de temps passe en blocage."""
    sql = """
        SELECT
            ticket_key, project_key, assignee, current_status,
            time_blocked_hours, nb_status_changes, lead_time_hours, risk_level
        FROM features.tickets
        WHERE was_blocked = 1
        ORDER BY time_blocked_hours DESC
        LIMIT :limit
    """
    return run_query(sql, {"limit": limit})


@app.get("/analytics/pipeline-failure-rate", tags=["Analytics"])
def get_pipeline_failure_rate():
    """Taux d'echec des pipelines par semaine."""
    sql = """
        SELECT
            TO_CHAR(DATE_TRUNC('week', created_at), 'IYYY-IW') as week,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status = 'failed') as failed,
            ROUND(
                COUNT(*) FILTER (WHERE status = 'failed') * 100.0 / NULLIF(COUNT(*), 0),
                1
            ) as failure_rate_pct
        FROM cleaned.pipelines
        WHERE created_at IS NOT NULL
        GROUP BY DATE_TRUNC('week', created_at)
        ORDER BY week DESC
        LIMIT 12
    """
    return run_query(sql)
