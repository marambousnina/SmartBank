-- ============================================================
--  SmartBank Metrics -- Data Warehouse Star Schema
--  Optimise pour Power BI (Star Schema)
--  Schema : dwh
-- ============================================================
--
--  Architecture :
--
--           DIM_DATE
--               |
--  DIM_ASSIGNEE-+--FACT_TICKETS--+--DIM_PROJECT
--               |                |
--           DIM_STATUS       DIM_SPRINT
--
--           DIM_DATE
--               |
--           FACT_DEPLOYMENTS
--
--           DIM_DATE
--               |
--  DIM_ASSIGNEE-+--FACT_COMMITS--DIM_PROJECT
--
--           DIM_DATE
--               |
--           FACT_DORA_SNAPSHOT
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dwh;
GRANT ALL ON SCHEMA dwh TO smartbank_user;

-- ============================================================
-- DIMENSION : DIM_DATE
-- Cle : date_key (YYYYMMDD) — Power BI time intelligence
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        INTEGER PRIMARY KEY,   -- ex: 20240315
    full_date       DATE    NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,     -- 1-4
    month           SMALLINT NOT NULL,     -- 1-12
    month_name      VARCHAR(20) NOT NULL,  -- Janvier, Fevrier...
    month_year      VARCHAR(10) NOT NULL,  -- 2024-03
    week_of_year    SMALLINT NOT NULL,     -- 1-53
    week_label      VARCHAR(12) NOT NULL,  -- 2024-W11
    day_of_month    SMALLINT NOT NULL,     -- 1-31
    day_of_week     SMALLINT NOT NULL,     -- 1=Lundi .. 7=Dimanche
    day_name        VARCHAR(20) NOT NULL,  -- Lundi, Mardi...
    is_weekend      BOOLEAN NOT NULL DEFAULT FALSE,
    semester        SMALLINT NOT NULL      -- 1 ou 2
);

-- ============================================================
-- DIMENSION : DIM_ASSIGNEE
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.dim_assignee (
    assignee_key    SERIAL PRIMARY KEY,
    assignee_name   VARCHAR(200) NOT NULL UNIQUE,
    assignee_email  VARCHAR(200),
    -- enrichissement possible
    team            VARCHAR(100),
    role            VARCHAR(100)
);

-- ============================================================
-- DIMENSION : DIM_PROJECT
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.dim_project (
    project_key     SERIAL PRIMARY KEY,
    project_code    VARCHAR(50) NOT NULL UNIQUE,  -- ex: MBC, DL
    project_name    VARCHAR(200),
    domain          VARCHAR(100)
);

-- ============================================================
-- DIMENSION : DIM_STATUS
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.dim_status (
    status_key      SERIAL PRIMARY KEY,
    status_name     VARCHAR(100) NOT NULL UNIQUE,
    status_category VARCHAR(50),    -- To Do / In Progress / Done
    is_final        BOOLEAN NOT NULL DEFAULT FALSE
);

-- ============================================================
-- DIMENSION : DIM_SPRINT
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.dim_sprint (
    sprint_key      SERIAL PRIMARY KEY,
    sprint_name     VARCHAR(200) NOT NULL,
    sprint_state    VARCHAR(50),    -- active / closed / future
    project_code    VARCHAR(50),
    UNIQUE (sprint_name, project_code)
);

-- ============================================================
-- DIMENSION : DIM_RISK_LEVEL
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.dim_risk_level (
    risk_key        SERIAL PRIMARY KEY,
    risk_level      VARCHAR(20) NOT NULL UNIQUE,  -- Low/Medium/High/Critical
    risk_min_score  DOUBLE PRECISION,
    risk_max_score  DOUBLE PRECISION,
    risk_color      VARCHAR(20)   -- pour Power BI conditional formatting
);

-- ============================================================
-- DIMENSION : DIM_DORA_LEVEL
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.dim_dora_level (
    dora_key        SERIAL PRIMARY KEY,
    dora_level      VARCHAR(20) NOT NULL UNIQUE,  -- Elite/High/Medium/Low
    dora_score      SMALLINT,   -- 4=Elite, 3=High, 2=Medium, 1=Low
    dora_color      VARCHAR(20)
);


-- ============================================================
-- FAIT : FACT_TICKETS
-- Grain : 1 ligne par ticket Jira
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.fact_tickets (
    ticket_fact_key     SERIAL PRIMARY KEY,

    -- Cles etrangeres dimensions
    ticket_key          VARCHAR(50)  NOT NULL,
    date_key_created    INTEGER      REFERENCES dwh.dim_date(date_key),
    date_key_resolved   INTEGER      REFERENCES dwh.dim_date(date_key),
    date_key_due        INTEGER      REFERENCES dwh.dim_date(date_key),
    assignee_key        INTEGER      REFERENCES dwh.dim_assignee(assignee_key),
    project_key         INTEGER      REFERENCES dwh.dim_project(project_key),
    status_key          INTEGER      REFERENCES dwh.dim_status(status_key),
    sprint_key          INTEGER      REFERENCES dwh.dim_sprint(sprint_key),
    risk_key            INTEGER      REFERENCES dwh.dim_risk_level(risk_key),

    -- Mesures (valeurs numeriques pour Power BI)
    lead_time_hours         DOUBLE PRECISION,
    cycle_time_hours        DOUBLE PRECISION,
    time_blocked_hours      DOUBLE PRECISION,
    time_in_review_hours    DOUBLE PRECISION,
    time_in_qa_hours        DOUBLE PRECISION,
    nb_status_changes       INTEGER,
    nb_commits              INTEGER,
    nb_mrs                  INTEGER,
    avg_merge_time_hours    DOUBLE PRECISION,
    risk_score              DOUBLE PRECISION,

    -- Indicateurs booleens (utiles pour les mesures DAX)
    is_delayed              SMALLINT,   -- 0/1
    is_bug                  SMALLINT,   -- 0/1
    was_blocked             SMALLINT,   -- 0/1
    lead_time_is_final      SMALLINT,   -- 0/1

    -- Attributs degrades (evite jointures Power BI)
    issue_type              VARCHAR(100),
    priority                VARCHAR(50),
    data_source             VARCHAR(50),  -- jira+git / git_only

    -- Metadata
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ticket_key)
);

-- ============================================================
-- FAIT : FACT_DEPLOYMENTS
-- Grain : 1 ligne par execution de pipeline CI/CD
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.fact_deployments (
    deployment_fact_key SERIAL PRIMARY KEY,

    -- Cles etrangeres
    pipeline_id         BIGINT       NOT NULL,
    date_key            INTEGER      REFERENCES dwh.dim_date(date_key),

    -- Mesures
    duration_minutes    DOUBLE PRECISION,
    is_production       BOOLEAN,
    is_failed           BOOLEAN,

    -- Attributs degrades
    status              VARCHAR(50),
    ref                 VARCHAR(200),
    sha                 VARCHAR(100),

    -- Metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pipeline_id)
);

-- ============================================================
-- FAIT : FACT_COMMITS
-- Grain : 1 ligne par commit Git
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.fact_commits (
    commit_fact_key     SERIAL PRIMARY KEY,

    -- Cles etrangeres
    sha                 VARCHAR(100) NOT NULL,
    date_key            INTEGER      REFERENCES dwh.dim_date(date_key),
    assignee_key        INTEGER      REFERENCES dwh.dim_assignee(assignee_key),
    project_key         INTEGER      REFERENCES dwh.dim_project(project_key),

    -- Attributs degrades
    ticket_key          VARCHAR(50),
    data_source         VARCHAR(50),  -- jira+git / git_only
    title               TEXT,

    -- Metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sha)
);

-- ============================================================
-- FAIT : FACT_DORA_SNAPSHOT
-- Grain : 1 snapshot DORA par semaine
-- Historique des 4 metriques DORA dans le temps
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.fact_dora_snapshot (
    dora_fact_key       SERIAL PRIMARY KEY,

    -- Cle dimension date
    date_key            INTEGER      REFERENCES dwh.dim_date(date_key),
    week_label          VARCHAR(12)  NOT NULL,   -- ex: 2024-W11

    -- Mesures Deployment Frequency
    deployment_count        INTEGER,
    deploy_freq_per_week    DOUBLE PRECISION,
    deploy_freq_key         INTEGER REFERENCES dwh.dim_dora_level(dora_key),

    -- Mesures Lead Time
    lead_time_jira_hours    DOUBLE PRECISION,
    lead_time_git_hours     DOUBLE PRECISION,
    lead_time_global_hours  DOUBLE PRECISION,
    lead_time_key           INTEGER REFERENCES dwh.dim_dora_level(dora_key),

    -- Mesures Change Failure Rate
    total_pipelines         INTEGER,
    failed_pipelines        INTEGER,
    cfr_pct                 DOUBLE PRECISION,
    cfr_key                 INTEGER REFERENCES dwh.dim_dora_level(dora_key),

    -- MTTR
    mttr_global_hours       DOUBLE PRECISION,
    mttr_key                INTEGER REFERENCES dwh.dim_dora_level(dora_key),

    -- Metadata
    snapshot_date           DATE DEFAULT CURRENT_DATE,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (week_label, snapshot_date)
);

-- ============================================================
-- FAIT : FACT_TEAM_PERFORMANCE
-- Grain : 1 ligne par (assignee, project, snapshot_date)
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.fact_team_performance (
    team_fact_key       SERIAL PRIMARY KEY,

    -- Cles etrangeres
    date_key            INTEGER      REFERENCES dwh.dim_date(date_key),
    assignee_key        INTEGER      REFERENCES dwh.dim_assignee(assignee_key),
    project_key         INTEGER      REFERENCES dwh.dim_project(project_key),

    -- Mesures
    nb_tickets_assigned     INTEGER,
    nb_tickets_done         INTEGER,
    nb_bugs_assigned        INTEGER,
    nb_delayed              INTEGER,
    nb_commits_total        INTEGER,
    nb_mrs_total            INTEGER,

    avg_lead_time_hours     DOUBLE PRECISION,
    avg_cycle_time_hours    DOUBLE PRECISION,
    avg_time_blocked_hours  DOUBLE PRECISION,
    on_time_rate            DOUBLE PRECISION,
    completion_rate         DOUBLE PRECISION,
    performance_score       DOUBLE PRECISION,

    -- Metadata
    snapshot_date           DATE DEFAULT CURRENT_DATE,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (assignee_key, project_key, snapshot_date)
);

-- ============================================================
-- INDEXES pour performances Power BI
-- ============================================================

-- fact_tickets
CREATE INDEX IF NOT EXISTS idx_ft_project      ON dwh.fact_tickets(project_key);
CREATE INDEX IF NOT EXISTS idx_ft_assignee     ON dwh.fact_tickets(assignee_key);
CREATE INDEX IF NOT EXISTS idx_ft_date_created ON dwh.fact_tickets(date_key_created);
CREATE INDEX IF NOT EXISTS idx_ft_date_resolved ON dwh.fact_tickets(date_key_resolved);
CREATE INDEX IF NOT EXISTS idx_ft_risk         ON dwh.fact_tickets(risk_key);
CREATE INDEX IF NOT EXISTS idx_ft_status       ON dwh.fact_tickets(status_key);

-- fact_deployments
CREATE INDEX IF NOT EXISTS idx_fd_date         ON dwh.fact_deployments(date_key);
CREATE INDEX IF NOT EXISTS idx_fd_prod         ON dwh.fact_deployments(is_production);

-- fact_commits
CREATE INDEX IF NOT EXISTS idx_fc_date         ON dwh.fact_commits(date_key);
CREATE INDEX IF NOT EXISTS idx_fc_assignee     ON dwh.fact_commits(assignee_key);
CREATE INDEX IF NOT EXISTS idx_fc_project      ON dwh.fact_commits(project_key);

-- fact_dora
CREATE INDEX IF NOT EXISTS idx_fdora_week      ON dwh.fact_dora_snapshot(week_label);
CREATE INDEX IF NOT EXISTS idx_fdora_date      ON dwh.fact_dora_snapshot(date_key);

-- fact_team
CREATE INDEX IF NOT EXISTS idx_fteam_assignee  ON dwh.fact_team_performance(assignee_key);
CREATE INDEX IF NOT EXISTS idx_fteam_project   ON dwh.fact_team_performance(project_key);
CREATE INDEX IF NOT EXISTS idx_fteam_date      ON dwh.fact_team_performance(date_key);

-- ============================================================
-- DROITS
-- ============================================================
GRANT ALL ON ALL TABLES IN SCHEMA dwh TO smartbank_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dwh TO smartbank_user;
