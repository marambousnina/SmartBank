-- ============================================================
-- SmartBank Metrics - PostgreSQL Database Schema
-- ============================================================
-- Schémas: cleaned | features | dora_metrics | team_metrics | ml_results
-- ============================================================

-- ============================================================
-- 0. EXTENSIONS & SCHEMAS
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS cleaned;
CREATE SCHEMA IF NOT EXISTS features;
CREATE SCHEMA IF NOT EXISTS dora_metrics;
CREATE SCHEMA IF NOT EXISTS team_metrics;
CREATE SCHEMA IF NOT EXISTS ml_results;

-- Droits utilisateur
GRANT ALL ON SCHEMA cleaned      TO smartbank_user;
GRANT ALL ON SCHEMA features     TO smartbank_user;
GRANT ALL ON SCHEMA dora_metrics TO smartbank_user;
GRANT ALL ON SCHEMA team_metrics TO smartbank_user;
GRANT ALL ON SCHEMA ml_results   TO smartbank_user;


-- ============================================================
-- 1. SCHEMA: cleaned  (données nettoyées)
-- ============================================================

CREATE TABLE IF NOT EXISTS cleaned.jira_status_history (
    id                  SERIAL PRIMARY KEY,
    ticket_key          VARCHAR(50)  NOT NULL,
    status              VARCHAR(100),
    status_entry_date   TIMESTAMP,
    status_exit_date    TIMESTAMP,
    time_in_status_hours DOUBLE PRECISION,
    priority            VARCHAR(50),
    assignee            VARCHAR(200),
    assignee_email      VARCHAR(200),
    created             TIMESTAMP,
    updated             TIMESTAMP,
    resolution_date     TIMESTAMP,
    due_date            TIMESTAMP,
    sprint_state        VARCHAR(50),
    project_key         VARCHAR(50),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ticket_key, status, status_entry_date)
);

CREATE TABLE IF NOT EXISTS cleaned.commits (
    id          SERIAL PRIMARY KEY,
    sha         VARCHAR(100) UNIQUE NOT NULL,
    author      VARCHAR(200),
    email       VARCHAR(200),
    commit_date TIMESTAMP,
    title       TEXT,
    ticket_key  VARCHAR(50),
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleaned.pipelines (
    id                  SERIAL PRIMARY KEY,
    pipeline_id         BIGINT UNIQUE NOT NULL,
    status              VARCHAR(50),
    ref                 VARCHAR(200),
    sha                 VARCHAR(100),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    duration_minutes    DOUBLE PRECISION,
    is_production       BOOLEAN,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cleaned.jobs (
    id              SERIAL PRIMARY KEY,
    job_id          BIGINT UNIQUE NOT NULL,
    pipeline_id     BIGINT,
    name            VARCHAR(200),
    status          VARCHAR(50),
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    sha             VARCHAR(100),
    duration_minutes DOUBLE PRECISION,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES cleaned.pipelines(pipeline_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS cleaned.merge_requests (
    id                  SERIAL PRIMARY KEY,
    mr_id               BIGINT UNIQUE NOT NULL,
    title               TEXT,
    author              VARCHAR(200),
    state               VARCHAR(50),
    created_at          TIMESTAMP,
    merged_at           TIMESTAMP,
    merge_time_hours    DOUBLE PRECISION,
    ticket_key          VARCHAR(50),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 2. SCHEMA: features  (feature engineering)
-- ============================================================

CREATE TABLE IF NOT EXISTS features.tickets (
    id                          SERIAL PRIMARY KEY,
    ticket_key                  VARCHAR(50) NOT NULL,
    project_key                 VARCHAR(50),
    project                     VARCHAR(200),
    issue_type                  VARCHAR(100),
    current_status              VARCHAR(100),
    status_category             VARCHAR(50),
    priority                    VARCHAR(50),
    assignee                    VARCHAR(200),
    assignee_email              VARCHAR(200),
    sprint_state                VARCHAR(50),
    created                     TIMESTAMP,
    resolution_date             TIMESTAMP,
    due_date                    TIMESTAMP,
    -- Lead Time
    lead_time_hours             DOUBLE PRECISION,
    lead_time_is_final          SMALLINT,
    is_delayed                  SMALLINT,
    -- Status Transitions
    nb_status_changes           INTEGER,
    time_blocked_hours          DOUBLE PRECISION,
    time_in_review_hours        DOUBLE PRECISION,
    time_in_qa_hours            DOUBLE PRECISION,
    time_correction_review_hours DOUBLE PRECISION,
    time_correction_qa_hours    DOUBLE PRECISION,
    was_blocked                 SMALLINT,
    -- Cycle Time
    cycle_time_hours            DOUBLE PRECISION,
    -- Git Integration
    nb_commits                  INTEGER,
    nb_mrs                      INTEGER,
    avg_merge_time_hours        DOUBLE PRECISION,
    data_source                 VARCHAR(50),
    -- Classification
    is_bug                      SMALLINT,
    month_year                  VARCHAR(10),
    -- Risk
    risk_score                  DOUBLE PRECISION,
    risk_level                  VARCHAR(20),
    -- Project Aggregates
    nb_tickets_total            INTEGER,
    nb_tickets_done             INTEGER,
    completion_rate             DOUBLE PRECISION,
    -- Metadata
    pipeline_run_id             UUID DEFAULT uuid_generate_v4(),
    loaded_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS features.assignee_metrics (
    id                      SERIAL PRIMARY KEY,
    assignee                VARCHAR(200) NOT NULL,
    nb_tickets_assigned     INTEGER,
    nb_tickets_deployed     INTEGER,
    avg_lead_time_hours     DOUBLE PRECISION,
    on_time_rate            DOUBLE PRECISION,
    pipeline_run_id         UUID DEFAULT uuid_generate_v4(),
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS features.project_metrics (
    id                  SERIAL PRIMARY KEY,
    project_key         VARCHAR(50) NOT NULL,
    nb_tickets_total    INTEGER,
    nb_tickets_done     INTEGER,
    completion_rate     DOUBLE PRECISION,
    pipeline_run_id     UUID DEFAULT uuid_generate_v4(),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 3. SCHEMA: dora_metrics  (DORA KPIs et tendances)
-- ============================================================

CREATE TABLE IF NOT EXISTS dora_metrics.kpis (
    id                      SERIAL PRIMARY KEY,
    -- Deployment Frequency
    deploy_freq_per_week    DOUBLE PRECISION,
    deploy_freq_level       VARCHAR(20),
    -- Lead Time
    lead_time_jira_hours    DOUBLE PRECISION,
    lead_time_git_hours     DOUBLE PRECISION,
    lead_time_global_hours  DOUBLE PRECISION,
    lead_time_level         VARCHAR(20),
    -- Change Failure Rate
    cfr_pct                 DOUBLE PRECISION,
    cfr_level               VARCHAR(20),
    -- MTTR
    mttr_jira_hours         DOUBLE PRECISION,
    mttr_jobs_hours         DOUBLE PRECISION,
    mttr_global_hours       DOUBLE PRECISION,
    mttr_level              VARCHAR(20),
    -- Supporting Counts
    commits_jira_git        INTEGER,
    commits_git_only        INTEGER,
    total_deploys           INTEGER,
    total_pipelines         INTEGER,
    failed_pipelines        INTEGER,
    nb_bugs_jira            INTEGER,
    nb_failed_jobs          INTEGER,
    nb_success_jobs         INTEGER,
    avg_job_fail_min        DOUBLE PRECISION,
    avg_job_fix_min         DOUBLE PRECISION,
    -- Metadata
    snapshot_date           DATE DEFAULT CURRENT_DATE,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dora_metrics.summary (
    id          SERIAL PRIMARY KEY,
    metric      VARCHAR(100),
    source      VARCHAR(50),
    value       DOUBLE PRECISION,
    unit        VARCHAR(50),
    dora_level  VARCHAR(20),
    snapshot_date DATE DEFAULT CURRENT_DATE,
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dora_metrics.by_project (
    id                      SERIAL PRIMARY KEY,
    project_key             VARCHAR(50) NOT NULL,
    nb_tickets              INTEGER,
    nb_deployed             INTEGER,
    avg_lead_time_h         DOUBLE PRECISION,
    median_lead_time_h      DOUBLE PRECISION,
    nb_delayed              INTEGER,
    delay_rate_pct          DOUBLE PRECISION,
    data_source             VARCHAR(50),
    snapshot_date           DATE DEFAULT CURRENT_DATE,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dora_metrics.by_source (
    id                  SERIAL PRIMARY KEY,
    data_source         VARCHAR(50) NOT NULL,
    nb_items            INTEGER,
    avg_lead_time       DOUBLE PRECISION,
    median_lead_time    DOUBLE PRECISION,
    description         TEXT,
    snapshot_date       DATE DEFAULT CURRENT_DATE,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dora_metrics.weekly (
    id                      SERIAL PRIMARY KEY,
    week                    VARCHAR(10) NOT NULL,
    total_pipelines         INTEGER,
    success_pipelines       INTEGER,
    failed_pipelines        INTEGER,
    change_failure_rate     DOUBLE PRECISION,
    deployment_count        INTEGER,
    snapshot_date           DATE DEFAULT CURRENT_DATE,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (week, snapshot_date)
);

CREATE TABLE IF NOT EXISTS dora_metrics.evolution (
    id                  SERIAL PRIMARY KEY,
    week_current        VARCHAR(10),
    week_prev           VARCHAR(10),
    lead_time_delta     DOUBLE PRECISION,
    deploy_freq_delta   DOUBLE PRECISION,
    cfr_delta           DOUBLE PRECISION,
    mttr_delta          DOUBLE PRECISION,
    snapshot_date       DATE DEFAULT CURRENT_DATE,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dora_metrics.jobs_stats (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(200),
    status          VARCHAR(50),
    nb_jobs         INTEGER,
    avg_duration_min DOUBLE PRECISION,
    snapshot_date   DATE DEFAULT CURRENT_DATE,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 4. SCHEMA: team_metrics  (performance équipe & personne)
-- ============================================================

CREATE TABLE IF NOT EXISTS team_metrics.person_dashboard (
    id                          SERIAL PRIMARY KEY,
    assignee                    VARCHAR(200) NOT NULL,
    project_key                 VARCHAR(50),
    data_source_dominant        VARCHAR(50),
    -- Ticket Counts
    nb_tickets_assigned         INTEGER,
    nb_tickets_done             INTEGER,
    completion_rate             DOUBLE PRECISION,
    nb_story                    INTEGER,
    nb_epic                     INTEGER,
    nb_task                     INTEGER,
    nb_bug                      INTEGER,
    -- Bug & Delay
    nb_bugs_assigned            INTEGER,
    nb_bugs_resolved            INTEGER,
    nb_delayed                  INTEGER,
    on_time_rate                DOUBLE PRECISION,
    -- Timing
    avg_lead_time_hours         DOUBLE PRECISION,
    avg_cycle_time_hours        DOUBLE PRECISION,
    avg_time_blocked_hours      DOUBLE PRECISION,
    -- Git
    nb_commits_total            INTEGER,
    nb_mrs_total                INTEGER,
    nb_tickets_jira_git         INTEGER,
    nb_tickets_jira_only        INTEGER,
    -- Performance Scores
    score_delais                DOUBLE PRECISION,
    score_lead_time             DOUBLE PRECISION,
    score_blocages              DOUBLE PRECISION,
    score_volume                DOUBLE PRECISION,
    performance_score           DOUBLE PRECISION,
    performance_level           VARCHAR(50),
    -- Team Comparison
    team_median_lead_time       DOUBLE PRECISION,
    team_avg_on_time_rate       DOUBLE PRECISION,
    lead_time_vs_team           DOUBLE PRECISION,
    on_time_vs_team             DOUBLE PRECISION,
    commits_vs_team             DOUBLE PRECISION,
    charge_level                VARCHAR(50),
    -- Metadata
    snapshot_date               DATE DEFAULT CURRENT_DATE,
    loaded_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_metrics.assignee_performance (
    id                      SERIAL PRIMARY KEY,
    assignee                VARCHAR(200) NOT NULL,
    project_key             VARCHAR(50),
    nb_tickets_assigned     INTEGER,
    nb_tickets_deployed     INTEGER,
    nb_tickets_in_progress  INTEGER,
    nb_bugs_assigned        INTEGER,
    nb_bugs_resolved        INTEGER,
    avg_lead_time_hours     DOUBLE PRECISION,
    avg_cycle_time_hours    DOUBLE PRECISION,
    avg_time_blocked_hours  DOUBLE PRECISION,
    avg_status_changes      DOUBLE PRECISION,
    on_time_rate            DOUBLE PRECISION,
    nb_commits_linked       INTEGER,
    nb_mrs_linked           INTEGER,
    performance_score       DOUBLE PRECISION,
    snapshot_date           DATE DEFAULT CURRENT_DATE,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_metrics.team_performance (
    id                      SERIAL PRIMARY KEY,
    project_key             VARCHAR(50) NOT NULL,
    nb_members              INTEGER,
    nb_tickets_total        INTEGER,
    nb_tickets_deployed     INTEGER,
    nb_bugs_total           INTEGER,
    team_lead_time_median   DOUBLE PRECISION,
    team_lead_time_avg      DOUBLE PRECISION,
    team_cycle_time_avg     DOUBLE PRECISION,
    team_blocked_avg        DOUBLE PRECISION,
    team_on_time_rate       DOUBLE PRECISION,
    team_bug_rate           DOUBLE PRECISION,
    team_completion_rate    DOUBLE PRECISION,
    team_velocity_per_week  DOUBLE PRECISION,
    snapshot_date           DATE DEFAULT CURRENT_DATE,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (project_key, snapshot_date)
);

CREATE TABLE IF NOT EXISTS team_metrics.person_vs_team (
    id                      SERIAL PRIMARY KEY,
    assignee                VARCHAR(200),
    project_key             VARCHAR(50),
    individual_lead_time    DOUBLE PRECISION,
    team_median_lead_time   DOUBLE PRECISION,
    vs_team_delta           DOUBLE PRECISION,
    performance_vs_team_pct DOUBLE PRECISION,
    snapshot_date           DATE DEFAULT CURRENT_DATE,
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_metrics.sprint_metrics (
    id              SERIAL PRIMARY KEY,
    sprint_name     VARCHAR(200),
    project_key     VARCHAR(50),
    sprint_state    VARCHAR(50),
    nb_tickets      INTEGER,
    nb_done         INTEGER,
    velocity        DOUBLE PRECISION,
    avg_lead_time   DOUBLE PRECISION,
    snapshot_date   DATE DEFAULT CURRENT_DATE,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_metrics.monthly_activity (
    id              SERIAL PRIMARY KEY,
    assignee        VARCHAR(200),
    project_key     VARCHAR(50),
    month_year      VARCHAR(10),
    nb_tickets      INTEGER,
    nb_done         INTEGER,
    nb_commits      INTEGER,
    avg_lead_time   DOUBLE PRECISION,
    snapshot_date   DATE DEFAULT CURRENT_DATE,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (assignee, project_key, month_year, snapshot_date)
);

CREATE TABLE IF NOT EXISTS team_metrics.yearly_performance (
    id                          SERIAL PRIMARY KEY,
    assignee                    VARCHAR(200),
    project_key                 VARCHAR(50),
    year                        INTEGER,
    nb_tickets_year             INTEGER,
    nb_done_year                INTEGER,
    nb_bugs_year                INTEGER,
    nb_delayed_year             INTEGER,
    nb_blocked_year             INTEGER,
    nb_commits_year             INTEGER,
    nb_mrs_year                 INTEGER,
    avg_lead_time_year          DOUBLE PRECISION,
    avg_cycle_time_year         DOUBLE PRECISION,
    on_time_rate_year           DOUBLE PRECISION,
    completion_rate_year        DOUBLE PRECISION,
    nb_tickets_jira_git_year    INTEGER,
    nb_tickets_jira_only_year   INTEGER,
    data_source_year            VARCHAR(50),
    perf_score_year             DOUBLE PRECISION,
    snapshot_date               DATE DEFAULT CURRENT_DATE,
    loaded_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_metrics.task_type_distribution (
    id              SERIAL PRIMARY KEY,
    assignee        VARCHAR(200),
    project_key     VARCHAR(50),
    issue_type      VARCHAR(100),
    nb_tickets      INTEGER,
    pct_total       DOUBLE PRECISION,
    snapshot_date   DATE DEFAULT CURRENT_DATE,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_metrics.person_score_detail (
    id                  SERIAL PRIMARY KEY,
    assignee            VARCHAR(200),
    project_key         VARCHAR(50),
    score_component     VARCHAR(100),
    score_value         DOUBLE PRECISION,
    score_max           DOUBLE PRECISION,
    description         TEXT,
    snapshot_date       DATE DEFAULT CURRENT_DATE,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 5. SCHEMA: ml_results  (modèles ML, prédictions, métriques)
-- ============================================================

CREATE TABLE IF NOT EXISTS ml_results.models (
    id              SERIAL PRIMARY KEY,
    model_id        UUID DEFAULT uuid_generate_v4() UNIQUE,
    model_name      VARCHAR(200) NOT NULL,
    model_type      VARCHAR(100),
    target          VARCHAR(200),
    features_used   TEXT[],
    hyperparameters JSONB,
    trained_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description     TEXT
);

CREATE TABLE IF NOT EXISTS ml_results.model_metrics (
    id          SERIAL PRIMARY KEY,
    model_id    UUID REFERENCES ml_results.models(model_id),
    metric_name VARCHAR(100) NOT NULL,
    metric_value DOUBLE PRECISION,
    dataset     VARCHAR(50),
    evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ml_results.predictions (
    id              SERIAL PRIMARY KEY,
    model_id        UUID REFERENCES ml_results.models(model_id),
    entity_type     VARCHAR(50),
    entity_id       VARCHAR(100),
    predicted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    prediction      JSONB,
    actual_value    DOUBLE PRECISION,
    confidence      DOUBLE PRECISION
);


-- ============================================================
-- 6. INDEX pour les performances de requête
-- ============================================================

-- cleaned
CREATE INDEX IF NOT EXISTS idx_jira_ticket_key     ON cleaned.jira_status_history(ticket_key);
CREATE INDEX IF NOT EXISTS idx_jira_assignee       ON cleaned.jira_status_history(assignee);
CREATE INDEX IF NOT EXISTS idx_commits_sha         ON cleaned.commits(sha);
CREATE INDEX IF NOT EXISTS idx_commits_ticket      ON cleaned.commits(ticket_key);
CREATE INDEX IF NOT EXISTS idx_pipelines_status    ON cleaned.pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipelines_prod      ON cleaned.pipelines(is_production);
CREATE INDEX IF NOT EXISTS idx_jobs_pipeline       ON cleaned.jobs(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_mr_ticket           ON cleaned.merge_requests(ticket_key);

-- features
CREATE INDEX IF NOT EXISTS idx_tickets_key         ON features.tickets(ticket_key);
CREATE INDEX IF NOT EXISTS idx_tickets_assignee    ON features.tickets(assignee);
CREATE INDEX IF NOT EXISTS idx_tickets_project     ON features.tickets(project_key);
CREATE INDEX IF NOT EXISTS idx_tickets_risk        ON features.tickets(risk_level);

-- dora_metrics
CREATE INDEX IF NOT EXISTS idx_kpis_snapshot       ON dora_metrics.kpis(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_weekly_week         ON dora_metrics.weekly(week);
CREATE INDEX IF NOT EXISTS idx_byproject_key       ON dora_metrics.by_project(project_key);

-- team_metrics
CREATE INDEX IF NOT EXISTS idx_person_assignee     ON team_metrics.person_dashboard(assignee);
CREATE INDEX IF NOT EXISTS idx_person_project      ON team_metrics.person_dashboard(project_key);
CREATE INDEX IF NOT EXISTS idx_team_project        ON team_metrics.team_performance(project_key);
CREATE INDEX IF NOT EXISTS idx_yearly_year         ON team_metrics.yearly_performance(year);
CREATE INDEX IF NOT EXISTS idx_yearly_assignee     ON team_metrics.yearly_performance(assignee);

-- ml_results
CREATE INDEX IF NOT EXISTS idx_pred_model          ON ml_results.predictions(model_id);
CREATE INDEX IF NOT EXISTS idx_pred_entity         ON ml_results.predictions(entity_id);
