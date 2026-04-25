-- ============================================================
--  Migration 04 : Correction des colonnes manquantes
--  Cause : le CSV Spark génère des colonnes supplémentaires
--  non présentes dans le schéma initial → INSERT échoue
-- ============================================================

-- ── dora_metrics.by_project ─────────────────────────────────
-- Métriques DORA calculées par projet depuis les pipelines Git
ALTER TABLE dora_metrics.by_project
    ADD COLUMN IF NOT EXISTS deploy_freq_per_week DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cfr_pct              DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS mttr_hours           DOUBLE PRECISION;

-- ── features.assignee_metrics ───────────────────────────────
ALTER TABLE features.assignee_metrics
    ADD COLUMN IF NOT EXISTS snapshot_date DATE DEFAULT CURRENT_DATE;

-- ── features.project_metrics ────────────────────────────────
ALTER TABLE features.project_metrics
    ADD COLUMN IF NOT EXISTS snapshot_date DATE DEFAULT CURRENT_DATE;

-- ── features.personnel ──────────────────────────────────────
ALTER TABLE features.personnel
    ADD COLUMN IF NOT EXISTS snapshot_date DATE DEFAULT CURRENT_DATE;

-- ── features.email_person_map ───────────────────────────────
ALTER TABLE features.email_person_map
    ADD COLUMN IF NOT EXISTS snapshot_date DATE DEFAULT CURRENT_DATE;

-- ── features.tickets ────────────────────────────────────────
-- Le CSV features/tickets_features_spark.csv ajoute des colonnes
ALTER TABLE features.tickets
    ADD COLUMN IF NOT EXISTS snapshot_date      DATE DEFAULT CURRENT_DATE,
    ADD COLUMN IF NOT EXISTS assignee_email     VARCHAR(200),
    ADD COLUMN IF NOT EXISTS project_key_alt    VARCHAR(50);
