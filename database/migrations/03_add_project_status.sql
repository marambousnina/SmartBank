-- ============================================================
--  Migration 03 : Ajout colonne status + metriques projet
--  Table : dwh.dim_project
-- ============================================================

-- Statut géré manuellement : Actif / En pause / Terminé / En attente
ALTER TABLE dwh.dim_project
    ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'Actif';

-- Dates de début et fin planifiées
ALTER TABLE dwh.dim_project
    ADD COLUMN IF NOT EXISTS start_date DATE;

ALTER TABLE dwh.dim_project
    ADD COLUMN IF NOT EXISTS end_date DATE;

-- Description courte
ALTER TABLE dwh.dim_project
    ADD COLUMN IF NOT EXISTS description TEXT;

-- Initialise le statut selon la progression (peut être surchargé manuellement)
UPDATE dwh.dim_project dp
SET status = CASE
    WHEN (
        SELECT COALESCE(
            COUNT(CASE WHEN ds.status_category = 'Done' THEN 1 END) * 100.0
            / NULLIF(COUNT(ft.ticket_fact_key), 0),
            0
        )
        FROM dwh.fact_tickets ft
        JOIN dwh.dim_status ds ON ft.status_key = ds.status_key
        WHERE ft.project_key = dp.project_key
    ) >= 75 THEN 'Terminé'
    WHEN (
        SELECT COALESCE(
            COUNT(CASE WHEN ds.status_category = 'Done' THEN 1 END) * 100.0
            / NULLIF(COUNT(ft.ticket_fact_key), 0),
            0
        )
        FROM dwh.fact_tickets ft
        JOIN dwh.dim_status ds ON ft.status_key = ds.status_key
        WHERE ft.project_key = dp.project_key
    ) >= 30 THEN 'Actif'
    ELSE 'En pause'
END
WHERE status = 'Actif';
