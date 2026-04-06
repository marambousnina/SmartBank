"""
=============================================================
 DORA Metrics Project
 Script  : check_data.py
 Utilitaire : Vérification de la qualité des données générées
=============================================================
Rôle : Vérifier que tous les fichiers CSV générés sont
       corrects, cohérents et utiles pour Power BI.
Lance : python scripts/check_data.py
=============================================================
"""

import pandas as pd
import os

# ── Couleurs console ──────────────────────────────────────────
OK   = "✅"
WARN = "⚠️ "
ERR  = "❌"

def check(condition, msg_ok, msg_fail):
    if condition:
        print(f"      {OK}  {msg_ok}")
    else:
        print(f"      {WARN} {msg_fail}")

# ── Fichiers à vérifier ───────────────────────────────────────
FILES = {

    # ── Script 01 ──────────────────────────────────────────
    "data/cleaned/jira_cleaned_spark.csv": {
        "page":    "Source → Script 02",
        "min_rows": 100,
        "check_cols": ["TicketKey", "Status", "Assignee", "Created"],
        "check_values": {"Status": ["To Be Deployed", "In Process", "Blocked"]},
    },
    "data/cleaned/commits_cleaned_spark.csv": {
        "page":    "Source → Script 04",
        "min_rows": 100,
        "check_cols": ["sha", "author", "date"],
        "check_values": {},
    },
    "data/cleaned/pipelines_cleaned_spark.csv": {
        "page":    "Source → Script 03",
        "min_rows": 100,
        "check_cols": ["pipeline_id", "status", "is_production"],
        "check_values": {"status": ["success", "failed"]},
    },

    # ── Script 02 ──────────────────────────────────────────
    "data/features/tickets_features_spark.csv": {
        "page":    "Tous les dashboards",
        "min_rows": 50,
        "check_cols": [
            "TicketKey", "Assignee", "ProjectKey",
            "lead_time_hours", "cycle_time_hours",
            "is_delayed", "was_blocked", "risk_level",
            "month_year", "completion_rate"
        ],
        "check_values": {"risk_level": ["Low", "Medium", "High"]},
        "check_nulls": ["lead_time_hours"],
    },
    "data/features/assignee_metrics_spark.csv": {
        "page":    "Page Personne + Équipes",
        "min_rows": 5,
        "check_cols": [
            "Assignee", "nb_tickets_assigned",
            "nb_tickets_deployed", "on_time_rate"
        ],
        "check_values": {},
    },
    "data/features/project_metrics_spark.csv": {
        "page":    "Page Projets",
        "min_rows": 2,
        "check_cols": [
            "ProjectKey", "nb_tickets_total",
            "nb_tickets_done", "completion_rate"
        ],
        "check_values": {},
    },

    # ── Script 03 ──────────────────────────────────────────
    "data/metrics/dora_metrics_summary_spark.csv": {
        "page":    "Vue Executive — résumé DORA",
        "min_rows": 4,
        "check_cols": ["metric", "definition", "value", "dora_level"],
        "check_values": {},
    },
    "data/metrics/dora_metrics_weekly_spark.csv": {
        "page":    "Vue Executive — graphique tendance",
        "min_rows": 10,
        "check_cols": [
            "week", "deployment_count",
            "change_failure_rate", "median_lead_time_hours"
        ],
        "check_values": {},
    },
    "data/metrics/dora_kpis_spark.csv": {
        "page":    "Vue Executive — KPI cards",
        "min_rows": 1,
        "check_cols": [
            "total_tickets", "total_commits",
            "global_lead_time_h", "global_cfr_pct"
        ],
        "check_values": {},
    },
    "data/metrics/dora_by_project_spark.csv": {
        "page":    "Projets — filtre Power BI",
        "min_rows": 2,
        "check_cols": ["ProjectKey", "median_lead_time_h", "delay_rate_pct"],
        "check_values": {},
    },

    # ── Script 04 ──────────────────────────────────────────
    "data/team/assignee_performance_spark.csv": {
        "page":    "Page Personne",
        "min_rows": 5,
        "check_cols": [
            "Assignee", "ProjectKey",
            "nb_tickets_assigned", "avg_lead_time_hours",
            "on_time_rate", "performance_score"
        ],
        "check_values": {},
    },
    "data/team/team_performance_spark.csv": {
        "page":    "Page Équipes",
        "min_rows": 2,
        "check_cols": [
            "ProjectKey", "nb_members",
            "team_lead_time_median", "team_on_time_rate",
            "team_velocity_per_week"
        ],
        "check_values": {},
    },
    "data/team/sprint_metrics_spark.csv": {
        "page":    "Page Sprint",
        "min_rows": 2,
        "check_cols": [
            "ProjectKey", "SprintState",
            "sprint_velocity", "sprint_completion_rate"
        ],
        "check_values": {},
    },
    "data/team/person_vs_team_spark.csv": {
        "page":    "Page Personne — comparaison",
        "min_rows": 5,
        "check_cols": [
            "Assignee", "avg_lead_time_hours",
            "lead_time_vs_team", "on_time_vs_team"
        ],
        "check_values": {},
    },
    "data/team/monthly_activity_spark.csv": {
        "page":    "Page Personne — tendance",
        "min_rows": 10,
        "check_cols": [
            "Assignee", "month_year",
            "nb_tickets_month", "avg_lead_time_month"
        ],
        "check_values": {},
    },
}

# ══════════════════════════════════════════════════════════════
# VÉRIFICATION
# ══════════════════════════════════════════════════════════════
print()
print("=" * 65)
print("  RAPPORT DE VÉRIFICATION DES DONNÉES")
print("=" * 65)

total = 0
ok    = 0
warn  = 0
missing = 0

for filepath, config in FILES.items():
    total += 1
    print()
    print(f"  📄 {filepath}")
    print(f"     Page : {config['page']}")

    # Existence
    if not os.path.exists(filepath):
        print(f"      {ERR}  FICHIER MANQUANT — lance le script correspondant")
        missing += 1
        continue

    # Chargement
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"      {ERR}  Erreur lecture : {e}")
        missing += 1
        continue

    file_ok = True

    # Nb lignes
    nb = len(df)
    if nb >= config["min_rows"]:
        print(f"      {OK}  {nb} lignes  (min attendu : {config['min_rows']})")
    else:
        print(f"      {WARN} Seulement {nb} lignes (min attendu : {config['min_rows']})")
        file_ok = False

    # Colonnes attendues
    missing_cols = [c for c in config["check_cols"] if c not in df.columns]
    if not missing_cols:
        print(f"      {OK}  Toutes les colonnes présentes")
    else:
        print(f"      {WARN} Colonnes manquantes : {missing_cols}")
        file_ok = False

    # Valeurs non nulles dans colonnes clés
    for col in config.get("check_nulls", []):
        if col in df.columns:
            non_null = df[col].notna().sum()
            pct = round(non_null / nb * 100, 1)
            if pct > 0:
                print(f"      {OK}  {col} : {non_null} valeurs non-null ({pct}%)")
            else:
                print(f"      {WARN} {col} : 100% de valeurs NULL")
                file_ok = False

    # Valeurs attendues dans colonnes catégorielles
    for col, expected_vals in config.get("check_values", {}).items():
        if col in df.columns:
            actual_vals = df[col].dropna().unique().tolist()
            found = [v for v in expected_vals if v in actual_vals]
            if found:
                print(f"      {OK}  {col} contient : {found}")
            else:
                print(f"      {WARN} {col} : valeurs attendues {expected_vals} non trouvées")

    # Aperçu rapide
    print(f"      → Colonnes : {list(df.columns)[:6]}{'...' if len(df.columns) > 6 else ''}")

    if file_ok:
        ok += 1
    else:
        warn += 1

# ── Résumé ────────────────────────────────────────────────────
print()
print("=" * 65)
print(f"  RÉSUMÉ : {ok}/{total} fichiers OK  |  {warn} warnings  |  {missing} manquants")
print("=" * 65)

if missing > 0:
    print()
    print("  Fichiers manquants → scripts à lancer :")
    for filepath, config in FILES.items():
        if not os.path.exists(filepath):
            script = "01" if "cleaned" in filepath else \
                     "02" if "features" in filepath else \
                     "03" if "metrics" in filepath else "04"
            print(f"    python scripts/0{script}_*_spark.py  →  {filepath}")
print()