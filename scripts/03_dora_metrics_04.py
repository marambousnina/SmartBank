"""
03_dora_metrics_04.py
----------------------
Génère 4 fichiers de métriques DORA temporelles :
  - dora_project_weekly.csv   : DORA par projet × semaine
  - dora_project_monthly.csv  : DORA par projet × mois
  - dora_person_weekly.csv    : DORA individuel × semaine
  - dora_person_monthly.csv   : DORA individuel × mois

Gestion des phases projet :
  Phase 1 - Jira only   : git_coverage = 0%  → Lead Time + performance Jira uniquement
  Phase 2 - Transition  : git_coverage < 50% → toutes métriques, partiel Git
  Phase 2 - Jira+Git    : git_coverage ≥ 50% → toutes métriques DORA complètes

Source des données :
  tickets_features_spark.csv  ← features Jira (lead_time, cycle_time, bugs, etc.)
  commits_cleaned_spark.csv   ← commits Git (email, date, TicketKey)
  merge_requests_cleaned_spark.csv ← MRs (state, merge_time, TicketKey)
  pipelines_cleaned_spark.csv ← déploiements production (is_production, status)
"""

import os
import sys
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

# ── Spark ──────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("DORA_Temporal_Metrics") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR")

FEATURES = "data/features/"
CLEANED  = "data/cleaned/"
METRICS  = "data/metrics/"
os.makedirs(METRICS, exist_ok=True)

CALC_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("=" * 60)
print("  Script 03_04 — DORA Métriques Temporelles")
print("=" * 60)

# ══════════════════════════════════════════════════════════════
# 1. CHARGEMENT
# ══════════════════════════════════════════════════════════════
tickets = spark.read.csv(
    FEATURES + "tickets_features_spark.csv", header=True, inferSchema=False)
commits = spark.read.csv(
    CLEANED  + "commits_cleaned_spark.csv",  header=True, inferSchema=False)
mrs = spark.read.csv(
    CLEANED  + "merge_requests_cleaned_spark.csv", header=True, inferSchema=False)
pipelines = spark.read.csv(
    CLEANED  + "pipelines_cleaned_spark.csv", header=True, inferSchema=False)

# Cast numériques
tickets = tickets \
    .withColumn("lead_time_hours",      F.col("lead_time_hours").cast(DoubleType())) \
    .withColumn("cycle_time_hours",     F.col("cycle_time_hours").cast(DoubleType())) \
    .withColumn("nb_commits",           F.col("nb_commits").cast(DoubleType())) \
    .withColumn("nb_mrs",               F.col("nb_mrs").cast(DoubleType())) \
    .withColumn("avg_merge_time_hours", F.col("avg_merge_time_hours").cast(DoubleType())) \
    .withColumn("is_delayed",           F.col("is_delayed").cast(DoubleType())) \
    .withColumn("is_bug",               F.col("is_bug").cast(DoubleType()))

mrs = mrs.withColumn("merge_time_hours", F.col("merge_time_hours").cast(DoubleType()))
pipelines = pipelines.withColumn("duration_minutes", F.col("duration_minutes").cast(DoubleType()))

print(f"   Tickets   : {tickets.count()}")
print(f"   Commits   : {commits.count()}")
print(f"   MRs       : {mrs.count()}")
print(f"   Pipelines : {pipelines.count()}")

# ══════════════════════════════════════════════════════════════
# 2. PHASE PAR PROJET (recalculé depuis tickets)
# ══════════════════════════════════════════════════════════════
proj_phase = tickets.groupBy("ProjectKey").agg(
    F.round(
        F.sum(F.when(F.col("nb_commits") > 0, 1).otherwise(0)).cast(DoubleType()) /
        F.count("TicketKey") * 100, 1
    ).alias("git_coverage_pct")
).withColumn(
    "data_source",
    F.when(F.col("git_coverage_pct") == 0, "jira_only").otherwise("jira+git")
).withColumn(
    "project_phase",
    F.when(F.col("git_coverage_pct") == 0,  "Phase 1 - Jira only")
     .when(F.col("git_coverage_pct") < 50,  "Phase 2 - Transition")
     .otherwise(                             "Phase 2 - Jira+Git")
)

# ══════════════════════════════════════════════════════════════
# 3. ENRICHISSEMENT DES SOURCES GIT AVEC ProjectKey
# ══════════════════════════════════════════════════════════════
PROJ_PATTERN = r'([A-Z]{2,8})-\d+'

def extract_proj(col_expr):
    """Extrait le ProjectKey via regexp_extract — 100% natif Spark, sans UDF Python."""
    raw = F.regexp_extract(col_expr, PROJ_PATTERN, 1)
    return F.when(raw != "", raw).otherwise(F.lit(None))

# Commits : ProjectKey depuis TicketKey, sinon depuis le titre du commit
commits = commits.withColumn(
    "ProjectKey",
    F.coalesce(
        F.when(F.col("TicketKey").isNotNull() & (F.col("TicketKey") != ""),
               extract_proj(F.col("TicketKey"))),
        extract_proj(F.col("title"))
    )
)

# MRs : ProjectKey depuis TicketKey, sinon depuis le titre de la MR
mrs = mrs.withColumn(
    "ProjectKey",
    F.coalesce(
        F.when(F.col("TicketKey").isNotNull() & (F.col("TicketKey") != ""),
               extract_proj(F.col("TicketKey"))),
        extract_proj(F.col("title"))
    )
)

# Pipelines : ProjectKey depuis le champ ref ("Feat/MBC-366-..." → "MBC")
pipelines = pipelines.withColumn("ProjectKey", extract_proj(F.col("ref")))

# Mapping author_id → email (depuis commits, pour enrichir les MRs)
author_email = commits.select("author", "email") \
    .filter(F.col("email").isNotNull()) \
    .dropDuplicates(["author"])

mrs = mrs.join(author_email, on="author", how="left")

# ══════════════════════════════════════════════════════════════
# 4. UTILITAIRE : COLONNES DE PÉRIODE (SEMAINE + MOIS)
# ══════════════════════════════════════════════════════════════
def add_period_cols(df, date_col):
    """Ajoute year_week, week_start/end, year_month, month_start/end."""
    ts = F.to_timestamp(F.regexp_replace(F.col(date_col), r'[+-]\d{2}:\d{2}$|Z$', ''))
    df = df.withColumn("_ts", ts)
    # Semaine
    df = df \
        .withColumn("year_week",
            F.concat(F.year("_ts").cast("string"), F.lit("-W"),
                     F.lpad(F.weekofyear("_ts").cast("string"), 2, "0"))) \
        .withColumn("week_start", F.date_trunc("week", "_ts").cast("date")) \
        .withColumn("week_end",   F.date_add(F.date_trunc("week", "_ts").cast("date"), 6))
    # Mois
    df = df \
        .withColumn("year_month",
            F.concat(F.year("_ts").cast("string"), F.lit("-"),
                     F.lpad(F.month("_ts").cast("string"), 2, "0"))) \
        .withColumn("month_start", F.date_trunc("month", "_ts").cast("date")) \
        .withColumn("month_end",   F.last_day("_ts").cast("date"))
    return df.drop("_ts")

tickets_t   = add_period_cols(tickets,   "Created")
commits_t   = add_period_cols(commits,   "date")
pipelines_t = add_period_cols(pipelines, "created_at")
mrs_merged  = add_period_cols(mrs.filter(F.col("state") == "merged"), "merged_at")

# Tickets résolus (pour throughput et bugs_fixed)
tickets_res = add_period_cols(
    tickets.filter(F.col("ResolutionDate").isNotNull() & (F.col("ResolutionDate") != "")),
    "ResolutionDate"
).withColumnRenamed("year_week",   "res_year_week") \
 .withColumnRenamed("week_start",  "res_week_start") \
 .withColumnRenamed("week_end",    "res_week_end") \
 .withColumnRenamed("year_month",  "res_year_month") \
 .withColumnRenamed("month_start", "res_month_start") \
 .withColumnRenamed("month_end",   "res_month_end")


# ══════════════════════════════════════════════════════════════
# FONCTIONS D'AGRÉGATION RÉUTILISABLES
# ══════════════════════════════════════════════════════════════
def apply_phase_filter(df):
    """Nullifie les métriques Git pour les projets Phase 1."""
    return df \
        .withColumn("deployment_frequency",
            F.when(F.col("data_source") == "jira_only", F.lit(None).cast(DoubleType()))
             .otherwise(F.col("deployment_frequency"))) \
        .withColumn("change_failure_rate",
            F.when(F.col("data_source") == "jira_only", F.lit(None).cast(DoubleType()))
             .otherwise(F.col("change_failure_rate"))) \
        .withColumn("mttr",
            F.when(F.col("data_source") == "jira_only", F.lit(None).cast(DoubleType()))
             .otherwise(F.col("mttr")))


# ══════════════════════════════════════════════════════════════
# 5. DORA PROJECT WEEKLY
# ══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("1. DORA Project Weekly")
print("=" * 60)

# Jira : métriques par projet × semaine (basé sur Created)
proj_jira_w = tickets_t.groupBy(
    "ProjectKey", "year_week", "week_start", "week_end"
).agg(
    F.round(F.avg("lead_time_hours"),  2).alias("lead_time"),
    F.round(F.avg("cycle_time_hours"), 2).alias("cycle_time"),
    F.count("TicketKey").alias("velocity"),
    F.sum(F.when(~F.col("CurrentStatus").isin("To Be Deployed", "Done"), 1)
           .otherwise(0)).alias("wip"),
    F.round(F.sum("is_bug") / F.count("TicketKey") * 100, 1).alias("bug_rate"),
)

# Throughput : tickets terminés par semaine (basé sur ResolutionDate)
throughput_w = tickets_res.groupBy("ProjectKey", "res_year_week").agg(
    F.count("TicketKey").alias("throughput")
).withColumnRenamed("res_year_week", "year_week")

# Pipelines : deployment_frequency, CFR, MTTR par projet × semaine
pipe_w = pipelines_t.filter(F.col("ProjectKey").isNotNull()) \
    .groupBy("ProjectKey", "year_week").agg(
        F.sum(F.when(F.col("is_production").isin("true", "True"), 1)
               .otherwise(0)).alias("deployment_frequency"),
        F.round(
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).cast(DoubleType()) /
            F.count("pipeline_id") * 100, 2
        ).alias("change_failure_rate"),
        F.round(F.avg("duration_minutes") / 60, 4).alias("mttr"),
    )

# Assemblage
proj_week = proj_jira_w \
    .join(throughput_w, on=["ProjectKey", "year_week"], how="left") \
    .join(pipe_w,       on=["ProjectKey", "year_week"], how="left") \
    .join(proj_phase,   on="ProjectKey",                how="left")

proj_week = apply_phase_filter(proj_week) \
    .withColumn("throughput",            F.coalesce(F.col("throughput"), F.lit(0))) \
    .withColumn("code_coverage",         F.lit(None).cast(DoubleType())) \
    .withColumn("calculation_timestamp", F.lit(CALC_TS)) \
    .withColumn("id", F.concat_ws("_", F.col("ProjectKey"), F.col("year_week"))) \
    .withColumnRenamed("ProjectKey",  "project_id") \
    .withColumnRenamed("week_start",  "metrics_period_start") \
    .withColumnRenamed("week_end",    "metrics_period_end")

proj_week_out = proj_week.select(
    "id", "project_id", "calculation_timestamp",
    "metrics_period_start", "metrics_period_end", "year_week",
    "lead_time", "deployment_frequency", "change_failure_rate", "mttr",
    "cycle_time", "throughput", "wip", "bug_rate", "velocity",
    "code_coverage", "data_source", "project_phase", "git_coverage_pct"
).orderBy("project_id", "year_week")

proj_week_pd = proj_week_out.toPandas()
proj_week_pd.to_csv(METRICS + "dora_project_weekly.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ dora_project_weekly.csv ({len(proj_week_pd)} lignes)")

# ══════════════════════════════════════════════════════════════
# 6. DORA PROJECT MONTHLY
# ══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("2. DORA Project Monthly")
print("=" * 60)

proj_jira_m = tickets_t.groupBy(
    "ProjectKey", "year_month", "month_start", "month_end"
).agg(
    F.round(F.avg("lead_time_hours"),  2).alias("lead_time"),
    F.round(F.avg("cycle_time_hours"), 2).alias("cycle_time"),
    F.count("TicketKey").alias("velocity"),
    F.sum(F.when(~F.col("CurrentStatus").isin("To Be Deployed", "Done"), 1)
           .otherwise(0)).alias("wip"),
    F.round(F.sum("is_bug") / F.count("TicketKey") * 100, 1).alias("bug_rate"),
)

throughput_m = tickets_res.groupBy("ProjectKey", "res_year_month").agg(
    F.count("TicketKey").alias("throughput")
).withColumnRenamed("res_year_month", "year_month")

pipe_m = pipelines_t.filter(F.col("ProjectKey").isNotNull()) \
    .groupBy("ProjectKey", "year_month").agg(
        F.sum(F.when(F.col("is_production").isin("true", "True"), 1)
               .otherwise(0)).alias("deployment_frequency"),
        F.round(
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).cast(DoubleType()) /
            F.count("pipeline_id") * 100, 2
        ).alias("change_failure_rate"),
        F.round(F.avg("duration_minutes") / 60, 4).alias("mttr"),
    )

proj_month = proj_jira_m \
    .join(throughput_m, on=["ProjectKey", "year_month"], how="left") \
    .join(pipe_m,       on=["ProjectKey", "year_month"], how="left") \
    .join(proj_phase,   on="ProjectKey",                 how="left")

proj_month = apply_phase_filter(proj_month) \
    .withColumn("throughput",            F.coalesce(F.col("throughput"), F.lit(0))) \
    .withColumn("code_coverage",         F.lit(None).cast(DoubleType())) \
    .withColumn("calculation_timestamp", F.lit(CALC_TS)) \
    .withColumn("id", F.concat_ws("_", F.col("ProjectKey"), F.col("year_month"))) \
    .withColumnRenamed("ProjectKey",   "project_id") \
    .withColumnRenamed("month_start",  "metrics_period_start") \
    .withColumnRenamed("month_end",    "metrics_period_end")

proj_month_out = proj_month.select(
    "id", "project_id", "calculation_timestamp",
    "metrics_period_start", "metrics_period_end", "year_month",
    "lead_time", "deployment_frequency", "change_failure_rate", "mttr",
    "cycle_time", "throughput", "wip", "bug_rate", "velocity",
    "code_coverage", "data_source", "project_phase", "git_coverage_pct"
).orderBy("project_id", "year_month")

proj_month_pd = proj_month_out.toPandas()
proj_month_pd.to_csv(METRICS + "dora_project_monthly.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ dora_project_monthly.csv ({len(proj_month_pd)} lignes)")

# ══════════════════════════════════════════════════════════════
# 7. DORA PERSON WEEKLY
# ══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("3. DORA Person Weekly")
print("=" * 60)

# Commits par email × semaine
commits_pw = commits_t \
    .filter(F.col("email").isNotNull()) \
    .groupBy("email", "year_week").agg(
        F.count("sha").alias("commit_count")
    )

# MRs mergées par email × semaine  (lead_time = avg merge_time)
mrs_pw = mrs_merged \
    .filter(F.col("email").isNotNull()) \
    .groupBy("email", "year_week").agg(
        F.count("mr_id").alias("prs_merged"),
        F.round(F.avg("merge_time_hours"), 2).alias("mr_lead_time"),
    )

# Jira : tâches par personne × projet × semaine (based on Created)
person_jira_w = tickets_t \
    .filter(F.col("AssigneeEmail").isNotNull() & (F.col("AssigneeEmail") != "")) \
    .groupBy("AssigneeEmail", "Assignee", "ProjectKey", "year_week", "week_start", "week_end") \
    .agg(
        F.sum(F.when(F.col("CurrentStatus").isin("To Be Deployed", "Done"), 1)
               .otherwise(0)).alias("tasks_completed"),
        F.count("TicketKey").alias("tasks_total"),
        F.sum("is_bug").alias("bugs_created"),
        F.round(F.avg("cycle_time_hours"), 2).alias("cycle_time"),
        F.round(F.avg("lead_time_hours"),  2).alias("jira_lead_time"),
        F.round(F.sum("is_bug") / F.count("TicketKey") * 100, 1).alias("change_failure_rate"),
    )

# Bugs corrigés par personne × semaine (basé sur ResolutionDate)
bugs_fixed_w = tickets_res \
    .filter((F.col("is_bug") == 1) & F.col("AssigneeEmail").isNotNull()) \
    .groupBy("AssigneeEmail", "ProjectKey", "res_year_week") \
    .agg(
        F.count("TicketKey").alias("bugs_fixed"),
        F.round(F.avg("cycle_time_hours"), 2).alias("mttr"),
    ).withColumnRenamed("res_year_week", "year_week")

# Assemblage personne weekly
person_week = person_jira_w \
    .join(
        commits_pw.withColumnRenamed("email", "_c_email")
                  .withColumnRenamed("year_week", "_c_yw"),
        (F.col("AssigneeEmail") == F.col("_c_email")) &
        (F.col("year_week")     == F.col("_c_yw")),
        "left"
    ).drop("_c_email", "_c_yw") \
    .join(
        mrs_pw.withColumnRenamed("email", "_m_email")
              .withColumnRenamed("year_week", "_m_yw"),
        (F.col("AssigneeEmail") == F.col("_m_email")) &
        (F.col("year_week")     == F.col("_m_yw")),
        "left"
    ).drop("_m_email", "_m_yw") \
    .join(bugs_fixed_w, on=["AssigneeEmail", "ProjectKey", "year_week"], how="left") \
    .join(proj_phase,   on="ProjectKey", how="left")

# lead_time = MR merge time si dispo, sinon lead_time Jira
person_week = person_week \
    .withColumn("lead_time",
        F.coalesce(F.col("mr_lead_time"), F.col("jira_lead_time"))) \
    .withColumn("commit_count", F.coalesce(F.col("commit_count"), F.lit(0))) \
    .withColumn("prs_merged",   F.coalesce(F.col("prs_merged"),   F.lit(0))) \
    .withColumn("bugs_fixed",   F.coalesce(F.col("bugs_fixed"),   F.lit(0))) \
    .withColumn("mttr",         F.coalesce(F.col("mttr"),         F.lit(0.0))) \
    .withColumn("person_data_source",
        F.when(F.col("commit_count") > 0, "jira+git").otherwise("jira_only")) \
    .withColumn("productivity_score", F.round(
        F.col("tasks_completed").cast(DoubleType()) * 3.0 +
        F.col("prs_merged").cast(DoubleType())      * 2.0 +
        F.least(F.col("commit_count").cast(DoubleType()), F.lit(50.0)) * 0.3 -
        F.col("bugs_created").cast(DoubleType())    * 2.0,
        2
    )) \
    .withColumn("calculation_timestamp", F.lit(CALC_TS)) \
    .withColumn("id", F.concat_ws("_", F.col("AssigneeEmail"), F.col("ProjectKey"), F.col("year_week"))) \
    .drop("data_source") \
    .withColumnRenamed("AssigneeEmail",      "person_id") \
    .withColumnRenamed("Assignee",           "person_name") \
    .withColumnRenamed("ProjectKey",         "project_id") \
    .withColumnRenamed("week_start",         "metrics_period_start") \
    .withColumnRenamed("week_end",           "metrics_period_end") \
    .withColumnRenamed("person_data_source", "data_source")

person_week_out = person_week.select(
    "id", "person_id", "person_name", "project_id",
    "calculation_timestamp", "metrics_period_start", "metrics_period_end", "year_week",
    "lead_time", "change_failure_rate", "mttr",
    "tasks_completed", "bugs_created", "bugs_fixed",
    "commit_count", "prs_merged", "cycle_time", "productivity_score",
    "data_source", "project_phase", "git_coverage_pct"
).orderBy("person_id", "project_id", "year_week")

person_week_pd = person_week_out.toPandas()
person_week_pd.to_csv(METRICS + "dora_person_weekly.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ dora_person_weekly.csv ({len(person_week_pd)} lignes)")

# ══════════════════════════════════════════════════════════════
# 8. DORA PERSON MONTHLY
# ══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("4. DORA Person Monthly")
print("=" * 60)

commits_pm = commits_t \
    .filter(F.col("email").isNotNull()) \
    .groupBy("email", "year_month").agg(
        F.count("sha").alias("commit_count")
    )

mrs_pm = mrs_merged \
    .filter(F.col("email").isNotNull()) \
    .groupBy("email", "year_month").agg(
        F.count("mr_id").alias("prs_merged"),
        F.round(F.avg("merge_time_hours"), 2).alias("mr_lead_time"),
    )

person_jira_m = tickets_t \
    .filter(F.col("AssigneeEmail").isNotNull() & (F.col("AssigneeEmail") != "")) \
    .groupBy("AssigneeEmail", "Assignee", "ProjectKey", "year_month", "month_start", "month_end") \
    .agg(
        F.sum(F.when(F.col("CurrentStatus").isin("To Be Deployed", "Done"), 1)
               .otherwise(0)).alias("tasks_completed"),
        F.count("TicketKey").alias("tasks_total"),
        F.sum("is_bug").alias("bugs_created"),
        F.round(F.avg("cycle_time_hours"), 2).alias("cycle_time"),
        F.round(F.avg("lead_time_hours"),  2).alias("jira_lead_time"),
        F.round(F.sum("is_bug") / F.count("TicketKey") * 100, 1).alias("change_failure_rate"),
    )

bugs_fixed_m = tickets_res \
    .filter((F.col("is_bug") == 1) & F.col("AssigneeEmail").isNotNull()) \
    .groupBy("AssigneeEmail", "ProjectKey", "res_year_month") \
    .agg(
        F.count("TicketKey").alias("bugs_fixed"),
        F.round(F.avg("cycle_time_hours"), 2).alias("mttr"),
    ).withColumnRenamed("res_year_month", "year_month")

person_month = person_jira_m \
    .join(
        commits_pm.withColumnRenamed("email", "_c_email")
                  .withColumnRenamed("year_month", "_c_ym"),
        (F.col("AssigneeEmail") == F.col("_c_email")) &
        (F.col("year_month")    == F.col("_c_ym")),
        "left"
    ).drop("_c_email", "_c_ym") \
    .join(
        mrs_pm.withColumnRenamed("email", "_m_email")
              .withColumnRenamed("year_month", "_m_ym"),
        (F.col("AssigneeEmail") == F.col("_m_email")) &
        (F.col("year_month")    == F.col("_m_ym")),
        "left"
    ).drop("_m_email", "_m_ym") \
    .join(bugs_fixed_m, on=["AssigneeEmail", "ProjectKey", "year_month"], how="left") \
    .join(proj_phase,   on="ProjectKey", how="left")

person_month = person_month \
    .withColumn("lead_time",
        F.coalesce(F.col("mr_lead_time"), F.col("jira_lead_time"))) \
    .withColumn("commit_count", F.coalesce(F.col("commit_count"), F.lit(0))) \
    .withColumn("prs_merged",   F.coalesce(F.col("prs_merged"),   F.lit(0))) \
    .withColumn("bugs_fixed",   F.coalesce(F.col("bugs_fixed"),   F.lit(0))) \
    .withColumn("mttr",         F.coalesce(F.col("mttr"),         F.lit(0.0))) \
    .withColumn("person_data_source",
        F.when(F.col("commit_count") > 0, "jira+git").otherwise("jira_only")) \
    .withColumn("productivity_score", F.round(
        F.col("tasks_completed").cast(DoubleType()) * 3.0 +
        F.col("prs_merged").cast(DoubleType())      * 2.0 +
        F.least(F.col("commit_count").cast(DoubleType()), F.lit(50.0)) * 0.3 -
        F.col("bugs_created").cast(DoubleType())    * 2.0,
        2
    )) \
    .withColumn("calculation_timestamp", F.lit(CALC_TS)) \
    .withColumn("id", F.concat_ws("_", F.col("AssigneeEmail"), F.col("ProjectKey"), F.col("year_month"))) \
    .drop("data_source") \
    .withColumnRenamed("AssigneeEmail",      "person_id") \
    .withColumnRenamed("Assignee",           "person_name") \
    .withColumnRenamed("ProjectKey",         "project_id") \
    .withColumnRenamed("month_start",        "metrics_period_start") \
    .withColumnRenamed("month_end",          "metrics_period_end") \
    .withColumnRenamed("person_data_source", "data_source")

person_month_out = person_month.select(
    "id", "person_id", "person_name", "project_id",
    "calculation_timestamp", "metrics_period_start", "metrics_period_end", "year_month",
    "lead_time", "change_failure_rate", "mttr",
    "tasks_completed", "bugs_created", "bugs_fixed",
    "commit_count", "prs_merged", "cycle_time", "productivity_score",
    "data_source", "project_phase", "git_coverage_pct"
).orderBy("person_id", "project_id", "year_month")

person_month_pd = person_month_out.toPandas()
person_month_pd.to_csv(METRICS + "dora_person_monthly.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ dora_person_monthly.csv ({len(person_month_pd)} lignes)")

# ══════════════════════════════════════════════════════════════
# 9. RÉSUMÉ
# ══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("RÉSUMÉ — MÉTRIQUES TEMPORELLES DORA")
print("=" * 60)
print(f"   dora_project_weekly.csv   : {len(proj_week_pd):>5} lignes  ✅")
print(f"   dora_project_monthly.csv  : {len(proj_month_pd):>5} lignes  ✅")
print(f"   dora_person_weekly.csv    : {len(person_week_pd):>5} lignes  ✅")
print(f"   dora_person_monthly.csv   : {len(person_month_pd):>5} lignes  ✅")
print()
print("✅ Script 03_dora_metrics_04 terminé.")

spark.stop()
