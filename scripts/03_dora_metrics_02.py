"""
=============================================================
 DORA Metrics Project — VERSION BIG DATA
 Script  : 04_bis_person_metrics_spark.py
 Étape   : 4 bis — Métriques détaillées par Personne
=============================================================
"""

import os
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-java-options "
    "-Djava.security.manager=allow "
    "pyspark-shell"
)

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("DORA_Person_Metrics") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

FEATURES = "data/features/"
CLEANED  = "data/cleaned/"
TEAM     = "data/team/"
os.makedirs(TEAM, exist_ok=True)

print("=" * 60)
print("Script 04 bis — Métriques détaillées par Personne")
print("=" * 60)

tickets = spark.read.csv(
    FEATURES + "tickets_features_spark.csv",
    header=True, inferSchema=False
)

# Chargement des commits pour les développeurs GitLab-only
commits_raw = spark.read.csv(
    CLEANED + "commits_cleaned_spark.csv",
    header=True, inferSchema=False
)
mrs_raw = spark.read.csv(
    CLEANED + "merge_requests_cleaned_spark.csv",
    header=True, inferSchema=False
)

tickets = tickets \
    .withColumn("lead_time_hours",    F.col("lead_time_hours").cast(DoubleType())) \
    .withColumn("cycle_time_hours",   F.col("cycle_time_hours").cast(DoubleType())) \
    .withColumn("time_blocked_hours", F.col("time_blocked_hours").cast(DoubleType())) \
    .withColumn("nb_commits",         F.col("nb_commits").cast(IntegerType())) \
    .withColumn("nb_mrs",             F.col("nb_mrs").cast(IntegerType())) \
    .withColumn("is_delayed",         F.col("is_delayed").cast(IntegerType())) \
    .withColumn("was_blocked",        F.col("was_blocked").cast(IntegerType())) \
    .withColumn("nb_status_changes",  F.col("nb_status_changes").cast(IntegerType()))

tickets = tickets.withColumn(
    "IssueType_norm",
    F.when(F.col("IssueType").isin("Task","Tâche","Sous-tâche"), "Task")
     .when(F.col("IssueType") == "Bug",   "Bug")
     .when(F.col("IssueType") == "Story", "Story")
     .when(F.col("IssueType") == "Epic",  "Epic")
     .otherwise("Autre")
).withColumn(
    "is_bug",
    F.when(F.col("IssueType_norm") == "Bug", 1).otherwise(0)
).withColumn(
    "is_done",
    F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0)
)

# data_source depuis tickets_features (ajouté par script 02)
# Si la colonne n'existe pas encore → on la crée ici
if "data_source" not in tickets.columns:
    tickets = tickets.withColumn(
        "data_source",
        F.when(F.col("nb_commits") > 0, "jira+git").otherwise("jira_only")
    )

print(f"   Tickets    : {tickets.count()}")
print(f"   Développeurs: {tickets.select('Assignee').distinct().count()}")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 1 — DISTRIBUTION TYPES DE TÂCHES
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("1. Distribution types de tâches")
print("=" * 60)

task_type_raw = tickets.groupBy("Assignee", "ProjectKey", "IssueType_norm") \
    .agg(F.count("TicketKey").alias("nb_tasks_type"))

total_per_person = tickets.groupBy("Assignee", "ProjectKey") \
    .agg(F.count("TicketKey").alias("total_tasks_person"))

task_type_dist = task_type_raw.join(
    total_per_person, on=["Assignee","ProjectKey"], how="left"
).withColumn(
    "pct_type",
    F.round(F.col("nb_tasks_type").cast(DoubleType()) /
            F.col("total_tasks_person") * 100, 1)
).orderBy("Assignee","ProjectKey","IssueType_norm")

task_type_dist.toPandas().to_csv(
    TEAM + "task_type_distribution_spark.csv", index=False, encoding="utf-8-sig"
)
print(f"   ✅ task_type_distribution_spark.csv ({task_type_dist.count()} lignes)")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 2 — TENDANCE PAR ANNÉE
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("2. Tendance par année")
print("=" * 60)

tickets_dated = tickets.withColumn(
    "Created_ts",
    F.coalesce(
        F.try_to_timestamp(F.col("Created"), F.lit("dd/MM/yyyy HH:mm")),
        F.try_to_timestamp(F.col("Created"), F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(F.col("Created"), F.lit("yyyy-MM-dd")),
    )
).withColumn("year", F.year(F.col("Created_ts")))

yearly_perf = tickets_dated \
    .filter(F.col("year").isNotNull()) \
    .groupBy("Assignee", "ProjectKey", "year") \
    .agg(
        F.count("TicketKey").alias("nb_tickets_year"),
        F.sum("is_done").alias("nb_done_year"),
        F.sum("is_bug").alias("nb_bugs_year"),
        F.sum("is_delayed").alias("nb_delayed_year"),
        F.sum("was_blocked").alias("nb_blocked_year"),
        F.sum("nb_commits").alias("nb_commits_year"),
        F.sum("nb_mrs").alias("nb_mrs_year"),
        F.round(F.avg("lead_time_hours"),  2).alias("avg_lead_time_year"),
        F.round(F.avg("cycle_time_hours"), 2).alias("avg_cycle_time_year"),
        F.round(
            F.sum(F.when(F.col("is_delayed")==0,1).otherwise(0)).cast(DoubleType()) /
            F.count("TicketKey") * 100, 1
        ).alias("on_time_rate_year"),
        F.round(
            F.sum("is_done").cast(DoubleType()) / F.count("TicketKey") * 100, 1
        ).alias("completion_rate_year"),
        # ── data_source par année ─────────────────────────────────────────
        F.sum(F.when(F.col("data_source")=="jira+git",  1).otherwise(0))
         .alias("nb_tickets_jira_git_year"),
        F.sum(F.when(F.col("data_source")=="jira_only", 1).otherwise(0))
         .alias("nb_tickets_jira_only_year"),
        F.when(
            F.sum(F.when(F.col("data_source")=="jira+git",1).otherwise(0)) > 0,
            F.lit("jira+git")
        ).otherwise(F.lit("jira_only")).alias("data_source_year"),
    ).orderBy("Assignee","year")

yearly_perf = yearly_perf.withColumn(
    "perf_score_year",
    F.round(F.least(F.lit(100.0),
        F.col("on_time_rate_year") * 0.4 +
        F.when(F.col("avg_lead_time_year") < 24,  30.0)
         .when(F.col("avg_lead_time_year") < 168, 20.0)
         .when(F.col("avg_lead_time_year") < 720, 10.0)
         .otherwise(0.0) +
        F.when(F.col("nb_blocked_year") == 0,  20.0)
         .when(F.col("nb_blocked_year") <= 2,  10.0)
         .otherwise(0.0) +
        F.when(F.col("nb_done_year") >= 10, 10.0)
         .when(F.col("nb_done_year") >= 5,   5.0)
         .otherwise(0.0)
    ), 1)
)

yearly_perf.toPandas().to_csv(
    TEAM + "yearly_performance_spark.csv", index=False, encoding="utf-8-sig"
)
print(f"   ✅ yearly_performance_spark.csv ({yearly_perf.count()} lignes)")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 3 — SCORE DÉTAILLÉ PAR COMPOSANTE
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("3. Score de performance détaillé")
print("=" * 60)

person_base = tickets.groupBy("Assignee", "ProjectKey").agg(
    F.count("TicketKey").alias("nb_tickets_assigned"),
    F.sum("is_done").alias("nb_tickets_done"),
    F.sum("is_bug").alias("nb_bugs_assigned"),
    F.sum(F.when((F.col("is_bug")==1)&(F.col("is_done")==1),1).otherwise(0))
     .alias("nb_bugs_resolved"),
    F.sum("is_delayed").alias("nb_delayed"),
    F.sum("was_blocked").alias("nb_blocked"),
    F.sum("nb_commits").alias("nb_commits_total"),
    F.sum("nb_mrs").alias("nb_mrs_total"),
    F.round(F.avg("lead_time_hours"),    2).alias("avg_lead_time_hours"),
    F.round(F.avg("cycle_time_hours"),   2).alias("avg_cycle_time_hours"),
    F.round(F.avg("time_blocked_hours"), 2).alias("avg_time_blocked_hours"),
    F.round(F.avg("nb_status_changes"),  2).alias("avg_status_changes"),
    F.round(
        F.sum(F.when(F.col("is_delayed")==0,1).otherwise(0)).cast(DoubleType()) /
        F.count("TicketKey") * 100, 1
    ).alias("on_time_rate"),
    F.round(
        F.sum("is_done").cast(DoubleType()) / F.count("TicketKey") * 100, 1
    ).alias("completion_rate"),
    # ── data_source hybride ───────────────────────────────────────────────
    F.sum(F.when(F.col("data_source")=="jira+git",  1).otherwise(0))
     .alias("nb_tickets_jira_git"),
    F.sum(F.when(F.col("data_source")=="jira_only", 1).otherwise(0))
     .alias("nb_tickets_jira_only"),
    F.when(
        F.sum(F.when(F.col("data_source")=="jira+git",1).otherwise(0)) > 0,
        F.lit("jira+git")
    ).otherwise(F.lit("jira_only")).alias("data_source_dominant"),
)

person_score = person_base \
    .withColumn("score_delais",
        F.round(F.col("on_time_rate") * 0.4, 1)
    ).withColumn("score_lead_time",
        F.when(F.col("avg_lead_time_hours") < 24,  30.0)
         .when(F.col("avg_lead_time_hours") < 168, 20.0)
         .when(F.col("avg_lead_time_hours") < 720, 10.0)
         .otherwise(0.0)
    ).withColumn("score_blocages",
        F.when(F.col("avg_time_blocked_hours") == 0, 20.0)
         .when(F.col("avg_time_blocked_hours") < 24, 10.0)
         .otherwise(0.0)
    ).withColumn("score_volume",
        F.when(F.col("nb_tickets_done") >= 10, 10.0)
         .when(F.col("nb_tickets_done") >= 5,   5.0)
         .otherwise(0.0)
    ).withColumn("performance_score",
        F.round(F.least(F.lit(100.0),
            F.col("score_delais") + F.col("score_lead_time") +
            F.col("score_blocages") + F.col("score_volume")
        ), 1)
    ).withColumn("performance_level",
        F.when(F.col("performance_score") >= 80, "Excellent")
         .when(F.col("performance_score") >= 60, "Bon")
         .when(F.col("performance_score") >= 40, "Moyen")
         .otherwise("À améliorer")
    )

person_score.toPandas().to_csv(
    TEAM + "person_score_detail_spark.csv", index=False, encoding="utf-8-sig"
)
print(f"   ✅ person_score_detail_spark.csv ({person_score.count()} lignes)")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 4 — PERSON DASHBOARD (unifié Power BI)
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("4. Person dashboard unifié")
print("=" * 60)

team_medians = tickets.groupBy("ProjectKey").agg(
    F.round(F.percentile_approx(F.col("lead_time_hours"), 0.5), 2)
     .alias("team_median_lead_time"),
    F.round(F.avg("nb_commits").cast(DoubleType()), 2).alias("team_avg_commits"),
    F.round(
        F.sum(F.when(F.col("is_delayed")==0,1).otherwise(0)).cast(DoubleType()) /
        F.count("TicketKey") * 100, 1
    ).alias("team_avg_on_time_rate"),
    F.round(
        F.sum("is_done").cast(DoubleType()) / F.count("TicketKey") * 100, 1
    ).alias("team_avg_completion_rate"),
    F.count("TicketKey").alias("team_total_tickets"),
    F.countDistinct("Assignee").alias("team_nb_members"),
)

task_pivot = task_type_dist.groupBy("Assignee","ProjectKey").pivot(
    "IssueType_norm", ["Story","Epic","Task","Bug","Autre"]
).agg(F.first("nb_tasks_type")).fillna(0)

for col_name in ["Story","Epic","Task","Bug","Autre"]:
    if col_name in task_pivot.columns:
        task_pivot = task_pivot.withColumnRenamed(col_name, f"nb_{col_name.lower()}")

person_dashboard = person_score \
    .join(team_medians, on="ProjectKey", how="left") \
    .join(task_pivot,   on=["Assignee","ProjectKey"], how="left") \
    .withColumn("lead_time_vs_team",
        F.round(F.col("avg_lead_time_hours") - F.col("team_median_lead_time"), 1)
    ).withColumn("commits_vs_team",
        F.round(F.col("nb_commits_total").cast(DoubleType()) - F.col("team_avg_commits"), 1)
    ).withColumn("on_time_vs_team",
        F.round(F.col("on_time_rate") - F.col("team_avg_on_time_rate"), 1)
    ).withColumn("charge_level",
        F.when(
            F.col("nb_tickets_assigned") > F.col("team_total_tickets") /
            F.col("team_nb_members") * 1.5, "Surchargé"
        ).when(
            F.col("nb_tickets_assigned") < F.col("team_total_tickets") /
            F.col("team_nb_members") * 0.5, "Sous-chargé"
        ).otherwise("Normal")
    ).orderBy("ProjectKey", F.col("performance_score").desc())

cols_final = [
    "Assignee", "ProjectKey", "data_source_dominant",
    "nb_tickets_assigned", "nb_tickets_done", "completion_rate",
    "nb_story", "nb_epic", "nb_task", "nb_bug",
    "nb_bugs_assigned", "nb_bugs_resolved",
    "nb_delayed", "on_time_rate",
    "avg_lead_time_hours", "avg_cycle_time_hours", "avg_time_blocked_hours",
    "nb_commits_total", "nb_mrs_total",
    "nb_tickets_jira_git", "nb_tickets_jira_only",
    "score_delais", "score_lead_time", "score_blocages", "score_volume",
    "performance_score", "performance_level",
    "team_median_lead_time", "team_avg_on_time_rate",
    "lead_time_vs_team", "on_time_vs_team", "commits_vs_team",
    "charge_level",
]

cols_exist = [c for c in cols_final if c in person_dashboard.columns]
person_dashboard = person_dashboard.select(cols_exist)

# ── Ajout des développeurs GitLab-only (sans tickets Jira) ────────────────────
# Récupérer les emails des développeurs déjà dans le dashboard (via Jira)
jira_emails = tickets.select(
    F.lower(F.trim(F.col("AssigneeEmail"))).alias("email")
).distinct()

# Commits des développeurs GitLab-only : ceux dont l'email n'est pas dans Jira
jira_email_set = set(
    r["email"] for r in jira_emails.collect()
)
git_only_commits = commits_raw \
    .withColumn("email_norm", F.lower(F.trim(
        F.regexp_replace(F.col("email"), '"', '')
    ))) \
    .filter(~F.col("email_norm").isin(jira_email_set))

# Nom = partie avant @ dans l'email (ex: khouja.jihene@gtiinfo.com.tn → khouja.jihene)
git_only_commits = git_only_commits.withColumn(
    "name_from_email",
    F.regexp_extract(F.col("email_norm"), r"^([^@]+)@", 1)
)

# Agrégats commits par nom (email prefix)
git_only_agg = git_only_commits.groupBy(
    F.col("name_from_email").alias("Assignee")
).agg(
    F.count("sha").alias("nb_commits_total"),
    F.first("email_norm").alias("_email_ref"),
)

# Agrégats MRs : jointure via email_norm
git_only_mrs = mrs_raw \
    .withColumn("mr_email", F.lower(F.trim(F.col("author")))) \
    .join(
        git_only_commits.select("email_norm").distinct(),
        F.col("mr_email") == F.col("email_norm"), how="inner"
    ).withColumn(
        "name_from_email",
        F.regexp_extract(F.col("email_norm"), r"^([^@]+)@", 1)
    ).groupBy(F.col("name_from_email").alias("Assignee")) \
    .agg(F.count("mr_id").alias("nb_mrs_total"))

git_only_df = git_only_agg \
    .join(git_only_mrs, on="Assignee", how="left") \
    .fillna({"nb_mrs_total": 0}) \
    .withColumn("ProjectKey",            F.lit("GITLAB")) \
    .withColumn("data_source_dominant",  F.lit("git_only")) \
    .withColumn("nb_tickets_assigned",   F.lit(0).cast(IntegerType())) \
    .withColumn("nb_tickets_done",       F.lit(0).cast(IntegerType())) \
    .withColumn("completion_rate",       F.lit(0.0).cast(DoubleType())) \
    .withColumn("nb_story",              F.lit(0).cast(IntegerType())) \
    .withColumn("nb_epic",               F.lit(0).cast(IntegerType())) \
    .withColumn("nb_task",               F.lit(0).cast(IntegerType())) \
    .withColumn("nb_bug",                F.lit(0).cast(IntegerType())) \
    .withColumn("nb_bugs_assigned",      F.lit(0).cast(IntegerType())) \
    .withColumn("nb_bugs_resolved",      F.lit(0).cast(IntegerType())) \
    .withColumn("nb_delayed",            F.lit(0).cast(IntegerType())) \
    .withColumn("on_time_rate",          F.lit(None).cast(DoubleType())) \
    .withColumn("avg_lead_time_hours",   F.lit(None).cast(DoubleType())) \
    .withColumn("avg_cycle_time_hours",  F.lit(None).cast(DoubleType())) \
    .withColumn("avg_time_blocked_hours",F.lit(0.0).cast(DoubleType())) \
    .withColumn("nb_tickets_jira_git",   F.lit(0).cast(IntegerType())) \
    .withColumn("nb_tickets_jira_only",  F.lit(0).cast(IntegerType())) \
    .withColumn("score_delais",          F.lit(0.0).cast(DoubleType())) \
    .withColumn("score_lead_time",       F.lit(0.0).cast(DoubleType())) \
    .withColumn("score_blocages",        F.lit(20.0).cast(DoubleType())) \
    .withColumn("score_volume",
        F.when(F.col("nb_commits_total") >= 100, 10.0)
         .when(F.col("nb_commits_total") >= 50,   5.0)
         .otherwise(0.0).cast(DoubleType())
    ) \
    .withColumn("performance_score",
        F.round(F.lit(20.0) + F.when(F.col("nb_commits_total") >= 100, 10.0)
         .when(F.col("nb_commits_total") >= 50, 5.0).otherwise(0.0), 1)
    ) \
    .withColumn("performance_level",
        F.when(F.col("performance_score") >= 80, "Excellent")
         .when(F.col("performance_score") >= 60, "Bon")
         .when(F.col("performance_score") >= 40, "Moyen")
         .otherwise("À améliorer")
    ) \
    .withColumn("team_median_lead_time", F.lit(None).cast(DoubleType())) \
    .withColumn("team_avg_on_time_rate", F.lit(None).cast(DoubleType())) \
    .withColumn("lead_time_vs_team",     F.lit(None).cast(DoubleType())) \
    .withColumn("on_time_vs_team",       F.lit(None).cast(DoubleType())) \
    .withColumn("commits_vs_team",       F.lit(None).cast(DoubleType())) \
    .withColumn("charge_level",          F.lit("GitLab only"))

# Harmoniser les colonnes avant union
git_only_df = git_only_df.select(cols_exist)

# Union avec le dashboard Jira
person_dashboard = person_dashboard.unionByName(git_only_df)

person_dashboard.toPandas().to_csv(
    TEAM + "person_dashboard_spark.csv", index=False, encoding="utf-8-sig"
)
n_total   = person_dashboard.count()
n_git_only = person_dashboard.filter(F.col("data_source_dominant") == "git_only").count()
print(f"   ✅ person_dashboard_spark.csv ({n_total} lignes dont {n_git_only} GitLab-only, {len(cols_exist)} colonnes)")
print()


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("RÉSUMÉ — Script 04 bis")
print("=" * 60)
print("   task_type_distribution_spark.csv   ✅")
print("   yearly_performance_spark.csv       ✅")
print("   person_score_detail_spark.csv      ✅")
print("   person_dashboard_spark.csv         ✅")
print()
print("✅ Script 04 bis terminé.")
print("   Prochaine étape : 05_machine_learning_spark.py")

spark.stop()