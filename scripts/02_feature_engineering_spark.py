"""
=============================================================
 DORA Metrics Project — VERSION BIG DATA
 Script  : 02_feature_engineering_spark.py
 Étape   : 2 — Feature Engineering avec Apache PySpark
=============================================================
Rôle : Enrichir chaque ticket Jira avec des variables calculées
       utilisées pour les métriques DORA, le Machine Learning
       et les visualisations Power BI.

Variables créées par ticket :
  - lead_time_hours              : Created → To Be Deployed
  - cycle_time_hours             : In Process → To Be Deployed
  - nb_status_changes            : transitions de statut
  - time_blocked_hours           : temps total en Blocked
  - was_blocked                  : flag 0/1
  - time_in_review_hours         : temps en In Review
  - time_in_qa_hours             : temps en QA In Process
  - time_correction_review_hours : temps en Correction Post Review
  - time_correction_qa_hours     : temps en Correction Post QA
  - is_delayed                   : livré après DueDate (0/1)
  - nb_commits                   : commits Git liés
  - nb_mrs                       : Merge Requests liées
  - avg_merge_time_hours         : temps moyen de merge
  - is_bug                       : IssueType == Bug (0/1)
  - month_year                   : mois de création (yyyy-MM)
  - risk_score                   : score de risque (0–100)
  - risk_level                   : Low / Medium / High
  - completion_rate              : % tickets Done par projet
  - nb_tickets_total             : total tickets par projet
  - nb_tickets_done              : tickets déployés par projet
  - data_source                  : "jira+git" ou "jira_only"

Sorties :
  data/features/tickets_features_spark.csv
  data/features/assignee_metrics_spark.csv
  data/features/project_metrics_spark.csv
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

spark = SparkSession.builder \
    .appName("DORA_Feature_Engineering") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions",
            "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions",
            "-Djava.security.manager=allow") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

CLEANED  = "data/cleaned/"
FEATURES = "data/features/"
os.makedirs(FEATURES, exist_ok=True)

print("=" * 60)
print("Chargement des données nettoyées (Spark)")
print("=" * 60)

jira = spark.read.csv(
    CLEANED + "jira_cleaned_spark.csv",
    header=True, inferSchema=False
)
commits = spark.read.csv(
    CLEANED + "commits_cleaned_spark.csv",
    header=True, inferSchema=False
)
mrs = spark.read.csv(
    CLEANED + "merge_requests_cleaned_spark.csv",
    header=True, inferSchema=False
)

def to_ts(df, cols):
    for c in cols:
        if c in df.columns:
            cleaned = F.regexp_replace(F.col(c), r"[+-]\d{2}:\d{2}$|Z$", "")
            df = df.withColumn(c, F.coalesce(
                F.try_to_timestamp(cleaned, F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp(cleaned, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
                F.try_to_timestamp(cleaned, F.lit("dd/MM/yyyy HH:mm")),
                F.try_to_timestamp(cleaned, F.lit("dd/MM/yyyy")),
                F.try_to_timestamp(cleaned, F.lit("yyyy-MM-dd")),
            ))
    return df

jira    = to_ts(jira,    ["StatusEntryDate","StatusExitDate","Created","ResolutionDate"])
commits = to_ts(commits, ["date"])
mrs     = to_ts(mrs,     ["created_at","merged_at"])

jira = jira.withColumn("TimeInStatusHours", F.col("TimeInStatusHours").cast(DoubleType()))

print(f"   Jira    : {jira.count()} lignes")
print(f"   Commits : {commits.count()} lignes")
print(f"   MR      : {mrs.count()} lignes")
print()


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 1 — LEAD TIME & DATES DE BASE
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("Calcul des features par ticket")
print("=" * 60)

current = jira.filter(F.col("IsCurrent") == "Yes") \
    .select(
        "TicketKey", "Project", "ProjectKey", "IssueType",
        "Status", "StatusCategory", "Priority",
        "Assignee", "AssigneeEmail", "SprintState",
        "Created", "ResolutionDate", "DueDate"
    ) \
    .withColumnRenamed("Status", "CurrentStatus")

FINAL_STATUSES = ["To Be Deployed", "Closed", "Done", "Resolved"]

current = current.withColumn(
    "lead_time_hours",
    F.when(
        F.col("Created").isNotNull() &
        F.col("CurrentStatus").isin(FINAL_STATUSES) &
        F.col("ResolutionDate").isNotNull(),
        F.greatest(
            F.round((F.unix_timestamp("ResolutionDate") - F.unix_timestamp("Created")) / 3600, 2),
            F.lit(0.0)
        )
    ).when(
        F.col("Created").isNotNull(),
        F.greatest(
            F.round((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("Created")) / 3600, 2),
            F.lit(0.0)
        )
    ).otherwise(F.lit(None).cast(DoubleType()))
).withColumn(
    "lead_time_is_final",
    F.when(F.col("CurrentStatus").isin(FINAL_STATUSES), 1).otherwise(0)
)

due_parsed = F.coalesce(
    F.try_to_timestamp(F.col("DueDate"), F.lit("yyyy/MM/dd")),
    F.try_to_timestamp(F.col("DueDate"), F.lit("yyyy-MM-dd")),
    F.try_to_timestamp(F.col("DueDate"), F.lit("dd/MM/yyyy")),
)
current = current.withColumn("DueDateParsed", due_parsed)
current = current.withColumn(
    "is_delayed",
    F.when(
        F.col("DueDateParsed").isNotNull(),
        F.when(
            F.col("ResolutionDate").isNotNull(),
            (F.col("ResolutionDate") > F.col("DueDateParsed")).cast(IntegerType())
        ).otherwise(
            (F.current_timestamp() > F.col("DueDateParsed")).cast(IntegerType())
        )
    ).otherwise(F.lit(0))
).drop("DueDateParsed")


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 2 — AGRÉGATS PAR TICKET (historique statuts)
# ══════════════════════════════════════════════════════════════════════════════

nb_transitions = jira.groupBy("TicketKey") \
    .agg(F.count("*").alias("nb_status_changes"))

def time_in_status(df, status_name, alias):
    return df.filter(F.col("Status") == status_name) \
        .groupBy("TicketKey") \
        .agg(F.round(F.sum("TimeInStatusHours"), 2).alias(alias))

blocked_time  = time_in_status(jira, "Blocked",                "time_blocked_hours")
review_time   = time_in_status(jira, "In Review",              "time_in_review_hours")
qa_time       = time_in_status(jira, "QA In Process",          "time_in_qa_hours")
corr_rev_time = time_in_status(jira, "Correction Post Review", "time_correction_review_hours")
corr_qa_time  = time_in_status(jira, "Correction Post QA",    "time_correction_qa_hours")

INPROCESS_STATUSES = [
    "In Process", "In Progress", "En cours",
    "Development", "En développement", "In Development"
]
inprocess_entry = jira.filter(F.col("Status").isin(INPROCESS_STATUSES)) \
    .groupBy("TicketKey") \
    .agg(F.min("StatusEntryDate").alias("first_inprocess_date"))

cycle_time = current.join(inprocess_entry, on="TicketKey", how="left") \
    .withColumn(
        "cycle_time_hours",
        F.when(
            F.col("lead_time_hours").isNotNull() &
            F.col("first_inprocess_date").isNotNull(),
            F.greatest(
                F.round(
                    (F.unix_timestamp("ResolutionDate") -
                     F.unix_timestamp("first_inprocess_date")) / 3600,
                    2
                ),
                F.lit(0.0)
            )
        ).otherwise(F.lit(None).cast(DoubleType()))
    ) \
    .select("TicketKey", "cycle_time_hours")


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 3 — FEATURES GIT (commits + MR par ticket)
# ══════════════════════════════════════════════════════════════════════════════

commits = commits.withColumn(
    "commit_email",
    F.lower(F.trim(F.regexp_replace(F.col("email"), '"', '')))
)

current = current.withColumn(
    "assignee_email",
    F.lower(F.trim(F.col("AssigneeEmail")))
)

ticket_window = current.select(
    F.col("TicketKey").alias("tw_TicketKey"),
    F.col("assignee_email").alias("tw_email"),
    F.col("Created").alias("t_start"),
    F.coalesce(F.col("ResolutionDate"), F.current_timestamp()).alias("t_end")
).filter(F.col("tw_email").isNotNull() & (F.col("tw_email") != ""))

commits_agg = commits \
    .filter(F.col("date").isNotNull() & F.col("commit_email").isNotNull()) \
    .join(
        ticket_window,
        (F.col("commit_email") == F.col("tw_email")) &
        (F.col("date")         >= F.col("t_start"))  &
        (F.col("date")         <= F.col("t_end")),
        how="inner"
    ) \
    .groupBy("tw_TicketKey") \
    .agg(F.count("sha").alias("nb_commits")) \
    .withColumnRenamed("tw_TicketKey", "TicketKey")

mr_auth_col = "author_username" if "author_username" in mrs.columns else "author"

hash_email_map = commits.groupBy("author") \
    .agg(F.first("commit_email").alias("mr_email"))

mrs_with_email = mrs \
    .withColumn("merge_time_hours", F.col("merge_time_hours").cast(DoubleType())) \
    .join(hash_email_map, mrs[mr_auth_col] == hash_email_map["author"], how="left") \
    .filter(F.col("mr_email").isNotNull())

mrs_agg = mrs_with_email \
    .filter(F.col("created_at").isNotNull()) \
    .join(
        ticket_window,
        (F.col("mr_email")   == F.col("tw_email")) &
        (F.col("created_at") >= F.col("t_start"))  &
        (F.col("created_at") <= F.col("t_end")),
        how="inner"
    ) \
    .groupBy("tw_TicketKey") \
    .agg(
        F.count("mr_id").alias("nb_mrs"),
        F.round(F.avg("merge_time_hours"), 2).alias("avg_merge_time_hours")
    ) \
    .withColumnRenamed("tw_TicketKey", "TicketKey")


# ══════════════════════════════════════════════════════════════════════════════
# ASSEMBLAGE FINAL
# ══════════════════════════════════════════════════════════════════════════════

ticket_features = current \
    .join(nb_transitions, on="TicketKey", how="left") \
    .join(blocked_time,   on="TicketKey", how="left") \
    .join(review_time,    on="TicketKey", how="left") \
    .join(qa_time,        on="TicketKey", how="left") \
    .join(corr_rev_time,  on="TicketKey", how="left") \
    .join(corr_qa_time,   on="TicketKey", how="left") \
    .join(cycle_time,     on="TicketKey", how="left") \
    .join(commits_agg,    on="TicketKey", how="left") \
    .join(mrs_agg,        on="TicketKey", how="left") \
    .fillna({
        "nb_status_changes":            0,
        "time_blocked_hours":           0.0,
        "time_in_review_hours":         0.0,
        "time_in_qa_hours":             0.0,
        "time_correction_review_hours": 0.0,
        "time_correction_qa_hours":     0.0,
        "nb_commits":                   0,
        "nb_mrs":                       0,
        "avg_merge_time_hours":         0.0,
    }) \
    .withColumn(
        "was_blocked",
        F.when(F.col("time_blocked_hours") > 0, 1).otherwise(0)
    )


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 4 — FEATURES DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

# ── 4.0 data_source — classification hybride ──────────────────────────────
# jira+git  : ticket Jira avec commits GitLab liés (nb_commits > 0)
# jira_only : ticket Jira sans commit GitLab lié
ticket_features = ticket_features.withColumn(
    "data_source",
    F.when(F.col("nb_commits") > 0, "jira+git").otherwise("jira_only")
)

# ── 4.1 is_bug + month_year ───────────────────────────────────────────────
ticket_features = ticket_features \
    .withColumn("is_bug",
        F.when(F.col("IssueType") == "Bug", 1).otherwise(0)
    ) \
    .withColumn("month_year",
        F.date_format(F.col("Created"), "yyyy-MM")
    )

# ── 4.2 risk_score + risk_level ───────────────────────────────────────────
ticket_features = ticket_features \
    .withColumn("risk_score",
        (F.col("is_delayed").cast(DoubleType())  * 40) +
        (F.col("was_blocked").cast(DoubleType()) * 30) +
        F.when(F.col("lead_time_hours") > 500, 20)
         .when(F.col("lead_time_hours") > 200, 10)
         .otherwise(0) +
        F.when(F.col("nb_status_changes") > 8, 10).otherwise(0)
    ) \
    .withColumn("risk_level",
        F.when(F.col("risk_score") >= 60, "High")
         .when(F.col("risk_score") >= 30, "Medium")
         .otherwise("Low")
    )

# ── 4.3 completion_rate par projet ────────────────────────────────────────
proj_stats = ticket_features.groupBy("ProjectKey").agg(
    F.count("TicketKey").alias("nb_tickets_total"),
    F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0))
     .alias("nb_tickets_done"),
)
proj_stats = proj_stats.withColumn(
    "completion_rate",
    F.round(F.col("nb_tickets_done") / F.col("nb_tickets_total") * 100, 1)
)
ticket_features = ticket_features.join(proj_stats, on="ProjectKey", how="left")

# ── 4.4 métriques par Assignee ────────────────────────────────────────────
assignee_stats = ticket_features.groupBy("Assignee").agg(
    F.count("TicketKey").alias("nb_tickets_assigned"),
    F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0))
     .alias("nb_tickets_deployed_person"),
    F.round(F.avg("lead_time_hours"), 2).alias("avg_lead_time_person"),
    F.round(
        F.sum(F.when(F.col("is_delayed") == 0, 1).otherwise(0)) /
        F.count("TicketKey") * 100, 1
    ).alias("on_time_rate"),
)
assignee_stats.toPandas().to_csv(
    FEATURES + "assignee_metrics_spark.csv", index=False, encoding="utf-8-sig"
)
proj_stats.toPandas().to_csv(
    FEATURES + "project_metrics_spark.csv", index=False, encoding="utf-8-sig"
)

# Sauvegarde finale
ticket_features.toPandas().to_csv(
    FEATURES + "tickets_features_spark.csv", index=False, encoding="utf-8-sig"
)

total     = ticket_features.count()
deployed  = ticket_features.filter(F.col("lead_time_hours").isNotNull()).count()
blocked   = ticket_features.filter(F.col("was_blocked") == 1).count()
delayed   = ticket_features.filter(F.col("is_delayed")  == 1).count()
jira_git  = ticket_features.filter(F.col("data_source") == "jira+git").count()
jira_only = ticket_features.filter(F.col("data_source") == "jira_only").count()

print(f"   Tickets total              : {total}")
print(f"   Lead Time calculé          : {deployed}")
print(f"   Tickets bloqués            : {blocked}")
print(f"   Tickets en retard          : {delayed}")
print(f"   data_source = jira+git     : {jira_git}")
print(f"   data_source = jira_only    : {jira_only}")
print()
print("✅ Étape 2 Spark terminée.")
print("   Prochaine étape : 03_dora_metrics_hybrid_spark.py")

spark.stop()