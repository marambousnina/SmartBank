"""
=============================================================
 DORA Metrics Project — VERSION HYBRIDE (Git + Jira)
 Script  : 03_dora_metrics_hybrid_spark.py
 Étape   : 3 — Calcul DORA avec 2 sources de données

 Logique hybride :
   ┌─────────────┬───────────────────────────────────────────┐
   │ data_source │ Formule utilisée                          │
   ├─────────────┼───────────────────────────────────────────┤
   │ jira+git    │ Lead Time = Created(Jira) → Deploy(Git)  │
   │             │ MTTR = Bug(Jira) → Fix commit(Git)       │
   │ git_only    │ Lead Time = Commit → Merge MR             │
   │             │ MTTR = Revert commit → Hotfix commit      │
   └─────────────┴───────────────────────────────────────────┘

 4 métriques DORA calculées :
   1. Deployment Frequency  → depuis pipelines Git (success)
   2. Lead Time for Changes → hybride Jira+Git / Git seulement
   3. Change Failure Rate   → pipelines failed / total
   4. MTTR                  → hybride Jira+Git / hotfix Git

 Sorties :
   data/metrics/dora_metrics_summary_spark.csv
   data/metrics/dora_metrics_weekly_spark.csv
   data/metrics/dora_kpis_spark.csv
   data/metrics/dora_by_project_spark.csv
   data/metrics/dora_by_source_spark.csv   ← NOUVEAU
=============================================================
"""

import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-java-options -Djava.security.manager=allow pyspark-shell"
)

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

spark = SparkSession.builder \
    .appName("DORA_Hybrid_Metrics") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR")

CLEANED  = "data/cleaned/"
FEATURES = "data/features/"
METRICS  = "data/metrics/"
os.makedirs(METRICS, exist_ok=True)

print("=" * 60)
print("🚀 Script 03 — DORA Metrics Hybride (Git + Jira)")
print("=" * 60)

# ── Chargement ─────────────────────────────────────────────
tickets   = spark.read.csv(FEATURES + "tickets_features_spark.csv",
                            header=True, inferSchema=False)
commits   = spark.read.csv(CLEANED  + "commits_cleaned_spark.csv",
                            header=True, inferSchema=False)
pipelines = spark.read.csv(CLEANED  + "pipelines_cleaned_spark.csv",
                            header=True, inferSchema=False)
mrs       = spark.read.csv(CLEANED  + "merge_requests_cleaned_spark.csv",
                            header=True, inferSchema=False)
jobs      = spark.read.csv(CLEANED  + "jobs_cleaned_spark.csv",
                            header=True, inferSchema=False)

# Conversions types
tickets   = tickets.withColumn("lead_time_hours", F.col("lead_time_hours").cast(DoubleType())) \
                   .withColumn("is_delayed",       F.col("is_delayed").cast(IntegerType()))
pipelines = pipelines.withColumn("duration_minutes", F.col("duration_minutes").cast(DoubleType()))
mrs       = mrs.withColumn("merge_time_hours", F.col("merge_time_hours").cast(DoubleType()))
jobs      = jobs.withColumn("duration_minutes", F.col("duration_minutes").cast(DoubleType()))

# Parsing dates
def to_ts(df, cols):
    for c in cols:
        if c in df.columns:
            cl = F.regexp_replace(F.col(c), r"[+-]\d{2}:\d{2}$|Z$", "")
            df = df.withColumn(c, F.coalesce(
                F.try_to_timestamp(cl, F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp(cl, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS")),
                F.try_to_timestamp(cl, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
                F.try_to_timestamp(cl, F.lit("dd/MM/yyyy HH:mm")),
                F.try_to_timestamp(cl, F.lit("yyyy-MM-dd")),
            ))
    return df

tickets   = to_ts(tickets,   ["Created", "ResolutionDate"])
commits   = to_ts(commits,   ["date"])
pipelines = to_ts(pipelines, ["created_at", "updated_at"])
mrs       = to_ts(mrs,       ["created_at", "merged_at"])
jobs      = to_ts(jobs,      ["started_at", "finished_at"])

print(f"   Tickets   : {tickets.count()}")
print(f"   Commits   : {commits.count()}")
print(f"   Pipelines : {pipelines.count()}")
print(f"   MRs       : {mrs.count()}")
print(f"   Jobs      : {jobs.count()}")
print()


# ══════════════════════════════════════════════════════════════
# ÉTAPE 1 — CLASSIFIER LES COMMITS PAR data_source
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("1. Classification commits par data_source")
print("=" * 60)

# Extraire TicketKey depuis le titre du commit
commits = commits.withColumn(
    "jira_ref",
    F.when(
        F.regexp_extract(F.col("title"), r"([A-Z]+-\d+)", 1) != "",
        F.regexp_extract(F.col("title"), r"([A-Z]+-\d+)", 1)
    ).otherwise(F.lit(None).cast(StringType()))
)

# Classifier : jira+git si TicketKey trouvé, git_only sinon
commits = commits.withColumn(
    "data_source",
    F.when(F.col("jira_ref").isNotNull(), "jira+git").otherwise("git_only")
)

n_jira_git  = commits.filter(F.col("data_source") == "jira+git").count()
n_git_only  = commits.filter(F.col("data_source") == "git_only").count()
print(f"   Commits jira+git  : {n_jira_git}")
print(f"   Commits git_only  : {n_git_only}")

# Même classification pour les MRs
mrs = mrs.withColumn(
    "jira_ref",
    F.when(
        F.regexp_extract(F.col("title"), r"([A-Z]+-\d+)", 1) != "",
        F.regexp_extract(F.col("title"), r"([A-Z]+-\d+)", 1)
    ).otherwise(F.lit(None).cast(StringType()))
).withColumn(
    "data_source",
    F.when(F.col("jira_ref").isNotNull(), "jira+git").otherwise("git_only")
)
print()


# ══════════════════════════════════════════════════════════════
# MÉTRIQUE 1 — DEPLOYMENT FREQUENCY
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("2. Deployment Frequency (depuis pipelines Git)")
print("=" * 60)

# Source : pipelines success = déploiements
# Même calcul Jira+Git et Git only (Git est la vérité pour les déploiements)
prod_deploys = pipelines.filter(F.col("is_production").isin("true", "True"))
total_deploys = prod_deploys.count()

# Période totale en semaines
dates_range = pipelines.agg(
    F.min("created_at").alias("min_date"),
    F.max("created_at").alias("max_date")
).collect()[0]

if dates_range["min_date"] and dates_range["max_date"]:
    weeks = max(1, (dates_range["max_date"] - dates_range["min_date"]).days / 7)
else:
    weeks = 52

deploy_freq = round(total_deploys / weeks, 2)

# Niveau DORA
if deploy_freq >= 1:
    df_level = "Elite"
elif deploy_freq >= 1/7:
    df_level = "High"
elif deploy_freq >= 1/30:
    df_level = "Medium"
else:
    df_level = "Low"

print(f"   Total déploiements production : {total_deploys}")
print(f"   Période                       : {round(weeks,1)} semaines")
print(f"   Deployment Frequency          : {deploy_freq} / semaine → {df_level}")
print()


# ══════════════════════════════════════════════════════════════
# MÉTRIQUE 2 — LEAD TIME (HYBRIDE)
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("3. Lead Time for Changes (hybride Jira+Git / Git only)")
print("=" * 60)

# ── Cas 1 : jira+git — Lead Time depuis tickets_features ──────
# Lead Time = Created (Jira) → ResolutionDate ou aujourd'hui
lt_jira = tickets.filter(F.col("lead_time_hours").isNotNull()) \
    .agg(
        F.round(F.avg("lead_time_hours"), 2).alias("avg_lt"),
        F.round(F.percentile_approx("lead_time_hours", 0.5), 2).alias("median_lt"),
        F.count("TicketKey").alias("nb_tickets")
    ).collect()[0]

# ── Cas 2 : git_only — Lead Time = MR created → MR merged ─────
lt_git = mrs.filter(
    (F.col("data_source") == "git_only") &
    (F.col("merge_time_hours").isNotNull()) &
    (F.col("state") == "merged")
).agg(
    F.round(F.avg("merge_time_hours"), 2).alias("avg_lt"),
    F.round(F.percentile_approx("merge_time_hours", 0.5), 2).alias("median_lt"),
    F.count("mr_id").alias("nb_mrs")
).collect()[0]

# ── Lead Time global (combiné) ─────────────────────────────────
lt_global_avg = round((
    (lt_jira["avg_lt"] or 0) * lt_jira["nb_tickets"] +
    (lt_git["avg_lt"] or 0) * lt_git["nb_mrs"]
) / (lt_jira["nb_tickets"] + lt_git["nb_mrs"]), 2)

if lt_global_avg <= 24:
    lt_level = "Elite"
elif lt_global_avg <= 168:
    lt_level = "High"
elif lt_global_avg <= 720:
    lt_level = "Medium"
else:
    lt_level = "Low"

print(f"   [jira+git]  Lead Time moyen   : {lt_jira['avg_lt']}h  ({lt_jira['nb_tickets']} tickets)")
print(f"   [git_only]  Lead Time moyen   : {lt_git['avg_lt']}h   ({lt_git['nb_mrs']} MRs)")
print(f"   [global]    Lead Time combiné : {lt_global_avg}h → {lt_level}")
print()


# ══════════════════════════════════════════════════════════════
# MÉTRIQUE 3 — CHANGE FAILURE RATE
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("4. Change Failure Rate (depuis pipelines Git)")
print("=" * 60)

total_pipes = pipelines.count()
failed_pipes = pipelines.filter(F.col("status") == "failed").count()
cfr = round(failed_pipes / total_pipes * 100, 2) if total_pipes > 0 else 0

# Détection commits revert/hotfix (approximation git_only)
fail_keywords = ["revert", "hotfix", "rollback", "fix production", "fix prod"]
commits_failed = commits
for kw in fail_keywords:
    commits_failed = commits
    break
n_fail_commits = commits.filter(
    F.lower(F.col("title")).rlike("revert|hotfix|rollback|fix.prod")
).count()

if cfr <= 5:
    cfr_level = "Elite"
elif cfr <= 10:
    cfr_level = "High"
elif cfr <= 15:
    cfr_level = "Medium"
else:
    cfr_level = "Low"

print(f"   Pipelines failed / total      : {failed_pipes}/{total_pipes}")
print(f"   CFR                           : {cfr}% → {cfr_level}")
print(f"   Commits revert/hotfix/rollback: {n_fail_commits} (approximation git_only)")
print()


# ══════════════════════════════════════════════════════════════
# MÉTRIQUE 4 — MTTR (HYBRIDE)
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("5. MTTR — Mean Time To Restore (hybride)")
print("=" * 60)

# ── Cas 1 : jira+git — MTTR depuis les tickets Bugs Jira ──────
bugs_jira = tickets.filter(
    (F.col("IssueType").isin("Bug", "bug")) &
    F.col("lead_time_hours").isNotNull()
).agg(
    F.round(F.avg("lead_time_hours"), 2).alias("mttr_jira"),
    F.count("TicketKey").alias("nb_bugs")
).collect()[0]

# ── Cas 2 : git_only — MTTR précis depuis les JOBS ────────────
# Logique : pipeline failed → temps jusqu'au prochain pipeline success
# Via jobs : job failed → job success sur le même nom de job
# MTTR = temps entre started_at du job failed → finished_at du job fixed

# Métriques jobs par type
jobs_stats = jobs.groupBy("name", "status").agg(
    F.count("job_id").alias("nb_jobs"),
    F.round(F.avg("duration_minutes"), 2).alias("avg_duration_min"),
).orderBy("name", "status")

# Jobs failed avec durée
jobs_failed = jobs.filter(F.col("status") == "failed") \
    .filter(F.col("duration_minutes").isNotNull())

# Jobs success avec durée
jobs_success = jobs.filter(F.col("status") == "success") \
    .filter(F.col("duration_minutes").isNotNull())

# MTTR via jobs : pour chaque pipeline failed, trouver le prochain success
# Approximation : MTTR = avg durée pipeline failed + avg durée pipeline fix
avg_failed_duration = pipelines.filter(F.col("status") == "failed") \
    .agg(F.round(F.avg("duration_minutes") / 60, 2).alias("v")).collect()[0]["v"] or 0

avg_success_duration = pipelines.filter(F.col("status") == "success") \
    .agg(F.round(F.avg("duration_minutes") / 60, 2).alias("v")).collect()[0]["v"] or 0

# MTTR jobs = temps moyen pour corriger un job failed
# = durée avg job failed + durée avg job success de fix
nb_failed_jobs   = jobs_failed.count()
nb_success_jobs  = jobs_success.count()
avg_job_fail_min = jobs_failed.agg(F.round(F.avg("duration_minutes"), 2).alias("v")).collect()[0]["v"] or 0
avg_job_fix_min  = jobs_success.agg(F.round(F.avg("duration_minutes"), 2).alias("v")).collect()[0]["v"] or 0

mttr_jobs_hours = round((avg_job_fail_min + avg_job_fix_min) / 60, 2)

# MTTR global combiné
mttr_jira_val = bugs_jira["mttr_jira"] or 0
mttr_jobs_val = mttr_jobs_hours

if mttr_jira_val and mttr_jobs_val:
    mttr_global = round((mttr_jira_val + mttr_jobs_val) / 2, 2)
elif mttr_jira_val:
    mttr_global = mttr_jira_val
else:
    mttr_global = mttr_jobs_val

if mttr_global <= 1:
    mttr_level = "Elite"
elif mttr_global <= 24:
    mttr_level = "High"
elif mttr_global <= 168:
    mttr_level = "Medium"
else:
    mttr_level = "Low"

print(f"   [jira+git]  MTTR bugs Jira       : {mttr_jira_val}h ({bugs_jira['nb_bugs']} bugs)")
print(f"   [git_only]  MTTR via jobs        : {mttr_jobs_val}h")
print(f"     → Jobs failed   : {nb_failed_jobs} | avg durée : {round(avg_job_fail_min,1)} min")
print(f"     → Jobs success  : {nb_success_jobs} | avg durée : {round(avg_job_fix_min,1)} min")
print(f"   [global]    MTTR combiné         : {mttr_global}h → {mttr_level}")
print()


# ══════════════════════════════════════════════════════════════
# SAUVEGARDE — Résumé DORA
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("6. Sauvegarde des résultats")
print("=" * 60)

# Summary global
summary_rows = [
    {"metric": "Lead Time for Changes",  "source": "jira+git",
     "value": lt_jira["avg_lt"],    "unit": "heures", "dora_level": lt_level},
    {"metric": "Lead Time (git_only)",   "source": "git_only",
     "value": lt_git["avg_lt"],     "unit": "heures", "dora_level": lt_level},
    {"metric": "Lead Time (global)",     "source": "hybride",
     "value": lt_global_avg,        "unit": "heures", "dora_level": lt_level},
    {"metric": "Deployment Frequency",   "source": "git",
     "value": deploy_freq,          "unit": "/semaine", "dora_level": df_level},
    {"metric": "Change Failure Rate",    "source": "git",
     "value": cfr,                  "unit": "%",      "dora_level": cfr_level},
    {"metric": "MTTR (jira+git)",        "source": "jira+git",
     "value": mttr_jira_val,        "unit": "heures", "dora_level": mttr_level},
    {"metric": "MTTR (git_only/jobs)",   "source": "git_only",
     "value": mttr_jobs_val,        "unit": "heures", "dora_level": mttr_level},
    {"metric": "MTTR (global)",          "source": "hybride",
     "value": mttr_global,          "unit": "heures", "dora_level": mttr_level},
]
pd.DataFrame(summary_rows).to_csv(
    METRICS + "dora_metrics_summary_spark.csv", index=False, encoding="utf-8-sig"
)
print(f"   ✅ dora_metrics_summary_spark.csv")

# Statistiques par source
by_source = pd.DataFrame([
    {"data_source": "jira+git",
     "nb_items":       lt_jira["nb_tickets"],
     "avg_lead_time":  lt_jira["avg_lt"],
     "median_lead_time": lt_jira["median_lt"],
     "description":    "Tickets Jira avec commits Git liés"},
    {"data_source": "git_only",
     "nb_items":       lt_git["nb_mrs"],
     "avg_lead_time":  lt_git["avg_lt"],
     "median_lead_time": lt_git["median_lt"],
     "description":    "MRs Git sans ticket Jira (MWBR + autres)"},
])
by_source.to_csv(METRICS + "dora_by_source_spark.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ dora_by_source_spark.csv")

# KPIs
kpis = pd.DataFrame([{
    "deploy_freq_per_week":     deploy_freq,
    "deploy_freq_level":        df_level,
    "lead_time_jira_hours":     lt_jira["avg_lt"],
    "lead_time_git_hours":      lt_git["avg_lt"],
    "lead_time_global_hours":   lt_global_avg,
    "lead_time_level":          lt_level,
    "cfr_pct":                  cfr,
    "cfr_level":                cfr_level,
    "mttr_jira_hours":          mttr_jira_val,
    "mttr_jobs_hours":          mttr_jobs_val,
    "mttr_global_hours":        mttr_global,
    "mttr_level":               mttr_level,
    "commits_jira_git":         n_jira_git,
    "commits_git_only":         n_git_only,
    "total_deploys":            total_deploys,
    "total_pipelines":          total_pipes,
    "failed_pipelines":         failed_pipes,
    "nb_bugs_jira":             bugs_jira["nb_bugs"],
    "nb_failed_jobs":           nb_failed_jobs,
    "nb_success_jobs":          nb_success_jobs,
    "avg_job_fail_min":         round(avg_job_fail_min, 2),
    "avg_job_fix_min":          round(avg_job_fix_min, 2),
}])
kpis.to_csv(METRICS + "dora_kpis_spark.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ dora_kpis_spark.csv")

# Évolution hebdomadaire
weekly = pipelines.withColumn(
    "year", F.year(F.col("created_at"))
).withColumn(
    "week_num", F.weekofyear(F.col("created_at"))
).withColumn(
    "week", F.concat(
        F.col("year").cast("string"),
        F.lit("-W"),
        F.lpad(F.col("week_num").cast("string"), 2, "0")
    )
).groupBy("week").agg(
    F.count("pipeline_id").alias("total_pipelines"),
    F.sum(F.when(F.col("status")=="success",1).otherwise(0)).alias("success_pipelines"),
    F.sum(F.when(F.col("status")=="failed",1).otherwise(0)).alias("failed_pipelines"),
    F.round(
        F.sum(F.when(F.col("status")=="failed",1).otherwise(0)).cast(DoubleType()) /
        F.count("pipeline_id") * 100, 2
    ).alias("change_failure_rate"),
    F.sum(F.when(F.col("is_production").isin("true", "True"),1).otherwise(0)).alias("deployment_count"),
).orderBy("week")
weekly.toPandas().to_csv(METRICS + "dora_metrics_weekly_spark.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ dora_metrics_weekly_spark.csv")

# Par projet — phase réelle basée sur la couverture Git
by_project = tickets.withColumn(
    "nb_commits", F.col("nb_commits").cast(DoubleType())
).groupBy("ProjectKey").agg(
    F.count("TicketKey").alias("nb_tickets"),
    F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0)).alias("nb_deployed"),
    F.round(F.avg("lead_time_hours"), 2).alias("avg_lead_time_h"),
    F.round(F.percentile_approx("lead_time_hours", 0.5), 2).alias("median_lead_time_h"),
    F.sum("is_delayed").alias("nb_delayed"),
    F.round(F.sum("is_delayed").cast(DoubleType()) / F.count("TicketKey") * 100, 1).alias("delay_rate_pct"),
    F.sum(F.when(F.col("nb_commits") > 0, 1).otherwise(0)).alias("nb_tickets_with_git"),
    F.round(
        F.sum(F.when(F.col("nb_commits") > 0, 1).otherwise(0)).cast(DoubleType()) /
        F.count("TicketKey") * 100, 1
    ).alias("git_coverage_pct"),
).withColumn(
    # Phase 1 : 0% Git  → jira_only
    # Phase 2 : >0% Git → jira+git (partiel ou complet)
    "data_source",
    F.when(F.col("git_coverage_pct") == 0, "jira_only").otherwise("jira+git")
).withColumn(
    # Phase projet selon avancement Git
    "project_phase",
    F.when(F.col("git_coverage_pct") == 0,   "Phase 1 - Jira only")
     .when(F.col("git_coverage_pct") < 50,   "Phase 2 - Transition")
     .otherwise(                              "Phase 2 - Jira+Git")
).withColumn(
    # Métriques DORA disponibles selon la phase
    "dora_available",
    F.when(F.col("data_source") == "jira_only",
           "Lead Time uniquement (pas de Git)")
     .otherwise("Lead Time + Deployment Freq + CFR + MTTR")
)
by_project.toPandas().to_csv(METRICS + "dora_by_project_spark.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ dora_by_project_spark.csv")

# Statistiques jobs (nouveau fichier)
jobs_stats.toPandas().to_csv(METRICS + "jobs_stats_spark.csv", index=False, encoding="utf-8-sig")
print(f"   ✅ jobs_stats_spark.csv")

print()
print("=" * 60)
print("RÉSUMÉ FINAL — MÉTRIQUES DORA HYBRIDES")
print("=" * 60)
print()
print(f"  {'Métrique':<30} {'Valeur':<20} {'Source':<15} {'Niveau'}")
print(f"  {'-'*75}")
print(f"  {'Deployment Frequency':<30} {str(deploy_freq)+' /sem':<20} {'Git':<15} {df_level}")
print(f"  {'Lead Time (jira+git)':<30} {str(lt_jira['avg_lt'])+'h':<20} {'Jira+Git':<15} {lt_level}")
print(f"  {'Lead Time (git_only)':<30} {str(lt_git['avg_lt'])+'h':<20} {'Git only':<15} {lt_level}")
print(f"  {'Lead Time (global)':<30} {str(lt_global_avg)+'h':<20} {'Hybride':<15} {lt_level}")
print(f"  {'Change Failure Rate':<30} {str(cfr)+'%':<20} {'Git':<15} {cfr_level}")
print(f"  {'MTTR (jira+git)':<30} {str(mttr_jira_val)+'h':<20} {'Jira+Git':<15} {mttr_level}")
print(f"  {'MTTR (git/jobs)':<30} {str(mttr_jobs_val)+'h':<20} {'Jobs Git':<15} {mttr_level}")
print(f"  {'MTTR (global)':<30} {str(mttr_global)+'h':<20} {'Hybride':<15} {mttr_level}")
print()
print("✅ Script 03 Hybride terminé.")
print("   Prochaine étape : 04_team_metrics_spark.py")

spark.stop()