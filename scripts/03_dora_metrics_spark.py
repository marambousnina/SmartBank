"""
=============================================================
 DORA Metrics Project — VERSION DISTRIBUEE (Git + Jira)
 Script  : 03_dora_metrics_spark.py
 Etape   : 3 — Calcul DORA distribue avec PySpark

 Logique hybride :
   jira+git  -> Lead Time = Created(Jira) -> Deploy(Git)
               MTTR = Bug(Jira) -> Fix commit(Git)
   git_only  -> Lead Time = Commit -> Merge MR
               MTTR = Revert commit -> Hotfix commit

 Modifications vs version precedente :
   [1] Master Spark configurable via config.yaml ou SPARK_MASTER_URL
   [2] Aggregations combinees -> moins de jobs Spark (collect unique)
   [3] Cache/persist sur les DataFrames reutilises
   [4] Sorties via spark.createDataFrame() + write Spark natif
   [5] Shuffle partitions adaptes au volume

 Configuration du cluster (config.yaml) :
   spark:
     master: spark://host:7077   # cluster standalone
     master: yarn                 # Hadoop
     master: local[*]             # developpement local
     executor_memory: 4g
     driver_memory: 4g
     shuffle_partitions: 8

 Sorties :
   data/metrics/dora_metrics_summary_spark.csv
   data/metrics/dora_metrics_weekly_spark.csv
   data/metrics/dora_kpis_spark.csv
   data/metrics/dora_by_project_spark.csv
   data/metrics/dora_by_source_spark.csv
   data/metrics/jobs_stats_spark.csv
=============================================================
"""

import os
import sys
import shutil
import yaml
from pathlib import Path

# Fix Windows: Spark workers must use the same Python executable as the driver.
# If the path contains spaces, use the Windows 8.3 short path to avoid worker crash.
def _get_python_exec():
    if sys.platform == "win32":
        try:
            import ctypes
            buf = ctypes.create_unicode_buffer(512)
            ctypes.windll.kernel32.GetShortPathNameW(sys.executable, buf, 512)
            short = buf.value
            return short if short else sys.executable
        except Exception:
            pass
    return sys.executable

_py_exec = _get_python_exec()
os.environ["PYSPARK_PYTHON"] = _py_exec
os.environ["PYSPARK_DRIVER_PYTHON"] = _py_exec

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-java-options -Djava.security.manager=allow pyspark-shell"
)

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType,
    StructType, StructField, LongType
)

# ══════════════════════════════════════════════════════════════
# 0. CONFIGURATION — lecture depuis config.yaml + env vars
# ══════════════════════════════════════════════════════════════
ROOT_DIR    = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT_DIR / "config" / "config.yaml"

def load_spark_config() -> dict:
    defaults = {
        "master":             "local[*]",
        "driver_memory":      "2g",
        "executor_memory":    "2g",
        "shuffle_partitions": "8",
    }
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8-sig", errors="ignore") as f:
            cfg = yaml.safe_load(f) or {}
        defaults.update(cfg.get("spark", {}))

    # Variables d'environnement prioritaires
    if os.environ.get("SPARK_MASTER_URL"):
        defaults["master"] = os.environ["SPARK_MASTER_URL"]

    return defaults

spark_cfg = load_spark_config()
SPARK_MASTER = str(spark_cfg["master"])

print("=" * 60)
print("  Script 03 — DORA Metrics (traitement distribue)")
print("=" * 60)
print(f"  Mode Spark : {SPARK_MASTER}")
print(f"  Driver mem : {spark_cfg['driver_memory']}")
print()

# ══════════════════════════════════════════════════════════════
# 1. SESSION SPARK — configurable cluster / local
# ══════════════════════════════════════════════════════════════
builder = (
    SparkSession.builder
    .appName("DORA_Hybrid_Metrics_Distributed")
    .master(SPARK_MASTER)
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
    .config("spark.driver.memory",            spark_cfg["driver_memory"])
    .config("spark.executor.memory",          spark_cfg["executor_memory"])
    # Shuffle partitions adaptes au volume de donnees
    # local[*] -> 8 suffit  |  cluster -> augmenter (200 defaut Spark)
    .config("spark.sql.shuffle.partitions",   str(spark_cfg["shuffle_partitions"]))
    # Optimisations broadcast pour petits DataFrames
    .config("spark.sql.autoBroadcastJoinThreshold", "10mb")
    # Adaptive Query Execution (Spark 3+)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.python.worker.faulthandler.enabled", "true")
)

# Configs supplementaires cluster uniquement
if SPARK_MASTER not in ("local", "local[*]") and not SPARK_MASTER.startswith("local["):
    builder = (
        builder
        .config("spark.executor.cores", "2")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "8")
        .config("spark.shuffle.service.enabled", "true")
    )

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ══════════════════════════════════════════════════════════════
# 2. CHEMINS
# ══════════════════════════════════════════════════════════════
CLEANED  = "data/cleaned/"
FEATURES = "data/features/"
METRICS  = "data/metrics/"
os.makedirs(METRICS, exist_ok=True)

# ══════════════════════════════════════════════════════════════
# 3. HELPER — ecriture CSV via Spark (fichier unique)
# ══════════════════════════════════════════════════════════════
def write_csv(df, path: str):
    """
    Ecrit un Spark DataFrame en CSV fichier unique.
    - Linux/Mac/Cluster : ecriture native Spark (distribue)
    - Windows local     : fallback toPandas() (winutils absent)
    Dans les deux cas, les CALCULS restent distribues sur le cluster.
    Seule la phase d'ecriture finale differe.
    """
    import platform
    on_windows = platform.system() == "Windows"

    if on_windows:
        # Fallback Windows : driver collecte les resultats (petits DFs)
        # et ecrit en local — les calculs Spark precedents sont distribues
        df.toPandas().to_csv(path, index=False, encoding="utf-8-sig")
    else:
        # Cluster Linux/Mac : ecriture distribuee native Spark
        tmp = path + "_tmp"
        if os.path.exists(tmp):
            shutil.rmtree(tmp)
        df.coalesce(1).write.mode("overwrite") \
          .option("header", "true").option("encoding", "UTF-8").csv(tmp)
        part_files = [f for f in os.listdir(tmp) if f.startswith("part-")]
        if part_files:
            shutil.move(os.path.join(tmp, part_files[0]), path)
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"  [OK] {os.path.basename(path)}")

# ══════════════════════════════════════════════════════════════
# 4. CHARGEMENT — lecture parallele sur le cluster
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("Chargement distribue des donnees")
print("=" * 60)

def to_ts(df, cols):
    """Parse plusieurs formats de date en parallele sur le cluster."""
    for c in cols:
        if c not in df.columns:
            continue
        cl = F.regexp_replace(F.col(c), r"[+-]\d{2}:\d{2}$|Z$", "")
        df = df.withColumn(c, F.coalesce(
            F.try_to_timestamp(cl, F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp(cl, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS")),
            F.try_to_timestamp(cl, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
            F.try_to_timestamp(cl, F.lit("dd/MM/yyyy HH:mm")),
            F.try_to_timestamp(cl, F.lit("yyyy-MM-dd")),
        ))
    return df

# Lecture CSV -> distribue sur les workers du cluster
tickets   = spark.read.csv(FEATURES + "tickets_features_spark.csv",   header=True, inferSchema=False)
commits   = spark.read.csv(CLEANED  + "commits_cleaned_spark.csv",    header=True, inferSchema=False)
pipelines = spark.read.csv(CLEANED  + "pipelines_cleaned_spark.csv",  header=True, inferSchema=False)
mrs       = spark.read.csv(CLEANED  + "merge_requests_cleaned_spark.csv", header=True, inferSchema=False)
jobs      = spark.read.csv(CLEANED  + "jobs_cleaned_spark.csv",        header=True, inferSchema=False)

# Cast types + parsing dates — transformations distribuees
tickets   = to_ts(tickets.withColumn("lead_time_hours", F.col("lead_time_hours").cast(DoubleType()))
                         .withColumn("is_delayed",       F.col("is_delayed").cast(IntegerType())),
                  ["Created", "ResolutionDate"])

pipelines = to_ts(pipelines.withColumn("duration_minutes", F.col("duration_minutes").cast(DoubleType())),
                  ["created_at", "updated_at"])

mrs       = to_ts(mrs.withColumn("merge_time_hours", F.col("merge_time_hours").cast(DoubleType())),
                  ["created_at", "merged_at"])

jobs      = to_ts(jobs.withColumn("duration_minutes", F.col("duration_minutes").cast(DoubleType())),
                  ["started_at", "finished_at"])

commits   = to_ts(commits, ["date"])

# ── CACHE des DataFrames reutilises plusieurs fois ────────────
# Sans cache : Spark relit et recalcule depuis le debut a chaque action
# Avec cache : les partitions sont gardees en memoire sur les workers
pipelines.cache()
jobs.cache()
tickets.cache()
commits.cache()

print("  [OK] DataFrames charges et mis en cache")
print()

# ══════════════════════════════════════════════════════════════
# 5. CLASSIFICATION DATA_SOURCE — transformation distribuee
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("Etape 1 — Classification par data_source")
print("=" * 60)

JIRA_KEY_PATTERN = r"([A-Z]+-\d+)"

# Classification commits : jira+git ou git_only
commits = commits.withColumn(
    "jira_ref",
    F.when(F.regexp_extract(F.col("title"), JIRA_KEY_PATTERN, 1) != "",
           F.regexp_extract(F.col("title"), JIRA_KEY_PATTERN, 1))
     .otherwise(F.lit(None).cast(StringType()))
).withColumn(
    "data_source",
    F.when(F.col("jira_ref").isNotNull(), "jira+git").otherwise("git_only")
)

# Classification MRs : meme logique
mrs = mrs.withColumn(
    "jira_ref",
    F.when(F.regexp_extract(F.col("title"), JIRA_KEY_PATTERN, 1) != "",
           F.regexp_extract(F.col("title"), JIRA_KEY_PATTERN, 1))
     .otherwise(F.lit(None).cast(StringType()))
).withColumn(
    "data_source",
    F.when(F.col("jira_ref").isNotNull(), "jira+git").otherwise("git_only")
)

# AMELIORATION : 1 seul job Spark au lieu de 2 .count() separés
# Ancienne version : 2 actions = 2 jobs Spark
#   n_jira_git = commits.filter(...).count()   <- job 1
#   n_git_only = commits.filter(...).count()   <- job 2
# Nouvelle version : 1 seul groupBy = 1 job Spark
commits_by_source = (
    commits.groupBy("data_source")
           .count()
           .collect()
)
source_counts = {r["data_source"]: r["count"] for r in commits_by_source}
n_jira_git = source_counts.get("jira+git", 0)
n_git_only = source_counts.get("git_only", 0)

print(f"  Commits jira+git : {n_jira_git}")
print(f"  Commits git_only : {n_git_only}")
print()

# ══════════════════════════════════════════════════════════════
# 6. METRIQUES DORA — aggregations combinees
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("Etape 2 — Calcul des 4 metriques DORA")
print("=" * 60)

# ── AMELIORATION : toutes les aggregations pipelines en 1 seul job ──
# Ancienne version : 6 actions séparées sur pipelines
#   total_deploys = prod_deploys.count()            <- job 1
#   dates_range   = pipelines.agg(min,max).collect() <- job 2
#   total_pipes   = pipelines.count()               <- job 3
#   failed_pipes  = pipelines.filter(...).count()   <- job 4
#   avg_failed_duration = pipelines.filter(...).agg <- job 5
#   avg_success_duration = pipelines.filter(...).agg <- job 6
#
# Nouvelle version : 1 seul agg = 1 job Spark
pipeline_agg = pipelines.agg(
    F.count("pipeline_id").alias("total_pipes"),
    F.sum(F.when(F.col("status") == "failed", 1).otherwise(0))
     .alias("failed_pipes"),
    F.sum(F.when(F.col("is_production").isin("true", "True", True), 1).otherwise(0))
     .alias("total_deploys"),
    F.min("created_at").alias("min_date"),
    F.max("created_at").alias("max_date"),
    F.round(
        F.avg(F.when(F.col("status") == "failed", F.col("duration_minutes"))) / 60, 2
    ).alias("avg_failed_duration_h"),
    F.round(
        F.avg(F.when(F.col("status") == "success", F.col("duration_minutes"))) / 60, 2
    ).alias("avg_success_duration_h"),
).collect()[0]  # <- 1 seul .collect() pour toutes les stats pipelines

total_pipes          = pipeline_agg["total_pipes"]
failed_pipes         = pipeline_agg["failed_pipes"]
total_deploys        = pipeline_agg["total_deploys"]
avg_failed_dur_h     = pipeline_agg["avg_failed_duration_h"] or 0
avg_success_dur_h    = pipeline_agg["avg_success_duration_h"] or 0

# Calcul semaines
if pipeline_agg["min_date"] and pipeline_agg["max_date"]:
    weeks = max(1, (pipeline_agg["max_date"] - pipeline_agg["min_date"]).days / 7)
else:
    weeks = 52

deploy_freq = round(total_deploys / weeks, 2)
cfr         = round(failed_pipes / total_pipes * 100, 2) if total_pipes > 0 else 0.0

# Niveaux DORA
def dora_deploy_level(freq):
    if freq >= 1:      return "Elite"
    if freq >= 1/7:    return "High"
    if freq >= 1/30:   return "Medium"
    return "Low"

def dora_lt_level(hours):
    if hours <= 24:    return "Elite"
    if hours <= 168:   return "High"
    if hours <= 720:   return "Medium"
    return "Low"

def dora_cfr_level(pct):
    if pct <= 5:       return "Elite"
    if pct <= 10:      return "High"
    if pct <= 15:      return "Medium"
    return "Low"

def dora_mttr_level(hours):
    if hours <= 1:     return "Elite"
    if hours <= 24:    return "High"
    if hours <= 168:   return "Medium"
    return "Low"

df_level  = dora_deploy_level(deploy_freq)
cfr_level = dora_cfr_level(cfr)

print(f"  [Deployment Frequency] {deploy_freq}/sem -> {df_level}")
print(f"  [Change Failure Rate]  {cfr}%            -> {cfr_level}")

# ── Lead Time : 2 agg separees (sources differentes) -> 1 job chacune ──
# AMELIORATION : tickets_agg combine MTTR + Lead Time en 1 seul job
#   Ancienne version :
#     lt_jira    = tickets.filter(...).agg(...).collect()[0]  <- job 1
#     bugs_jira  = tickets.filter(...).agg(...).collect()[0]  <- job 2
#   Nouvelle version : 1 seul agg conditionnel sur tickets
tickets_agg = tickets.agg(
    # Lead Time (tous les tickets avec valeur)
    F.round(F.avg(F.when(F.col("lead_time_hours").isNotNull(),
                         F.col("lead_time_hours"))), 2)
     .alias("avg_lt_jira"),
    F.round(F.percentile_approx(
        F.when(F.col("lead_time_hours").isNotNull(), F.col("lead_time_hours")),
        0.5), 2)
     .alias("median_lt_jira"),
    F.count(F.when(F.col("lead_time_hours").isNotNull(), 1))
     .alias("nb_tickets_lt"),
    # MTTR = bugs Jira avec lead_time
    F.round(F.avg(
        F.when(F.col("IssueType").isin("Bug", "bug") &
               F.col("lead_time_hours").isNotNull(),
               F.col("lead_time_hours"))
    ), 2).alias("mttr_jira"),
    F.count(F.when(F.col("IssueType").isin("Bug", "bug") &
                   F.col("lead_time_hours").isNotNull(), 1))
     .alias("nb_bugs"),
).collect()[0]  # <- 1 seul .collect() pour tous les KPIs tickets

avg_lt_jira   = tickets_agg["avg_lt_jira"]   or 0.0
median_lt_jira = tickets_agg["median_lt_jira"] or 0.0
nb_tickets_lt = tickets_agg["nb_tickets_lt"]
mttr_jira_val = tickets_agg["mttr_jira"]     or 0.0
nb_bugs       = tickets_agg["nb_bugs"]

# Lead Time git_only (MRs sans ticket Jira, merged)
lt_git_agg = mrs.filter(
    (F.col("data_source") == "git_only") &
    F.col("merge_time_hours").isNotNull() &
    (F.col("state") == "merged")
).agg(
    F.round(F.avg("merge_time_hours"), 2).alias("avg_lt"),
    F.round(F.percentile_approx("merge_time_hours", 0.5), 2).alias("median_lt"),
    F.count("mr_id").alias("nb_mrs"),
).collect()[0]  # <- 1 seul .collect() pour les MRs

avg_lt_git   = lt_git_agg["avg_lt"]  or 0.0
median_lt_git = lt_git_agg["median_lt"] or 0.0
nb_mrs_git   = lt_git_agg["nb_mrs"]

# Lead Time global pondere
total_items  = nb_tickets_lt + nb_mrs_git
lt_global    = round(
    (avg_lt_jira * nb_tickets_lt + avg_lt_git * nb_mrs_git) / max(total_items, 1),
    2
)
lt_level     = dora_lt_level(lt_global)

print(f"  [Lead Time jira+git]   {avg_lt_jira}h ({nb_tickets_lt} tickets)")
print(f"  [Lead Time git_only]   {avg_lt_git}h ({nb_mrs_git} MRs)")
print(f"  [Lead Time global]     {lt_global}h  -> {lt_level}")

# ── MTTR Jobs : 1 agg conditionnelle au lieu de 4 .collect() ──
# Ancienne version :
#   nb_failed_jobs   = jobs_failed.count()            <- job 1
#   nb_success_jobs  = jobs_success.count()           <- job 2
#   avg_job_fail_min = jobs_failed.agg(...).collect() <- job 3
#   avg_job_fix_min  = jobs_success.agg(...).collect() <- job 4
#
# Nouvelle version : 1 seul agg conditionnel = 1 job Spark
jobs_agg = jobs.agg(
    F.count(F.when(F.col("status") == "failed", 1)).alias("nb_failed"),
    F.count(F.when(F.col("status") == "success", 1)).alias("nb_success"),
    F.round(F.avg(F.when(F.col("status") == "failed",
                         F.col("duration_minutes"))), 2).alias("avg_fail_min"),
    F.round(F.avg(F.when(F.col("status") == "success",
                         F.col("duration_minutes"))), 2).alias("avg_fix_min"),
).collect()[0]  # <- 1 seul .collect() pour tous les KPIs jobs

nb_failed_jobs   = jobs_agg["nb_failed"]
nb_success_jobs  = jobs_agg["nb_success"]
avg_job_fail_min = jobs_agg["avg_fail_min"] or 0.0
avg_job_fix_min  = jobs_agg["avg_fix_min"]  or 0.0
mttr_jobs_hours  = round((avg_job_fail_min + avg_job_fix_min) / 60, 2)

# Commits revert/hotfix (1 seul filter + count)
n_fail_commits = commits.filter(
    F.lower(F.col("title")).rlike(r"revert|hotfix|rollback|fix.prod")
).count()

# MTTR global
if mttr_jira_val and mttr_jobs_hours:
    mttr_global = round((mttr_jira_val + mttr_jobs_hours) / 2, 2)
else:
    mttr_global = mttr_jira_val or mttr_jobs_hours

mttr_level = dora_mttr_level(mttr_global)

print(f"  [MTTR jira+git]        {mttr_jira_val}h ({nb_bugs} bugs)")
print(f"  [MTTR git/jobs]        {mttr_jobs_hours}h")
print(f"  [MTTR global]          {mttr_global}h   -> {mttr_level}")
print()

# ══════════════════════════════════════════════════════════════
# 7. DATAFRAMES DISTRIBUES POUR LES SORTIES VOLUMINEUSES
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("Etape 3 — Construction des DataFrames de sortie distribues")
print("=" * 60)

# ── Weekly trends — reste Spark : agregation sur 6000+ lignes ──
weekly = (
    pipelines
    .withColumn("week", F.date_format(F.col("created_at"), "yyyy-'W'ww"))
    .groupBy("week")
    .agg(
        F.count("pipeline_id").alias("total_pipelines"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_pipelines"),
        F.sum(F.when(F.col("status") == "failed",  1).otherwise(0)).alias("failed_pipelines"),
        F.round(
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).cast(DoubleType()) /
            F.count("pipeline_id") * 100, 2
        ).alias("change_failure_rate"),
        F.sum(F.when(F.col("is_production").isin("true", "True", True), 1).otherwise(0))
         .alias("deployment_count"),
    )
    .orderBy("week")
)

# ── By project — reste Spark : agregation sur les tickets ──────
by_project = (
    tickets
    .groupBy("ProjectKey")
    .agg(
        F.count("TicketKey").alias("nb_tickets"),
        F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0)).alias("nb_deployed"),
        F.round(F.avg("lead_time_hours"), 2).alias("avg_lead_time_h"),
        F.round(F.percentile_approx("lead_time_hours", 0.5), 2).alias("median_lead_time_h"),
        F.sum("is_delayed").alias("nb_delayed"),
        F.round(
            F.sum("is_delayed").cast(DoubleType()) / F.count("TicketKey") * 100, 1
        ).alias("delay_rate_pct"),
    )
    .withColumn("data_source", F.lit("jira+git"))
)

# ── Jobs stats — reste Spark : agregation sur 25k+ lignes ──────
jobs_stats = (
    jobs
    .groupBy("name", "status")
    .agg(
        F.count("job_id").alias("nb_jobs"),
        F.round(F.avg("duration_minutes"), 2).alias("avg_duration_min"),
    )
    .orderBy("name", "status")
)

# ── Petits DataFrames (1-8 lignes) -> spark.createDataFrame ─────
# AMELIORATION : spark.createDataFrame() au lieu de pd.DataFrame()
# -> reste dans le contexte Spark, peut etre distribue si besoin

summary_schema = StructType([
    StructField("metric",     StringType(), True),
    StructField("source",     StringType(), True),
    StructField("value",      DoubleType(), True),
    StructField("unit",       StringType(), True),
    StructField("dora_level", StringType(), True),
])
summary_data = [
    Row("Lead Time for Changes", "jira+git", float(avg_lt_jira),    "heures",   lt_level),
    Row("Lead Time (git_only)",  "git_only", float(avg_lt_git),     "heures",   lt_level),
    Row("Lead Time (global)",    "hybride",  float(lt_global),      "heures",   lt_level),
    Row("Deployment Frequency",  "git",      float(deploy_freq),    "/semaine", df_level),
    Row("Change Failure Rate",   "git",      float(cfr),            "%",        cfr_level),
    Row("MTTR (jira+git)",       "jira+git", float(mttr_jira_val),  "heures",   mttr_level),
    Row("MTTR (git_only/jobs)",  "git_only", float(mttr_jobs_hours),"heures",   mttr_level),
    Row("MTTR (global)",         "hybride",  float(mttr_global),    "heures",   mttr_level),
]
summary_df = spark.createDataFrame(summary_data, schema=summary_schema)

by_source_schema = StructType([
    StructField("data_source",    StringType(), True),
    StructField("nb_items",       LongType(),   True),
    StructField("avg_lead_time",  DoubleType(), True),
    StructField("median_lead_time", DoubleType(), True),
    StructField("description",    StringType(), True),
])
by_source_df = spark.createDataFrame([
    Row("jira+git", int(nb_tickets_lt), float(avg_lt_jira),  float(median_lt_jira),
        "Tickets Jira avec commits Git lies"),
    Row("git_only", int(nb_mrs_git),    float(avg_lt_git),   float(median_lt_git),
        "MRs Git sans ticket Jira"),
], schema=by_source_schema)

kpis_schema = StructType([
    StructField("deploy_freq_per_week",   DoubleType(), True),
    StructField("deploy_freq_level",      StringType(), True),
    StructField("lead_time_jira_hours",   DoubleType(), True),
    StructField("lead_time_git_hours",    DoubleType(), True),
    StructField("lead_time_global_hours", DoubleType(), True),
    StructField("lead_time_level",        StringType(), True),
    StructField("cfr_pct",                DoubleType(), True),
    StructField("cfr_level",              StringType(), True),
    StructField("mttr_jira_hours",        DoubleType(), True),
    StructField("mttr_jobs_hours",        DoubleType(), True),
    StructField("mttr_global_hours",      DoubleType(), True),
    StructField("mttr_level",             StringType(), True),
    StructField("commits_jira_git",       LongType(),   True),
    StructField("commits_git_only",       LongType(),   True),
    StructField("total_deploys",          LongType(),   True),
    StructField("total_pipelines",        LongType(),   True),
    StructField("failed_pipelines",       LongType(),   True),
    StructField("nb_bugs_jira",           LongType(),   True),
    StructField("nb_failed_jobs",         LongType(),   True),
    StructField("nb_success_jobs",        LongType(),   True),
    StructField("avg_job_fail_min",       DoubleType(), True),
    StructField("avg_job_fix_min",        DoubleType(), True),
])
kpis_df = spark.createDataFrame([Row(
    float(deploy_freq), df_level,
    float(avg_lt_jira), float(avg_lt_git), float(lt_global), lt_level,
    float(cfr),         cfr_level,
    float(mttr_jira_val), float(mttr_jobs_hours), float(mttr_global), mttr_level,
    int(n_jira_git),    int(n_git_only),
    int(total_deploys), int(total_pipes), int(failed_pipes),
    int(nb_bugs),       int(nb_failed_jobs), int(nb_success_jobs),
    float(avg_job_fail_min), float(avg_job_fix_min),
)], schema=kpis_schema)

print("  [OK] DataFrames de sortie construits")
print()

# ══════════════════════════════════════════════════════════════
# 8. SAUVEGARDE — ecriture distribuee via Spark
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("Etape 4 — Sauvegarde distribuee")
print("=" * 60)

write_csv(summary_df, METRICS + "dora_metrics_summary_spark.csv")
write_csv(by_source_df, METRICS + "dora_by_source_spark.csv")
write_csv(kpis_df,     METRICS + "dora_kpis_spark.csv")
write_csv(weekly,      METRICS + "dora_metrics_weekly_spark.csv")
write_csv(by_project,  METRICS + "dora_by_project_spark.csv")
write_csv(jobs_stats,  METRICS + "jobs_stats_spark.csv")

# ══════════════════════════════════════════════════════════════
# 9. RAPPORT FINAL
# ══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("RESUME FINAL — METRIQUES DORA DISTRIBUEES")
print("=" * 60)
print()
print(f"  {'Metrique':<30} {'Valeur':<20} {'Source':<15} {'Niveau'}")
print(f"  {'-'*75}")
print(f"  {'Deployment Frequency':<30} {str(deploy_freq)+' /sem':<20} {'Git':<15} {df_level}")
print(f"  {'Lead Time (jira+git)':<30} {str(avg_lt_jira)+'h':<20} {'Jira+Git':<15} {lt_level}")
print(f"  {'Lead Time (git_only)':<30} {str(avg_lt_git)+'h':<20} {'Git only':<15} {lt_level}")
print(f"  {'Lead Time (global)':<30} {str(lt_global)+'h':<20} {'Hybride':<15} {lt_level}")
print(f"  {'Change Failure Rate':<30} {str(cfr)+'%':<20} {'Git':<15} {cfr_level}")
print(f"  {'MTTR (jira+git)':<30} {str(mttr_jira_val)+'h':<20} {'Jira+Git':<15} {mttr_level}")
print(f"  {'MTTR (git/jobs)':<30} {str(mttr_jobs_hours)+'h':<20} {'Jobs Git':<15} {mttr_level}")
print(f"  {'MTTR (global)':<30} {str(mttr_global)+'h':<20} {'Hybride':<15} {mttr_level}")
print()
print(f"  Mode execution : {SPARK_MASTER}")
print(f"  Jobs Spark reduits : 10 collect() -> 5 collect() combines")
print()
print("Script 03 distribue termine.")
print("Prochaine etape : 04_team_metrics_spark.py")

# Liberer le cache des workers
pipelines.unpersist()
jobs.unpersist()
tickets.unpersist()
commits.unpersist()

spark.stop()
