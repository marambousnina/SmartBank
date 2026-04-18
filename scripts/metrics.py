"""
=============================================================
 SmartBank Metrics -- Script unifie (metrics par periode)
 Script  : metrics.py

 Produit 4 fichiers :
   data/metrics/metrics_individual_w.csv  (par personne x semaine)
   data/metrics/metrics_individual_m.csv  (par personne x mois)
   data/metrics/metrics_project_w.csv     (par projet x semaine)
   data/metrics/metrics_project_m.csv     (par projet x mois)

 Colonnes metrics_individual_* (15) :
   id, person_id, calculation_timestamp,
   metrics_period_start, metrics_period_end,
   lead_time, change_failure_rate, mttr,
   tasks_completed, bugs_created, bugs_fixed,
   commit_count, prs_merged, cycle_time, productivity_score

 Colonnes metrics_project_* (15) :
   id, project_id, calculation_timestamp,
   metrics_period_start, metrics_period_end,
   lead_time, deployment_frequency, change_failure_rate,
   mttr, cycle_time, throughput, wip, bug_rate,
   velocity, code_coverage

 Sources :
   features/tickets_features_spark.csv
   cleaned/commits_cleaned_spark.csv
   cleaned/pipelines_cleaned_spark.csv
   cleaned/merge_requests_cleaned_spark.csv
   cleaned/jobs_cleaned_spark.csv
=============================================================
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import calendar

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# -- Fix Windows
def _get_python_exec():
    if sys.platform == "win32":
        try:
            import ctypes
            buf = ctypes.create_unicode_buffer(512)
            ctypes.windll.kernel32.GetShortPathNameW(sys.executable, buf, 512)
            return buf.value or sys.executable
        except Exception:
            pass
    return sys.executable

_py = _get_python_exec()
os.environ["PYSPARK_PYTHON"]        = _py
os.environ["PYSPARK_DRIVER_PYTHON"] = _py
os.environ["PYSPARK_SUBMIT_ARGS"]   = (
    "--driver-java-options -Djava.security.manager=allow pyspark-shell"
)

import pandas as pd
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# ══════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════
ROOT_DIR    = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT_DIR / "config" / "config.yaml"

def load_spark_config() -> dict:
    defaults = {"master": "local[*]", "driver_memory": "2g",
                "executor_memory": "2g", "shuffle_partitions": "8"}
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8-sig", errors="ignore") as f:
            cfg = yaml.safe_load(f) or {}
        defaults.update(cfg.get("spark", {}))
    if os.environ.get("SPARK_MASTER_URL"):
        defaults["master"] = os.environ["SPARK_MASTER_URL"]
    return defaults

spark_cfg    = load_spark_config()
SPARK_MASTER = str(spark_cfg["master"])

CLEANED  = str(ROOT_DIR / "data" / "cleaned")  + "/"
FEATURES = str(ROOT_DIR / "data" / "features") + "/"
METRICS  = str(ROOT_DIR / "data" / "metrics")  + "/"
os.makedirs(METRICS, exist_ok=True)

CALC_TS = datetime.now().strftime("%Y-%m-%d %H:%M")

print("=" * 60)
print("  SmartBank Metrics -- Calcul par Semaine & Mois")
print("=" * 60)
print(f"  Calcul : {CALC_TS}")
print(f"  Spark  : {SPARK_MASTER}")
print()

# ══════════════════════════════════════════════════════════════
# SESSION SPARK
# ══════════════════════════════════════════════════════════════
spark = (
    SparkSession.builder
    .appName("SmartBank_Metrics_Periodic")
    .master(SPARK_MASTER)
    .config("spark.driver.extraJavaOptions",   "-Djava.security.manager=allow")
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
    .config("spark.driver.memory",   spark_cfg["driver_memory"])
    .config("spark.executor.memory", spark_cfg["executor_memory"])
    .config("spark.sql.shuffle.partitions",          str(spark_cfg["shuffle_partitions"]))
    .config("spark.sql.autoBroadcastJoinThreshold",  "10mb")
    .config("spark.sql.adaptive.enabled",            "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.legacy.timeParserPolicy",     "LEGACY")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════
def to_pandas(spark_df) -> pd.DataFrame:
    rows = spark_df.collect()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([r.asDict() for r in rows])


def parse_ts(df, col_name):
    cl = F.regexp_replace(F.col(col_name), r"[+-]\d{2}:\d{2}$|Z$", "")
    return df.withColumn(col_name, F.coalesce(
        F.try_to_timestamp(cl, F.lit("yyyy-MM-dd HH:mm:ss.SSS")),
        F.try_to_timestamp(cl, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(cl, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS")),
        F.try_to_timestamp(cl, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(cl, F.lit("dd/MM/yyyy HH:mm")),
        F.try_to_timestamp(cl, F.lit("yyyy-MM-dd")),
    ))


def month_end(d: str) -> str:
    """Retourne le dernier jour du mois pour une date 'YYYY-MM-DD'."""
    dt = datetime.strptime(d, "%Y-%m-%d")
    last = calendar.monthrange(dt.year, dt.month)[1]
    return f"{dt.year}-{dt.month:02d}-{last:02d}"


COLS_IND = [
    "id", "person_id", "calculation_timestamp",
    "metrics_period_start", "metrics_period_end",
    "lead_time", "change_failure_rate", "mttr",
    "tasks_completed", "bugs_created", "bugs_fixed",
    "commit_count", "prs_merged", "cycle_time", "productivity_score",
]

COLS_PROJ = [
    "id", "project_id", "calculation_timestamp",
    "metrics_period_start", "metrics_period_end",
    "lead_time", "deployment_frequency", "change_failure_rate",
    "mttr", "cycle_time", "throughput", "wip", "bug_rate",
    "velocity", "code_coverage",
]


# ══════════════════════════════════════════════════════════════
# CHARGEMENT DES DONNEES
# ══════════════════════════════════════════════════════════════
print("Chargement des donnees...")

tickets   = spark.read.csv(FEATURES + "tickets_features_spark.csv",       header=True, inferSchema=False)
commits   = spark.read.csv(CLEANED  + "commits_cleaned_spark.csv",        header=True, inferSchema=False)
pipelines = spark.read.csv(CLEANED  + "pipelines_cleaned_spark.csv",      header=True, inferSchema=False)
mrs       = spark.read.csv(CLEANED  + "merge_requests_cleaned_spark.csv", header=True, inferSchema=False)
jobs      = spark.read.csv(CLEANED  + "jobs_cleaned_spark.csv",           header=True, inferSchema=False)

tickets = (tickets
    .withColumn("lead_time_hours",    F.col("lead_time_hours").cast(DoubleType()))
    .withColumn("cycle_time_hours",   F.col("cycle_time_hours").cast(DoubleType()))
    .withColumn("time_blocked_hours", F.col("time_blocked_hours").cast(DoubleType()))
    .withColumn("is_delayed",         F.col("is_delayed").cast(IntegerType()))
    .withColumn("is_bug",             F.col("is_bug").cast(IntegerType()))
    .withColumn("nb_commits",         F.col("nb_commits").cast(IntegerType()))
    .withColumn("nb_mrs",             F.col("nb_mrs").cast(IntegerType()))
)
tickets   = parse_ts(tickets,   "Created")
pipelines = parse_ts(pipelines, "created_at")
pipelines = pipelines.withColumn("duration_minutes", F.col("duration_minutes").cast(DoubleType()))
mrs       = parse_ts(mrs, "created_at")
mrs       = mrs.withColumn("merge_time_hours", F.col("merge_time_hours").cast(DoubleType()))
jobs      = parse_ts(parse_ts(jobs, "started_at"), "finished_at")
jobs      = jobs.withColumn("duration_minutes", F.col("duration_minutes").cast(DoubleType()))

# Ajout colonnes periode sur les tickets
JIRA_KEY_PATTERN = r"([A-Z]+-\d+)"

tickets = (
    tickets
    .withColumn("week_start",  F.date_trunc("week",  F.col("Created")).cast("date"))
    .withColumn("month_start", F.date_trunc("month", F.col("Created")).cast("date"))
)

# Commits avec project key
commits = (
    commits
    .withColumn("jira_ref",
        F.when(F.regexp_extract(F.col("title"), JIRA_KEY_PATTERN, 1) != "",
               F.regexp_extract(F.col("title"), JIRA_KEY_PATTERN, 1))
         .otherwise(F.lit(None).cast(StringType()))
    )
    .withColumn("proj_key",
        F.regexp_extract(F.coalesce(F.col("TicketKey"), F.col("jira_ref")),
                         r"([A-Z0-9]+)-\d+", 1)
    )
)

# Cache
tickets.cache()
pipelines.cache()
mrs.cache()
jobs.cache()
commits.cache()

print("  [OK] Donnees chargees")
print()


# ══════════════════════════════════════════════════════════════
# PRE-CALCUL : CFR + MTTR par semaine et par mois (global)
# ══════════════════════════════════════════════════════════════
print("Pre-calcul CFR et MTTR par periode...")

def compute_pipeline_stats(period_col):
    return to_pandas(
        pipelines
        .withColumn("period", F.date_trunc(period_col, F.col("created_at")).cast("date"))
        .groupBy("period")
        .agg(
            F.count("pipeline_id").alias("total_pipes"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_pipes"),
            F.sum(F.when(F.lower(F.col("is_production").cast("string")) == "true", 1)
                  .otherwise(0)).alias("deploys"),
        )
        .withColumn("cfr",
            F.round(F.col("failed_pipes").cast(DoubleType()) /
                    F.when(F.col("total_pipes") > 0, F.col("total_pipes")).otherwise(F.lit(1)) * 100, 2)
        )
    )

def compute_mttr_stats(period_col):
    mttr_jobs = to_pandas(
        jobs
        .withColumn("period", F.date_trunc(period_col, F.col("started_at")).cast("date"))
        .filter(F.col("period").isNotNull())
        .groupBy("period")
        .agg(
            F.round(F.avg(F.when(F.col("status") == "failed",  F.col("duration_minutes"))), 2)
             .alias("avg_fail_min"),
            F.round(F.avg(F.when(F.col("status") == "success", F.col("duration_minutes"))), 2)
             .alias("avg_fix_min"),
        )
        .withColumn("mttr",
            F.round((F.coalesce(F.col("avg_fail_min"), F.lit(0.0)) +
                     F.coalesce(F.col("avg_fix_min"),  F.lit(0.0))) / 60, 2)
        )
    )
    mttr_bugs = to_pandas(
        tickets
        .filter((F.col("is_bug") == 1) & F.col("lead_time_hours").isNotNull())
        .withColumn("period", F.date_trunc(period_col, F.col("Created")).cast("date"))
        .groupBy("period")
        .agg(F.round(F.avg("lead_time_hours"), 2).alias("mttr_jira"))
    )
    # Combine (l'un ou l'autre peut etre vide)
    if mttr_jobs.empty and mttr_bugs.empty:
        return pd.DataFrame(columns=["period", "mttr"])
    elif mttr_jobs.empty:
        mttr_bugs = mttr_bugs.rename(columns={"mttr_jira": "mttr"})
        return mttr_bugs[["period", "mttr"]]
    elif mttr_bugs.empty:
        return mttr_jobs[["period", "mttr"]]
    merged = mttr_jobs.merge(mttr_bugs, on="period", how="outer")
    merged["mttr"] = merged.apply(lambda r: round(
        ((r["mttr"] if pd.notna(r.get("mttr")) else 0) +
         (r["mttr_jira"] if pd.notna(r.get("mttr_jira")) else 0)) / 2, 2
    ), axis=1)
    return merged[["period", "mttr"]]

pipe_w = compute_pipeline_stats("week")
pipe_m = compute_pipeline_stats("month")
mttr_w = compute_mttr_stats("week")
mttr_m = compute_mttr_stats("month")

# Dicts de lookup  {period_str: valeur}
cfr_week  = dict(zip(pipe_w["period"].astype(str), pipe_w["cfr"]))
cfr_month = dict(zip(pipe_m["period"].astype(str), pipe_m["cfr"]))
mttr_week = dict(zip(mttr_w["period"].astype(str), mttr_w["mttr"]))
mttr_month= dict(zip(mttr_m["period"].astype(str), mttr_m["mttr"]))
dep_week  = dict(zip(pipe_w["period"].astype(str), pipe_w["deploys"]))
dep_month = dict(zip(pipe_m["period"].astype(str), pipe_m["deploys"]))

print("  [OK] CFR/MTTR pre-calcules")
print()


# ══════════════════════════════════════════════════════════════
# FONCTION GENERIQUE : metriques individuelles par periode
# ══════════════════════════════════════════════════════════════
def build_individual(period_col: str, cfr_map: dict, mttr_map: dict,
                     period_type: str) -> pd.DataFrame:
    """
    period_col  : "week_start" ou "month_start"
    period_type : "week" ou "month"
    """
    agg = to_pandas(
        tickets.groupBy("Assignee", period_col).agg(
            F.count("TicketKey").alias("nb_tickets_assigned"),
            F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0))
             .alias("nb_tickets_deployed"),
            F.sum(F.col("is_bug")).alias("nb_bugs_assigned"),
            F.sum(F.when(
                (F.col("is_bug") == 1) & (F.col("CurrentStatus") == "To Be Deployed"), 1
            ).otherwise(0)).alias("nb_bugs_resolved"),
            F.round(F.avg("lead_time_hours"),    2).alias("avg_lead_time_hours"),
            F.round(F.avg("cycle_time_hours"),   2).alias("avg_cycle_time_hours"),
            F.round(F.avg("time_blocked_hours"), 2).alias("avg_time_blocked_hours"),
            F.round(
                F.sum(F.when(F.col("is_delayed") == 0, 1).otherwise(0)) /
                F.count("TicketKey") * 100, 1
            ).alias("on_time_rate"),
            F.sum(F.col("nb_commits")).alias("nb_commits_linked"),
            F.sum(F.col("nb_mrs")).alias("nb_mrs_linked"),
        )
    )

    if agg.empty:
        return pd.DataFrame(columns=COLS_IND)

    agg = agg.rename(columns={period_col: "period"})
    agg["period"] = agg["period"].astype(str)

    # Contraintes qualite
    agg["nb_mrs_linked"]     = agg["nb_mrs_linked"].fillna(0).astype(int)
    agg["nb_commits_linked"] = agg["nb_commits_linked"].fillna(0).astype(int)
    agg["nb_mrs_linked"]     = agg[["nb_mrs_linked", "nb_commits_linked"]].min(axis=1)
    agg["nb_bugs_assigned"]  = agg["nb_bugs_assigned"].fillna(0).astype(int)
    agg["nb_bugs_resolved"]  = agg["nb_bugs_resolved"].fillna(0).astype(int)
    agg["nb_bugs_resolved"]  = agg[["nb_bugs_resolved", "nb_bugs_assigned"]].min(axis=1)

    # CFR et MTTR depuis les dicts pre-calcules
    agg["cfr"]  = agg["period"].map(cfr_map).fillna(0.0)
    agg["mttr"] = agg["period"].map(mttr_map).fillna(0.0)

    # Score de performance (meme formule que script 04)
    def perf_score(r):
        score  = (r["on_time_rate"] or 0) * 0.4
        lt     = r["avg_lead_time_hours"] or 9999
        if   lt < 24:   score += 30
        elif lt < 168:  score += 20
        elif lt < 720:  score += 10
        blocked = r["avg_time_blocked_hours"] or 0
        if   blocked == 0: score += 20
        elif blocked < 24: score += 10
        dep = r["nb_tickets_deployed"] or 0
        if   dep >= 10: score += 10
        elif dep >= 5:  score += 5
        return round(min(score, 100.0), 1)

    agg["performance_score"] = agg.apply(perf_score, axis=1)

    # Periodes start / end
    if period_type == "week":
        agg["period_start"] = agg["period"]
        agg["period_end"]   = agg["period"].apply(
            lambda d: (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
            if d else d
        )
    else:
        agg["period_start"] = agg["period"]
        agg["period_end"]   = agg["period"].apply(
            lambda d: month_end(d) if d else d
        )

    out = pd.DataFrame({
        "id":                    range(1, len(agg) + 1),
        "person_id":             agg["Assignee"].values,
        "calculation_timestamp": CALC_TS,
        "metrics_period_start":  agg["period_start"].values,
        "metrics_period_end":    agg["period_end"].values,
        "lead_time":             agg["avg_lead_time_hours"].round(2).values,
        "change_failure_rate":   agg["cfr"].round(2).values,
        "mttr":                  agg["mttr"].round(2).values,
        "tasks_completed":       agg["nb_tickets_deployed"].fillna(0).astype(int).values,
        "bugs_created":          agg["nb_bugs_assigned"].values,
        "bugs_fixed":            agg["nb_bugs_resolved"].values,
        "commit_count":          agg["nb_commits_linked"].values,
        "prs_merged":            agg["nb_mrs_linked"].values,
        "cycle_time":            agg["avg_cycle_time_hours"].round(2).values,
        "productivity_score":    agg["performance_score"].values,
    })
    return out[COLS_IND]


# ══════════════════════════════════════════════════════════════
# FONCTION GENERIQUE : metriques projet par periode
# ══════════════════════════════════════════════════════════════
def build_project(period_col: str, cfr_map: dict, mttr_map: dict,
                  dep_map: dict, period_type: str) -> pd.DataFrame:

    # Metriques tickets par projet x periode
    team = to_pandas(
        tickets.groupBy("ProjectKey", period_col).agg(
            F.count("TicketKey").alias("nb_tickets_total"),
            F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0))
             .alias("nb_tickets_deployed"),
            F.sum(F.when(
                ~F.col("CurrentStatus").isin("To Be Deployed", "Closed"), 1
            ).otherwise(0)).alias("nb_tickets_wip"),
            F.sum(F.col("is_bug")).alias("nb_bugs_total"),
            F.round(F.avg("lead_time_hours"),   2).alias("proj_lead_time"),
            F.round(F.avg("cycle_time_hours"),  2).alias("proj_cycle_time"),
            F.round(
                F.sum(F.col("is_bug").cast(DoubleType())) /
                F.count("TicketKey") * 100, 1
            ).alias("proj_bug_rate"),
        )
    )

    if team.empty:
        return pd.DataFrame(columns=COLS_PROJ)

    team = team.rename(columns={period_col: "period"})
    team["period"] = team["period"].astype(str)

    # CFR, MTTR, deploys depuis les dicts
    team["cfr"]          = team["period"].map(cfr_map).fillna(0.0)
    team["mttr"]         = team["period"].map(mttr_map).fillna(0.0)
    team["dep_count"]    = team["period"].map(dep_map).fillna(0)

    # Velocite = tickets deployes par periode (1 semaine ou 1 mois)
    team["velocity"] = team["nb_tickets_deployed"].fillna(0).astype(float)

    # Deployment frequency :
    #   semaine → deploys cette semaine (1 valeur directe)
    #   mois    → deploys ce mois / nb_semaines du mois (freq hebdo)
    if period_type == "week":
        team["deployment_frequency"] = team["dep_count"].astype(float)
        team["period_start"] = team["period"]
        team["period_end"]   = team["period"].apply(
            lambda d: (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
            if d else d
        )
    else:
        def dep_freq_monthly(r):
            try:
                dt   = datetime.strptime(r["period"], "%Y-%m-%d")
                nw   = calendar.monthrange(dt.year, dt.month)[1] / 7
                return round(r["dep_count"] / max(nw, 1), 2)
            except Exception:
                return 0.0
        team["deployment_frequency"] = team.apply(dep_freq_monthly, axis=1)
        team["period_start"] = team["period"]
        team["period_end"]   = team["period"].apply(
            lambda d: month_end(d) if d else d
        )

    out = pd.DataFrame({
        "id":                    range(1, len(team) + 1),
        "project_id":            team["ProjectKey"].values,
        "calculation_timestamp": CALC_TS,
        "metrics_period_start":  team["period_start"].values,
        "metrics_period_end":    team["period_end"].values,
        "lead_time":             team["proj_lead_time"].round(2).values,
        "deployment_frequency":  team["deployment_frequency"].round(2).values,
        "change_failure_rate":   team["cfr"].round(2).values,
        "mttr":                  team["mttr"].round(2).values,
        "cycle_time":            team["proj_cycle_time"].round(2).values,
        "throughput":            team["velocity"].round(2).values,
        "wip":                   team["nb_tickets_wip"].fillna(0).astype(int).values,
        "bug_rate":              team["proj_bug_rate"].fillna(0.0).round(2).values,
        "velocity":              team["velocity"].round(2).values,
        "code_coverage":         None,
    })
    return out[COLS_PROJ]


# ══════════════════════════════════════════════════════════════
# GENERATION DES 4 FICHIERS
# ══════════════════════════════════════════════════════════════
print("Construction metrics_individual_w.csv ...")
ind_w = build_individual("week_start",  cfr_week,  mttr_week,  "week")
ind_w.to_csv(METRICS + "metrics_individual_w.csv", index=False, encoding="utf-8-sig")
print(f"  [OK] {len(ind_w)} lignes  ({ind_w['person_id'].nunique()} personnes, "
      f"{ind_w['metrics_period_start'].nunique()} semaines)")

print("Construction metrics_individual_m.csv ...")
ind_m = build_individual("month_start", cfr_month, mttr_month, "month")
ind_m.to_csv(METRICS + "metrics_individual_m.csv", index=False, encoding="utf-8-sig")
print(f"  [OK] {len(ind_m)} lignes  ({ind_m['person_id'].nunique()} personnes, "
      f"{ind_m['metrics_period_start'].nunique()} mois)")

print("Construction metrics_project_w.csv ...")
proj_w = build_project("week_start",  cfr_week,  mttr_week,  dep_week,  "week")
proj_w.to_csv(METRICS + "metrics_project_w.csv", index=False, encoding="utf-8-sig")
print(f"  [OK] {len(proj_w)} lignes  ({proj_w['project_id'].nunique()} projets, "
      f"{proj_w['metrics_period_start'].nunique()} semaines)")

print("Construction metrics_project_m.csv ...")
proj_m = build_project("month_start", cfr_month, mttr_month, dep_month, "month")
proj_m.to_csv(METRICS + "metrics_project_m.csv", index=False, encoding="utf-8-sig")
print(f"  [OK] {len(proj_m)} lignes  ({proj_m['project_id'].nunique()} projets, "
      f"{proj_m['metrics_period_start'].nunique()} mois)")

print()


# ══════════════════════════════════════════════════════════════
# RAPPORT FINAL
# ══════════════════════════════════════════════════════════════
print("=" * 60)
print("  RESUME FINAL")
print("=" * 60)
print()

for label, df in [
    ("metrics_individual_w", ind_w),
    ("metrics_individual_m", ind_m),
    ("metrics_project_w",    proj_w),
    ("metrics_project_m",    proj_m),
]:
    start = df["metrics_period_start"].min()
    end   = df["metrics_period_end"].max()
    print(f"  {label:<26}  {len(df):>5} lignes  |  {start} → {end}")

print()
print(f"  Colonnes individuelles : {COLS_IND}")
print()
print(f"  Colonnes projets       : {COLS_PROJ}")
print()
print("Script metrics termine.")

# Liberation
pipelines.unpersist()
jobs.unpersist()
tickets.unpersist()
mrs.unpersist()
commits.unpersist()
spark.stop()
