import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# -- Fix Windows : chemin Python sans espaces pour les workers Spark
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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)

# ── Créer la session Spark ────────────────────────────────────────────────────
# local[*] = utilise tous les cœurs du processeur disponibles
# Pour un vrai cluster → remplacer par l'URL du cluster
spark = SparkSession.builder \
    .appName("DORA_Metrics_Cleaning") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions",
            "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions",
            "-Djava.security.manager=allow") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()



spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR")

from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
RAW     = str(ROOT_DIR / "data" / "augmented") + "/"
CLEANED = str(ROOT_DIR / "data" / "cleaned")   + "/"
os.makedirs(CLEANED, exist_ok=True)

print("=" * 60)
print("  Session Spark demarree")
print(f"   Version Spark : {spark.version}")
print("=" * 60)
print()


def to_pandas(spark_df) -> pd.DataFrame:
    """Utilise collect() via py4j — evite les Python workers (crash Windows)."""
    rows = spark_df.collect()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([r.asDict() for r in rows])


# ══════════════════════════════════════════════════════════════════════════════
# FONCTION UTILITAIRE — Parsing de dates
# ══════════════════════════════════════════════════════════════════════════════
def parse_dates_spark(df, cols):
    """
    Tente plusieurs formats de date.
    Supprime les timezones (+01:00, Z) avant parsing.
    IMPORTANT Spark 4.x : le format doit etre passe avec F.lit()
    """
    for col in cols:
        if col not in df.columns:
            continue
        # Supprimer la timezone (+01:00, +00:00, Z) si presente
        cleaned = F.regexp_replace(F.col(col), r"[+-]\d{2}:\d{2}$|Z$", "")
        cleaned = F.trim(cleaned)
        # Essayer plusieurs formats avec F.lit() — obligatoire Spark 4.x
        df = df.withColumn(col,
            F.coalesce(
                F.try_to_timestamp(cleaned, F.lit("yyyy-MM-dd HH:mm:ss")),
                F.try_to_timestamp(cleaned, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSS")),
                F.try_to_timestamp(cleaned, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
                F.try_to_timestamp(cleaned, F.lit("dd/MM/yyyy HH:mm")),
                F.try_to_timestamp(cleaned, F.lit("dd/MM/yyyy")),
                F.try_to_timestamp(cleaned, F.lit("yyyy/MM/dd")),
                F.try_to_timestamp(cleaned, F.lit("yyyy-MM-dd")),
            )
        )
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 1. JIRA STATUS HISTORY
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("1. Nettoyage Jira Status History")
print("=" * 60)

jira = spark.read.csv(
    RAW + "jira_augmented.csv",
    header=True,
    inferSchema=False,   # On garde tout en String d'abord
    encoding="UTF-8"
)
total = jira.count()
print(f"   Lignes chargées            : {total}")

# Suppression des doublons
jira = jira.dropDuplicates(["TicketKey", "Status", "StatusEntryDate"])
after_dedup = jira.count()
print(f"   Doublons supprimés         : {total - after_dedup}")

# Conversion des dates
date_cols = ["StatusEntryDate", "StatusExitDate", "Created", "Updated", "ResolutionDate"]
jira = parse_dates_spark(jira, date_cols)

# Correction incohérence : StatusExitDate < StatusEntryDate
jira = jira.withColumn(
    "StatusExitDate",
    F.when(
        F.col("StatusExitDate") < F.col("StatusEntryDate"),
        F.lit(None).cast(TimestampType())
    ).otherwise(F.col("StatusExitDate"))
)

# Normalisation texte
for col in ["Status", "StatusCategory", "Priority", "IssueType"]:
    jira = jira.withColumn(col, F.trim(F.col(col)))
jira = jira.withColumn("ProjectKey", F.upper(F.trim(F.col("ProjectKey"))))

# Valeurs manquantes
jira = jira \
    .fillna({"Assignee": "Non assigné",
             "AssigneeEmail": "unknown@bank.tn",
             "Priority": "Medium",
             "SprintState": "No Sprint",
             "DueDate": ""})

# Supprimer lignes sans clé ni statut
jira = jira.filter(
    F.col("TicketKey").isNotNull() &
    (F.col("TicketKey") != "nan") &
    F.col("Status").isNotNull() &
    (F.col("Status") != "nan")
)

# Recalcul TimeInStatusHours si manquant
jira = jira.withColumn(
    "TimeInStatusHours",
    F.when(
        F.col("TimeInStatusHours").isNull() & F.col("StatusExitDate").isNotNull(),
        F.round(
            (F.unix_timestamp("StatusExitDate") - F.unix_timestamp("StatusEntryDate")) / 3600,
            2
        )
    ).otherwise(F.col("TimeInStatusHours").cast(DoubleType()))
)

# Sauvegarde
to_pandas(jira).to_csv(CLEANED + "jira_cleaned_spark.csv", index=False, encoding="utf-8-sig")
final_count = jira.count()
print(f"   [OK] Lignes sauvegardees   : {final_count}")
print(f"   Tickets uniques            : {jira.select('TicketKey').distinct().count()}")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 2. GITLAB COMMITS
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("2. Nettoyage GitLab Commits")
print("=" * 60)

commits = spark.read.csv(
    RAW + "gitlab_commits_augmented.csv",
    header=True, inferSchema=False
)
total = commits.count()
print(f"   Lignes chargées            : {total}")

commits = commits.dropDuplicates(["sha"])
print(f"   Doublons supprimés         : {total - commits.count()}")

commits = parse_dates_spark(commits, ["date"])
commits = commits.filter(F.col("sha").isNotNull() & F.col("date").isNotNull())

# Extraire clé Jira depuis le titre du commit (ex: MBC-45, DL-12)
commits = commits.withColumn(
    "TicketKey",
    F.regexp_extract(F.col("title"), r"([A-Z]+-\d+)", 1)
)
commits = commits.withColumn(
    "TicketKey",
    F.when(F.col("TicketKey") == "", None).otherwise(F.col("TicketKey"))
)

commits = commits \
    .withColumn("author", F.trim(F.col("author"))) \
    .withColumn("email",  F.lower(F.trim(F.col("email")))) \
    .withColumn("title",  F.trim(F.col("title")))

to_pandas(commits).to_csv(CLEANED + "commits_cleaned_spark.csv", index=False, encoding="utf-8-sig")
linked = commits.filter(F.col("TicketKey").isNotNull()).count()
print(f"   [OK] Lignes sauvegardees   : {commits.count()}")
print(f"   Commits liés à Jira        : {linked}")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 3. GITLAB PIPELINES
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("3. Nettoyage GitLab Pipelines")
print("=" * 60)

pipelines = spark.read.csv(
    RAW + "gitlab_pipelines_augmented.csv",
    header=True, inferSchema=False
)
total = pipelines.count()
print(f"   Lignes chargées            : {total}")

pipelines = pipelines.dropDuplicates(["pipeline_id"])
print(f"   Doublons supprimés         : {total - pipelines.count()}")

pipelines = parse_dates_spark(pipelines, ["created_at", "updated_at"])
pipelines = pipelines.filter(
    F.col("pipeline_id").isNotNull() & F.col("created_at").isNotNull()
)

# Flag production
pipelines = pipelines.withColumn(
    "is_production",
    F.col("ref").rlike("(?i)(main|master|release|merge-requests)")
)

# Durée non négative
pipelines = pipelines.withColumn(
    "duration_minutes",
    F.greatest(F.col("duration_minutes").cast(DoubleType()), F.lit(0.0))
)

# Filtrer statuts valides
valid_statuses = ["success", "failed", "canceled", "running", "pending"]
pipelines = pipelines \
    .withColumn("status", F.lower(F.trim(F.col("status")))) \
    .filter(F.col("status").isin(valid_statuses))

to_pandas(pipelines).to_csv(CLEANED + "pipelines_cleaned_spark.csv", index=False, encoding="utf-8-sig")
prod_count = pipelines.filter(F.col("is_production") == True).count()
print(f"   [OK] Lignes sauvegardees   : {pipelines.count()}")
print(f"   Pipelines production       : {prod_count}")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 4. GITLAB JOBS
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("4. Nettoyage GitLab Jobs")
print("=" * 60)

jobs = spark.read.csv(
    RAW + "gitlab_jobs_augmented.csv",
    header=True, inferSchema=False
)
total = jobs.count()
print(f"   Lignes chargées            : {total}")

jobs = jobs.dropDuplicates(["job_id"])
print(f"   Doublons supprimés         : {total - jobs.count()}")

jobs = parse_dates_spark(jobs, ["started_at", "finished_at"])

# Correction incohérence dates
jobs = jobs.withColumn(
    "finished_at",
    F.when(
        F.col("finished_at") < F.col("started_at"),
        F.lit(None).cast(TimestampType())
    ).otherwise(F.col("finished_at"))
)

# Calcul durée job
jobs = jobs.withColumn(
    "duration_minutes",
    F.when(
        F.col("started_at").isNotNull() & F.col("finished_at").isNotNull(),
        F.round(
            (F.unix_timestamp("finished_at") - F.unix_timestamp("started_at")) / 60,
            2
        )
    ).otherwise(F.lit(0.0))
)
jobs = jobs.withColumn(
    "duration_minutes",
    F.greatest(F.col("duration_minutes"), F.lit(0.0))
)

jobs = jobs \
    .withColumn("status", F.lower(F.trim(F.col("status")))) \
    .withColumn("name",   F.trim(F.col("name"))) \
    .filter(F.col("pipeline_id").isNotNull())

to_pandas(jobs).to_csv(CLEANED + "jobs_cleaned_spark.csv", index=False, encoding="utf-8-sig")
print(f"   [OK] Lignes sauvegardees   : {jobs.count()}")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 5. GITLAB MERGE REQUESTS
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("5. Nettoyage GitLab Merge Requests")
print("=" * 60)

mrs = spark.read.csv(
    RAW + "gitlab_merge_requests_augmented.csv",
    header=True, inferSchema=False
)
total = mrs.count()
print(f"   Lignes chargées            : {total}")

mrs = mrs.dropDuplicates(["mr_id"])
print(f"   Doublons supprimés         : {total - mrs.count()}")

mrs = parse_dates_spark(mrs, ["created_at", "merged_at"])

# Recalcul merge_time_hours
mrs = mrs.withColumn(
    "merge_time_hours",
    F.when(
        F.col("merged_at").isNotNull() & F.col("created_at").isNotNull(),
        F.greatest(
            F.round(
                (F.unix_timestamp("merged_at") - F.unix_timestamp("created_at")) / 3600,
                2
            ),
            F.lit(0.0)
        )
    ).otherwise(F.lit(0.0))
)

# Extraire clé Jira depuis titre MR
mrs = mrs.withColumn(
    "TicketKey",
    F.regexp_extract(F.col("title"), r"([A-Z]+-\d+)", 1)
)
mrs = mrs.withColumn(
    "TicketKey",
    F.when(F.col("TicketKey") == "", None).otherwise(F.col("TicketKey"))
)

mrs = mrs \
    .withColumn("state",  F.lower(F.trim(F.col("state")))) \
    .withColumn("title",  F.trim(F.col("title"))) \
    .withColumn("author", F.trim(F.col("author"))) \
    .filter(F.col("state").isin(["merged", "opened", "closed"]))

to_pandas(mrs).to_csv(CLEANED + "merge_requests_cleaned_spark.csv", index=False, encoding="utf-8-sig")
linked_mrs = mrs.filter(F.col("TicketKey").isNotNull()).count()
print(f"   [OK] Lignes sauvegardees   : {mrs.count()}")
print(f"   MR liés à Jira             : {linked_mrs}")
print()


# ── Rapport final ─────────────────────────────────────────────────────────────
print("=" * 60)
print("RAPPORT DE NETTOYAGE -- RESUME FINAL")
print("=" * 60)
print(f"   jira_cleaned_spark           {jira.count():>6} lignes  [OK]")
print(f"   commits_cleaned_spark        {commits.count():>6} lignes  [OK]")
print(f"   pipelines_cleaned_spark      {pipelines.count():>6} lignes  [OK]")
print(f"   jobs_cleaned_spark           {jobs.count():>6} lignes  [OK]")
print(f"   merge_requests_cleaned_spark {mrs.count():>6} lignes  [OK]")
print()
print("[OK] Etape 1 Spark terminee.")
print("   Prochaine etape : 02_feature_engineering_spark.py")

spark.stop()