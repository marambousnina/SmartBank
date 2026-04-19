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

spark.conf.set("spark.sql.session.timeZone", "UTC")
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
pipelines = spark.read.csv(
    CLEANED + "pipelines_cleaned_spark.csv",
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

current = jira.filter(F.col("IsCurrent").isin("True", "Yes", "true", "yes")) \
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
    F.round(F.avg("nb_commits"), 2).alias("avg_commits_per_ticket"),
    F.round(F.avg("nb_mrs"), 2).alias("avg_mrs_per_ticket"),
    F.round(F.avg("lead_time_hours"), 2).alias("avg_lead_time_hours"),
)
proj_stats = proj_stats.withColumn(
    "completion_rate",
    F.round(F.col("nb_tickets_done") / F.col("nb_tickets_total") * 100, 1)
)

# ── Commits par projet (via TicketKey) ────────────────────────────────────
commits_proj = commits \
    .withColumn("ProjectKey", F.regexp_extract(F.col("TicketKey"), r"^([A-Z]+)", 1)) \
    .filter(F.col("ProjectKey") != "") \
    .groupBy("ProjectKey").agg(
        F.count("sha").alias("nb_commits_git"),
        F.countDistinct("author").alias("nb_contributors"),
    )

# ── MRs par projet (via TicketKey) ────────────────────────────────────────
mrs_proj = mrs \
    .withColumn("ProjectKey", F.regexp_extract(F.col("TicketKey"), r"^([A-Z]+)", 1)) \
    .filter(F.col("ProjectKey") != "") \
    .groupBy("ProjectKey").agg(
        F.count("mr_id").alias("nb_mrs_git"),
        F.sum(F.when(F.col("state") == "merged", 1).otherwise(0)).alias("nb_mrs_merged"),
        F.round(F.avg(F.col("merge_time_hours").cast(DoubleType())), 2).alias("avg_merge_time_hours"),
    )

# ── Pipelines par projet (via ref) ────────────────────────────────────────
pipelines_proj = pipelines \
    .withColumn("ProjectKey", F.upper(F.regexp_extract(F.col("ref"), r"([A-Z]+)", 1))) \
    .filter(F.col("ProjectKey") != "") \
    .withColumn("is_production", F.col("is_production").isin("true", "True")) \
    .groupBy("ProjectKey").agg(
        F.count("pipeline_id").alias("nb_pipelines"),
        F.sum(F.when(F.col("is_production") == True, 1).otherwise(0)).alias("nb_deployments"),
        F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("nb_pipelines_failed"),
        F.round(F.avg(F.col("duration_minutes").cast(DoubleType())), 2).alias("avg_pipeline_duration_min"),
    )
pipelines_proj = pipelines_proj.withColumn(
    "cfr_pct",
    F.round(F.col("nb_pipelines_failed") / F.col("nb_pipelines") * 100, 1)
)

# ── Jointure enrichie ─────────────────────────────────────────────────────
proj_stats = proj_stats \
    .join(commits_proj,   on="ProjectKey", how="left") \
    .join(mrs_proj,       on="ProjectKey", how="left") \
    .join(pipelines_proj, on="ProjectKey", how="left") \
    .fillna({
        "nb_commits_git": 0, "nb_contributors": 0,
        "nb_mrs_git": 0, "nb_mrs_merged": 0, "avg_merge_time_hours": 0.0,
        "nb_pipelines": 0, "nb_deployments": 0,
        "nb_pipelines_failed": 0, "avg_pipeline_duration_min": 0.0, "cfr_pct": 0.0,
    })

ticket_features = ticket_features.join(
    proj_stats.select("ProjectKey", "nb_tickets_total", "nb_tickets_done", "completion_rate"),
    on="ProjectKey", how="left"
)

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


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — FICHIER PERSONNEL
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("Construction du fichier personnel.csv")
print("=" * 60)

# Comptes système/bots à exclure
EXCLUDE_EMAILS = {
    "devops-admin@attijaribank.com.tn",
    "devops-team@attijaribank.com.tn",
    "unknown@bank.tn",
    "helloword0@2",
}
EXCLUDE_PATTERNS = ["<", ">", "\ufffd", "\xef\xbf\xbd", "?"]

def is_valid_email(email):
    if not isinstance(email, str) or not email.strip():
        return False
    e = email.strip().lower()
    if e in EXCLUDE_EMAILS:
        return False
    try:
        e.encode("ascii")
    except UnicodeEncodeError:
        return False
    if any(p in email for p in EXCLUDE_PATTERNS):
        return False
    return "@" in e and "." in e.split("@")[-1]

def derive_department(email):
    if not isinstance(email, str):
        return ""
    domain = email.strip().lower().split("@")[-1]
    mapping = {
        "attijaribank.com.tn": "Attijari Bank",
        "gtiinfo.com.tn":      "GTI Info",
        "bank-sud.tn":         "Bank Sud",
        "craftfoundry.tech":   "Craft Foundry",
    }
    return mapping.get(domain, "Externe")

def name_from_email(email):
    local = email.split("@")[0]
    return " ".join(p.capitalize() for p in local.replace(".", " ").replace("_", " ").split())

# ── Charger les données augmented en pandas ───────────────────────────────
commits_pd = pd.read_csv(CLEANED + "commits_cleaned_spark.csv")
jira_pd    = pd.read_csv(CLEANED + "jira_cleaned_spark.csv")

# ── Git : une ligne par email valide ──────────────────────────────────────
git_df = (
    commits_pd[["author", "email"]]
    .dropna(subset=["email"])
    .assign(email_norm=lambda d: d["email"].str.strip().str.lower())
    .query("email_norm.apply(@is_valid_email)", engine="python")
    .drop_duplicates(subset="email_norm")
    [["author", "email_norm"]]
    .rename(columns={"email_norm": "email"})
)

# ── Jira : une ligne par email, avec nom, projet principal ────────────────
jira_current_pd = jira_pd[jira_pd["IsCurrent"].astype(str).isin(["True", "true", "Yes", "yes", "1"])]

jira_persons = (
    jira_current_pd[["Assignee", "AssigneeEmail", "Project"]]
    .dropna(subset=["AssigneeEmail"])
    .assign(email_norm=lambda d: d["AssigneeEmail"].str.strip().str.lower())
    .query("email_norm.apply(@is_valid_email)", engine="python")
)

# equipe = projet le plus fréquent par email
jira_equipe = (
    jira_persons.groupby("email_norm")["Project"]
    .agg(lambda x: x.value_counts().index[0])
    .reset_index()
    .rename(columns={"email_norm": "email", "Project": "equipe"})
)

# nom = premier Assignee non-null par email
jira_nom = (
    jira_persons.dropna(subset=["Assignee"])
    .groupby("email_norm")["Assignee"]
    .first()
    .reset_index()
    .rename(columns={"email_norm": "email", "Assignee": "nom_jira"})
)

jira_ref = jira_equipe.merge(jira_nom, on="email", how="left")

# ── Fusion Git + Jira ─────────────────────────────────────────────────────
git_emails  = set(git_df["email"])
jira_emails = set(jira_ref["email"])

both_emails      = git_emails & jira_emails
git_only_emails  = git_emails - jira_emails
jira_only_emails = jira_emails - git_emails

rows = []

for email in sorted(both_emails):
    jira_row = jira_ref[jira_ref["email"] == email].iloc[0]
    nom = jira_row["nom_jira"] if pd.notna(jira_row.get("nom_jira")) else name_from_email(email)
    rows.append({
        "email":       email,
        "nom":         nom,
        "departement": derive_department(email),
        "equipe":      "",
        "source":      "git+jira",
    })

for email in sorted(git_only_emails):
    rows.append({
        "email":       email,
        "nom":         name_from_email(email),
        "departement": derive_department(email),
        "equipe":      "",
        "source":      "git",
    })

for email in sorted(jira_only_emails):
    jira_row = jira_ref[jira_ref["email"] == email].iloc[0]
    nom = jira_row["nom_jira"] if pd.notna(jira_row.get("nom_jira")) else name_from_email(email)
    rows.append({
        "email":       email,
        "nom":         nom,
        "departement": derive_department(email),
        "equipe":      "",
        "source":      "jira",
    })

personnel_df = pd.DataFrame(rows, columns=["email", "nom", "departement", "equipe", "source"])
personnel_df.insert(0, "id", range(1, len(personnel_df) + 1))

# ── Enrichissement depuis data/raw/equipe.csv ─────────────────────────────
import unicodedata, re as _re

def _norm_name(name):
    if not isinstance(name, str):
        return ""
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower().replace(".", " ").replace("_", " ").replace("-", " ")
    name = _re.sub(r"\s+", " ", name).strip()
    return " ".join(sorted(name.split()))

eq_raw = pd.read_csv("data/raw/equipe.csv")
eq_raw["name_key"] = eq_raw["Name"].apply(_norm_name)
personnel_df["name_key"] = personnel_df["nom"].apply(_norm_name)

eq_lookup = (
    eq_raw[["name_key", "Department", "Teams"]]
    .drop_duplicates(subset="name_key")
    .set_index("name_key")
)

def _first_team(teams_str):
    if not isinstance(teams_str, str) or not teams_str.strip():
        return None
    return teams_str.split("\n")[0].strip()

matched_indices = set()

for idx, row in personnel_df.iterrows():
    key = row["name_key"]
    if key not in eq_lookup.index:
        continue
    matched_indices.add(idx)
    eq_row  = eq_lookup.loc[key]
    dept    = eq_row["Department"]
    teams   = _first_team(eq_row["Teams"])
    has_dept  = pd.notna(dept) and str(dept).strip()
    has_teams = bool(teams)

    # departement : P1 Department, P2 first team, P3 (→ P4) email domain déjà défini
    if has_dept:
        personnel_df.at[idx, "departement"] = str(dept).strip()
    elif has_teams:
        personnel_df.at[idx, "departement"] = teams

    # equipe : P1 first team, P2 departement (si teams null)
    if has_teams:
        personnel_df.at[idx, "equipe"] = teams
    else:
        personnel_df.at[idx, "equipe"] = personnel_df.at[idx, "departement"]

# equipe P3 : non matché → valeur du departement
for idx in personnel_df.index:
    if idx not in matched_indices:
        personnel_df.at[idx, "equipe"] = personnel_df.at[idx, "departement"]

personnel_df = personnel_df.drop(columns=["name_key"])

# ══════════════════════════════════════════════════════════════════════════════
# AMÉLIORATION 1 — Déduplication des doublons (même personne, emails différents)
# ══════════════════════════════════════════════════════════════════════════════
# Priorité email canonique : @attijaribank > @gtiinfo > @bank-sud > autres
EMAIL_PRIORITY = ["attijaribank.com.tn", "gtiinfo.com.tn", "bank-sud.tn"]

def _email_rank(email):
    domain = email.split("@")[-1] if "@" in email else ""
    try:
        return EMAIL_PRIORITY.index(domain)
    except ValueError:
        return len(EMAIL_PRIORITY)

def _merge_source(sources):
    s = set(sources)
    if "git+jira" in s or ("git" in s and "jira" in s):
        return "git+jira"
    if "git" in s:
        return "git"
    return "jira"

personnel_df["name_norm"] = personnel_df["nom"].apply(_norm_name)
personnel_df["email_rank"] = personnel_df["email"].apply(_email_rank)
personnel_df = personnel_df.sort_values("email_rank")

# ── Construire email_person_map (tous alias → person_id canonique) ─────────
alias_map_rows = []   # {alias_email, canonical_email}
canonical_rows = []   # une ligne par vraie personne

for name_key, group in personnel_df.groupby("name_norm", sort=False):
    canonical = group.iloc[0]   # meilleur email (trié par priorité)
    canonical_email = canonical["email"]
    merged_source   = _merge_source(group["source"].tolist())
    row = canonical.to_dict()
    row["source"] = merged_source
    canonical_rows.append(row)
    for _, alias in group.iterrows():
        alias_map_rows.append({
            "alias_email":     alias["email"],
            "canonical_email": canonical_email,
        })

personnel_clean = (
    pd.DataFrame(canonical_rows)
    .drop(columns=["email_rank", "name_norm", "id"], errors="ignore")
    .reset_index(drop=True)
)
personnel_clean.insert(0, "id", range(1, len(personnel_clean) + 1))

email_map = pd.DataFrame(alias_map_rows)   # alias_email → canonical_email

# ── Sauvegardes ───────────────────────────────────────────────────────────
personnel_clean.to_csv(FEATURES + "personnel.csv",       index=False, encoding="utf-8-sig")
email_map.to_csv(      FEATURES + "email_person_map.csv", index=False, encoding="utf-8-sig")

# ══════════════════════════════════════════════════════════════════════════════
# AMÉLIORATION 2 — Propager person_id dans assignee_metrics
# ══════════════════════════════════════════════════════════════════════════════
# Charger le fichier assignee_metrics déjà sauvegardé et enrichir avec person_id
assignee_df = pd.read_csv(FEATURES + "assignee_metrics_spark.csv")

# Construire le mapping Assignee → canonical_email via Jira
jira_email_map = (
    jira_pd[jira_pd["IsCurrent"].astype(str).isin(["True","true","Yes","yes","1"])]
    [["Assignee", "AssigneeEmail"]]
    .dropna()
    .drop_duplicates(subset="Assignee")
    .assign(AssigneeEmail=lambda d: d["AssigneeEmail"].str.strip().str.lower())
)
# Joindre avec email_map pour obtenir canonical_email
jira_email_map = jira_email_map.merge(
    email_map.rename(columns={"alias_email": "AssigneeEmail", "canonical_email": "canonical_email"}),
    on="AssigneeEmail", how="left"
)
jira_email_map["canonical_email"] = jira_email_map["canonical_email"].fillna(jira_email_map["AssigneeEmail"])

# Joindre assignee_metrics avec person_id
person_id_map = personnel_clean[["id", "email"]].rename(columns={"email": "canonical_email", "id": "person_id"})
jira_email_map = jira_email_map.merge(person_id_map, on="canonical_email", how="left")

assignee_df = assignee_df.merge(
    jira_email_map[["Assignee", "person_id"]].drop_duplicates(),
    on="Assignee", how="left"
)

# Agréger par person_id pour fusionner les doublons
numeric_cols = ["nb_tickets_assigned", "nb_tickets_deployed_person"]
mean_cols    = ["avg_lead_time_person", "on_time_rate"]

agg_dict = {c: "sum" for c in numeric_cols if c in assignee_df.columns}
agg_dict.update({c: "mean" for c in mean_cols if c in assignee_df.columns})
agg_dict["Assignee"] = "first"

assignee_merged = (
    assignee_df
    .dropna(subset=["person_id"])
    .groupby("person_id", as_index=False)
    .agg(agg_dict)
)
assignee_merged["on_time_rate"] = assignee_merged["on_time_rate"].round(1)
assignee_merged["avg_lead_time_person"] = assignee_merged["avg_lead_time_person"].round(2)

# Joindre avec personnel pour enrichir (nom, equipe, departement)
assignee_merged = assignee_merged.merge(
    personnel_clean[["id", "nom", "equipe", "departement"]].rename(columns={"id": "person_id"}),
    on="person_id", how="left"
)

assignee_merged.to_csv(FEATURES + "assignee_metrics_spark.csv", index=False, encoding="utf-8-sig")

n_before = len(personnel_df)
n_after  = len(personnel_clean)
print(f"   Personnes avant dédup : {n_before}")
print(f"   Personnes après dédup : {n_after}  ({n_before - n_after} doublons fusionnés)")
print(f"   Alias email mappés    : {len(email_map)}")
print(f"   Assignees enrichis    : {len(assignee_merged)}")
print(f"   Fichiers              : personnel.csv | email_person_map.csv | assignee_metrics_spark.csv")
print()
print("[OK] Etape 2 Spark terminee.")
print("   Prochaine etape : 03_dora_metrics_hybrid_spark.py")

spark.stop()