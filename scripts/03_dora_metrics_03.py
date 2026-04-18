"""
=============================================================
 DORA Metrics Project — VERSION BIG DATA
 Script  : 04_team_metrics_spark.py
 Étape   : 4 — Métriques Équipes & Personnes avec PySpark
=============================================================
Rôle : Calculer les métriques de performance par développeur,
       par équipe et par sprint pour alimenter les pages
       "Équipes" et "Personne" du dashboard Power BI.
       Traitement distribué via PySpark (scalable en production).

Métriques calculées :

  Par développeur (Assignee) :
  - nb_tickets_assigned     : total tickets assignés
  - nb_tickets_deployed     : tickets livrés (To Be Deployed)
  - nb_tickets_in_progress  : tickets en cours
  - nb_bugs_assigned        : bugs assignés
  - nb_bugs_resolved        : bugs résolus
  - avg_lead_time_hours     : Lead Time moyen
  - avg_cycle_time_hours    : Cycle Time moyen
  - avg_time_blocked_hours  : temps moyen en blocage
  - on_time_rate            : % tickets livrés à temps
  - nb_commits              : commits Git liés
  - nb_mrs                  : Merge Requests créées
  - avg_merge_time_hours    : temps moyen de merge
  - performance_score       : score global (0–100)

  Par équipe / projet :
  - team_velocity           : tickets déployés par semaine
  - team_lead_time_median   : Lead Time médian de l'équipe
  - team_on_time_rate       : % livraisons à temps
  - team_bug_rate           : % tickets qui sont des bugs

  Par sprint :
  - sprint_velocity         : tickets fermés dans le sprint
  - sprint_completion_rate  : % objectifs atteints
  - scope_change_count      : tickets ajoutés en cours de sprint

  Comparaison individu vs équipe :
  - lead_time_vs_team       : écart Lead Time individu / médiane équipe
  - commits_vs_team         : écart commits individu / moyenne équipe
  - on_time_vs_team         : écart on_time_rate individu / équipe

Sorties :
  data/team/assignee_performance_spark.csv  → métriques par développeur
  data/team/team_performance_spark.csv      → métriques par équipe/projet
  data/team/sprint_metrics_spark.csv        → métriques par sprint
  data/team/person_vs_team_spark.csv        → comparaison individu vs équipe
  data/team/monthly_activity_spark.csv      → activité mensuelle par personne
=============================================================
"""

import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

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
    .appName("DORA_Team_Metrics") \
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
TEAM     = "data/team/"

os.makedirs(TEAM, exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("Chargement des données")
print("=" * 60)

tickets = spark.read.csv(
    FEATURES + "tickets_features_spark.csv",
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

# ── Mapping email GitLab → nom lisible ────────────────────────────────────────
# Résout l'anonymisation des auteurs GitLab (hash SHA)
# Extrait depuis les adresses email des commits
EMAIL_TO_NAME = {
    "bensalem.saber@gtiinfo.com.tn":            "Saber Ben Salem",
    "teber.nour@gtiinfo.com.tn":                "Nour Teber",
    "chaouch.ahmed@attijaribank.com.tn":         "Ahmed Chaouch",
    "ahmed.chaouch.pro@gmail.com":               "Ahmed Chaouch",
    "jenhani.chaima@gtiinfo.com.tn":             "Chaima Jenhani",
    "hajkacem.mouadh@gtiinfo.com.tn":            "Mouadh Hajkacem",
    "sghari.oumayma@gtiinfo.com.tn":             "Oumayma Sghari",
    "khalifa.jawher@gtiinfo.com.tn":             "Jawher Khalifa",
    "daghfous.oumaima@gtiinfo.com.tn":           "Oumaima Daghfous",
    "daghfousoumaima@gtiinfo.com.tn":            "Oumaima Daghfous",
    "baccouche.yassine@gtiinfo.com.tn":          "Yassine Baccouche",
    "dridi.chaima@gtiinfo.com.tn":               "Chaima Dridi",
    "dridi.chayma@gtiinfo.com.tn":               "Chaima Dridi",
    "ochi.salim@gtiinfo.com.tn":                 "Salim Ochi",
    "benmakhlouf.faouzi@gtiinfo.com.tn":         "Faouzi Ben Makhlouf",
    "benhajsalah.yousef@attijaribank.com.tn":    "Yousef Ben Haj Salah",
    "bensalem.houssein@attijaribank.com.tn":     "Houssein Ben Salem",
    "bensalem.houssein@bank-sud.tn":             "Houssein Ben Salem",
    "benyahia.firas@attijaribank.com.tn":        "Firas Benyahia",
    "benyahia.firas@attijariank.com.tn":         "Firas Benyahia",
    "bakhouch.iyed@attijaribank.com.tn":         "Iyed Bakhouch",
    "bakhouch.iyed@bank-sud.tn":                 "Iyed Bakhouch",
    "mannai.mohamed@attijaribank.com.tn":        "Mohamed Mannai",
    "khouja.jihen@gtiinfo.com.tn":               "Jihen Khouja",
    "khouja.jihene@gtiinfo.com.tn":              "Jihen Khouja",
    "zoubeidi.med-ayoub@attijaribank.com.tn":    "Med Ayoub Zoubeidi",
    "devops-team@attijaribank.com.tn":           "DevOps Team",
    "devops-admin@attijaribank.com.tn":          "DevOps Admin",
    "ahmed@craftfoundry.tech":                   "Ahmed (externe)",
}

# create_map Spark — stable sur Windows (pas de join)
map_args = []
for k, v in EMAIL_TO_NAME.items():
    map_args += [F.lit(k), F.lit(v)]
email_to_name_map = F.create_map(*map_args)

# Ajouter colonne author_name aux commits
commits = commits.withColumn(
    "email_clean",
    F.lower(F.trim(F.regexp_replace(F.col("email"), '"', '')))
).withColumn(
    "author_name",
    F.coalesce(email_to_name_map[F.col("email_clean")], F.col("author"))
)


# Conversions de types
tickets = tickets \
    .withColumn("lead_time_hours",              F.col("lead_time_hours").cast(DoubleType())) \
    .withColumn("cycle_time_hours",             F.col("cycle_time_hours").cast(DoubleType())) \
    .withColumn("time_blocked_hours",           F.col("time_blocked_hours").cast(DoubleType())) \
    .withColumn("nb_commits",                   F.col("nb_commits").cast(IntegerType())) \
    .withColumn("nb_mrs",                       F.col("nb_mrs").cast(IntegerType())) \
    .withColumn("avg_merge_time_hours",         F.col("avg_merge_time_hours").cast(DoubleType())) \
    .withColumn("is_delayed",                   F.col("is_delayed").cast(IntegerType())) \
    .withColumn("is_bug",                       F.col("is_bug").cast(IntegerType())) \
    .withColumn("was_blocked",                  F.col("was_blocked").cast(IntegerType())) \
    .withColumn("nb_status_changes",            F.col("nb_status_changes").cast(IntegerType()))

mrs = mrs \
    .withColumn("merge_time_hours", F.col("merge_time_hours").cast(DoubleType()))

print(f"   tickets  : {tickets.count()} tickets")
print(f"   commits  : {commits.count()} commits")
print(f"   mrs      : {mrs.count()} MRs")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 1 — MÉTRIQUES PAR DÉVELOPPEUR (page Personne)
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("1. Métriques par développeur")
print("=" * 60)

# Commits par auteur — grouper par author_name (nom lisible depuis email)
commits_per_dev = commits.groupBy("author_name").agg(
    F.count("sha").alias("nb_commits_git"),
)

# MRs par auteur — vérifier les colonnes réelles
mrs_cols = mrs.columns
author_col_mrs = "author_username" if "author_username" in mrs_cols else \
                 "author_name"     if "author_name"     in mrs_cols else \
                 "author"          if "author"          in mrs_cols else None

if author_col_mrs:
    mrs_per_dev = mrs.groupBy(author_col_mrs).agg(
        F.count("mr_id").alias("nb_mrs_created"),
        F.round(F.avg("merge_time_hours"), 2).alias("avg_merge_time_hours_git"),
    ).withColumnRenamed(author_col_mrs, "author")
else:
    # Pas de colonne auteur trouvée → DataFrame vide
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    mrs_per_dev = spark.createDataFrame([], StructType([
        StructField("author",               StringType(), True),
        StructField("nb_mrs_created",       LongType(),   True),
        StructField("avg_merge_time_hours_git", DoubleType(), True),
    ]))

# Agrégats tickets par assignee
assignee_perf = tickets.groupBy("Assignee", "ProjectKey").agg(
    F.count("TicketKey").alias("nb_tickets_assigned"),
    F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0))
     .alias("nb_tickets_deployed"),
    F.sum(F.when(
        ~F.col("CurrentStatus").isin("To Be Deployed", "Closed"), 1
    ).otherwise(0)).alias("nb_tickets_in_progress"),
    F.sum(F.col("is_bug")).alias("nb_bugs_assigned"),
    F.sum(F.when(
        (F.col("is_bug") == 1) &
        (F.col("CurrentStatus") == "To Be Deployed"), 1
    ).otherwise(0)).alias("nb_bugs_resolved"),
    F.round(F.avg("lead_time_hours"),     2).alias("avg_lead_time_hours"),
    F.round(F.avg("cycle_time_hours"),    2).alias("avg_cycle_time_hours"),
    F.round(F.avg("time_blocked_hours"),  2).alias("avg_time_blocked_hours"),
    F.round(F.avg("nb_status_changes"),   2).alias("avg_status_changes"),
    F.round(
        F.sum(F.when(F.col("is_delayed") == 0, 1).otherwise(0)) /
        F.count("TicketKey") * 100, 1
    ).alias("on_time_rate"),
    F.sum(F.col("nb_commits")).alias("nb_commits_linked"),
    F.sum(F.col("nb_mrs")).alias("nb_mrs_linked"),
)

# Score de performance : 0–100
# Formule : on_time_rate * 0.4 + (1 - avg_lead_time normalisé) * 0.3 + ...
assignee_perf = assignee_perf.withColumn(
    "performance_score",
    F.round(
        F.least(F.lit(100.0),
            F.col("on_time_rate") * 0.4 +
            F.when(F.col("avg_lead_time_hours") < 24,  30.0)
             .when(F.col("avg_lead_time_hours") < 168, 20.0)
             .when(F.col("avg_lead_time_hours") < 720, 10.0)
             .otherwise(0.0) +
            F.when(F.col("avg_time_blocked_hours") == 0, 20.0)
             .when(F.col("avg_time_blocked_hours") < 24, 10.0)
             .otherwise(0.0) +
            F.when(F.col("nb_tickets_deployed") >= 10, 10.0)
             .when(F.col("nb_tickets_deployed") >= 5,   5.0)
             .otherwise(0.0)
        ), 1
    )
)

assignee_perf.toPandas().to_csv(
    TEAM + "assignee_performance_spark.csv",
    index=False, encoding="utf-8-sig"
)

nb_devs = assignee_perf.select("Assignee").distinct().count()
print(f"   Développeurs analysés    : {nb_devs}")
print(f"   ✅ assignee_performance_spark.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 2 — MÉTRIQUES PAR ÉQUIPE / PROJET (page Équipes)
# ══════════════════════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("2. Métriques par équipe / projet")
print("=" * 60)

team_perf = tickets.groupBy("ProjectKey").agg(
    F.countDistinct("Assignee").alias("nb_members"),
    F.count("TicketKey").alias("nb_tickets_total"),
    F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0))
     .alias("nb_tickets_deployed"),
    F.sum(F.col("is_bug")).alias("nb_bugs_total"),
    F.round(F.percentile_approx(F.col("lead_time_hours"), 0.5), 2).alias("team_lead_time_median"),
    F.round(F.avg("lead_time_hours"),    2).alias("team_lead_time_avg"),
    F.round(F.avg("cycle_time_hours"),   2).alias("team_cycle_time_avg"),
    F.round(F.avg("time_blocked_hours"), 2).alias("team_blocked_avg"),
    F.round(
        F.sum(F.when(F.col("is_delayed") == 0, 1).otherwise(0)) /
        F.count("TicketKey") * 100, 1
    ).alias("team_on_time_rate"),
    F.round(
        F.sum(F.col("is_bug").cast(DoubleType())) /
        F.count("TicketKey") * 100, 1
    ).alias("team_bug_rate"),
    F.round(
        F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0)).cast(DoubleType()) /
        F.count("TicketKey") * 100, 1
    ).alias("team_completion_rate"),
)

# Vélocité équipe : tickets déployés par semaine
velocity_df = tickets \
    .filter(F.col("CurrentStatus") == "To Be Deployed") \
    .withColumn("Created_ts", F.coalesce(
        F.try_to_timestamp(F.col("Created"), F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(F.col("Created"), F.lit("yyyy-MM-dd")),
    )) \
    .withColumn("week", F.date_trunc("week", F.col("Created_ts"))) \
    .groupBy("ProjectKey", "week") \
    .agg(F.count("TicketKey").alias("tickets_deployed_week"))

team_velocity = velocity_df.groupBy("ProjectKey") \
    .agg(F.round(F.avg("tickets_deployed_week"), 2).alias("team_velocity_per_week"))

team_perf = team_perf.join(team_velocity, on="ProjectKey", how="left")

team_perf.toPandas().to_csv(
    TEAM + "team_performance_spark.csv",
    index=False, encoding="utf-8-sig"
)

nb_teams = team_perf.count()
print(f"   Équipes / projets        : {nb_teams}")
print(f"   ✅ team_performance_spark.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 3 — MÉTRIQUES PAR SPRINT (page Sprint)
# ══════════════════════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("3. Métriques par sprint")
print("=" * 60)

# SprintState contient l'état du sprint (active, closed, future)
sprint_metrics = tickets.groupBy("ProjectKey", "SprintState").agg(
    F.count("TicketKey").alias("nb_tickets_sprint"),
    F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0))
     .alias("sprint_velocity"),
    F.round(
        F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0)).cast(DoubleType()) /
        F.count("TicketKey") * 100, 1
    ).alias("sprint_completion_rate"),
    F.sum(F.col("is_bug")).alias("nb_bugs_sprint"),
    F.sum(F.col("is_delayed")).alias("nb_delayed_sprint"),
    F.round(F.avg("lead_time_hours"),    2).alias("avg_lead_time_sprint"),
    F.round(F.avg("cycle_time_hours"),   2).alias("avg_cycle_time_sprint"),
    F.sum(F.col("was_blocked")).alias("nb_blocked_sprint"),
)

# Scope change : tickets ajoutés après le début (approximation : nb_status_changes > 1)
scope_change = tickets.groupBy("ProjectKey", "SprintState") \
    .agg(
        F.sum(F.when(F.col("nb_status_changes") > 3, 1).otherwise(0))
         .alias("scope_change_count")
    )

sprint_metrics = sprint_metrics.join(scope_change, on=["ProjectKey", "SprintState"], how="left")

sprint_metrics.toPandas().to_csv(
    TEAM + "sprint_metrics_spark.csv",
    index=False, encoding="utf-8-sig"
)

nb_sprints = sprint_metrics.count()
print(f"   Sprints analysés         : {nb_sprints}")
print(f"   ✅ sprint_metrics_spark.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 4 — COMPARAISON INDIVIDU VS ÉQUIPE (page Personne → graphique)
# ══════════════════════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("4. Comparaison individu vs équipe")
print("=" * 60)

# Médiane équipe par projet
team_medians = tickets.groupBy("ProjectKey").agg(
    F.round(F.percentile_approx(F.col("lead_time_hours"), 0.5), 2).alias("team_median_lead_time"),
    F.round(F.avg("nb_commits"),       2).alias("team_avg_commits"),
    F.round(
        F.sum(F.when(F.col("is_delayed") == 0, 1).otherwise(0)).cast(DoubleType()) /
        F.count("TicketKey") * 100, 1
    ).alias("team_avg_on_time_rate"),
)

# Jointure avec les métriques individuelles
person_vs_team = assignee_perf \
    .join(team_medians, on="ProjectKey", how="left") \
    .withColumn("lead_time_vs_team",
        F.round(F.col("avg_lead_time_hours") - F.col("team_median_lead_time"), 2)
    ) \
    .withColumn("commits_vs_team",
        F.round(F.col("nb_commits_linked").cast(DoubleType()) - F.col("team_avg_commits"), 2)
    ) \
    .withColumn("on_time_vs_team",
        F.round(F.col("on_time_rate") - F.col("team_avg_on_time_rate"), 2)
    ) \
    .select(
        "Assignee", "ProjectKey",
        "avg_lead_time_hours", "team_median_lead_time", "lead_time_vs_team",
        "nb_commits_linked",   "team_avg_commits",      "commits_vs_team",
        "on_time_rate",        "team_avg_on_time_rate", "on_time_vs_team",
        "performance_score",
    )

person_vs_team.toPandas().to_csv(
    TEAM + "person_vs_team_spark.csv",
    index=False, encoding="utf-8-sig"
)
print(f"   ✅ person_vs_team_spark.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 5 — ACTIVITÉ MENSUELLE PAR PERSONNE (tendance performance)
# ══════════════════════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("5. Activité mensuelle par développeur")
print("=" * 60)

monthly_activity = tickets.groupBy("Assignee", "month_year").agg(
    F.count("TicketKey").alias("nb_tickets_month"),
    F.sum(F.when(F.col("CurrentStatus") == "To Be Deployed", 1).otherwise(0))
     .alias("nb_deployed_month"),
    F.sum(F.col("is_bug")).alias("nb_bugs_month"),
    F.sum(F.col("is_delayed")).alias("nb_delayed_month"),
    F.round(F.avg("lead_time_hours"),  2).alias("avg_lead_time_month"),
    F.round(F.avg("cycle_time_hours"), 2).alias("avg_cycle_time_month"),
    F.sum(F.col("nb_commits")).alias("nb_commits_month"),
).orderBy("Assignee", "month_year")

monthly_activity.toPandas().to_csv(
    TEAM + "monthly_activity_spark.csv",
    index=False, encoding="utf-8-sig"
)

print(f"   ✅ monthly_activity_spark.csv")


# ══════════════════════════════════════════════════════════════════════════════
# 6 — AJOUTER AUTOMATIQUEMENT LES DÉVELOPPEURS GITLAB SANS TICKETS JIRA
# ══════════════════════════════════════════════════════════════════════════════
# Le code extrait automatiquement depuis les commits tous les développeurs
# qui n'ont aucun ticket Jira. Pas de liste hardcodée — tout est dynamique.
print()
print("=" * 60)
print("6. Développeurs GitLab sans tickets Jira")
print("=" * 60)

# ── Étape 1 : extraire tous les devs depuis les commits (via email) ────────
commits_pd = commits.toPandas()
commits_pd['email_clean'] = commits_pd['email'].str.strip().str.strip('"').str.lower()

# Appliquer le mapping email → nom depuis EMAIL_TO_NAME
commits_pd['developer_name'] = commits_pd['email_clean'].map(EMAIL_TO_NAME)

# Ignorer les emails invalides (sans nom mappé)
commits_pd = commits_pd[commits_pd['developer_name'].notna()]

# Compter commits par développeur (dédupliqué par nom)
git_devs_pd = commits_pd.groupby('developer_name').agg(
    nb_commits_git=('sha', 'count'),
    email_principal=('email_clean', 'first')
).reset_index()

# ── Étape 2 : trouver ceux qui ne sont PAS dans Jira ──────────────────────
# Récupérer tous les noms Jira depuis les tickets (via AssigneeEmail)
jira_emails_pd = tickets.select("Assignee", "AssigneeEmail").distinct().toPandas()
jira_emails_pd['email_clean'] = jira_emails_pd['AssigneeEmail'].str.strip().str.lower()
jira_emails_pd['developer_name'] = jira_emails_pd['email_clean'].map(EMAIL_TO_NAME)

# Noms GitLab déjà couverts par Jira
noms_dans_jira = set(jira_emails_pd['developer_name'].dropna().unique())

# Noms GitLab ABSENTS de Jira → nouvelles lignes à ajouter
git_only = git_devs_pd[~git_devs_pd['developer_name'].isin(noms_dans_jira)].copy()

print(f"   Développeurs GitLab total         : {len(git_devs_pd)}")
print(f"   Déjà dans Jira (via email)        : {len(noms_dans_jira)}")
print(f"   Nouveaux à ajouter                : {len(git_only)}")

# ── Étape 3 : construire les nouvelles lignes ──────────────────────────────
ap_pd = pd.read_csv(TEAM + "assignee_performance_spark.csv")
cols = list(ap_pd.columns)

new_rows = []
for _, row in git_only.iterrows():
    new_row = {c: None for c in cols}
    new_row["Assignee"]            = row['developer_name']
    new_row["ProjectKey"]          = "GITLAB"
    new_row["nb_tickets_assigned"] = 0
    new_row["nb_tickets_deployed"] = 0
    new_row["nb_tickets_in_progress"] = 0
    new_row["nb_bugs_assigned"]    = 0
    new_row["nb_bugs_resolved"]    = 0
    new_row["nb_commits_linked"]   = int(row['nb_commits_git'])
    new_row["nb_mrs_linked"]       = 0
    new_row["on_time_rate"]        = None
    new_row["performance_score"]   = None
    new_rows.append(new_row)

# ── Étape 4 : concatener et sauvegarder ───────────────────────────────────
new_df   = pd.DataFrame(new_rows, columns=cols)
ap_final = pd.concat([ap_pd, new_df], ignore_index=True)
ap_final.to_csv(TEAM + "assignee_performance_spark.csv", index=False, encoding="utf-8-sig")

print()
for r in new_rows:
    print(f"   + {r['Assignee']:<28} → {r['nb_commits_linked']} commits  (ProjectKey=GITLAB)")
print(f"   ✅ assignee_performance_spark.csv  ({len(ap_final)} lignes au total)")

# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("RÉSUMÉ — MÉTRIQUES ÉQUIPES")
print("=" * 60)
print(f"   Développeurs             : {nb_devs}")
print(f"   Équipes / projets        : {nb_teams}")
print(f"   Sprints                  : {nb_sprints}")
print()
print("Fichiers générés dans data/team/ :")
print("   assignee_performance_spark.csv  ✅")
print("   team_performance_spark.csv      ✅")
print("   sprint_metrics_spark.csv        ✅")
print("   person_vs_team_spark.csv        ✅")
print("   monthly_activity_spark.csv      ✅")
print()
print("✅ Étape 4 terminée.")
print("   Prochaine étape : 05_machine_learning_spark.py")

spark.stop()