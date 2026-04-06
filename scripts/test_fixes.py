"""
=============================================================
 DORA Metrics Project
 Script  : test_fixes.py
 Utilitaire : Tests de validation des 5 corrections
=============================================================
Rôle : Vérifier que les 5 corrections appliquées dans
       02_feature_engineering_spark.py ont bien amélioré
       la qualité des données.

Tests effectués :
  FIX 1 — lead_time_hours : couverture > 16%
  FIX 2 — nb_commits      : jointure temporelle active
  FIX 3 — cycle_time_hours: statuts étendus
  FIX 4 — nb_mrs          : jointure temporelle active
  FIX 5 — priorité exact > temporel (pas de doublons)

Lance : python scripts/test_fixes.py
=============================================================
"""

import pandas as pd
import os

# ── Couleurs console ──────────────────────────────────────────
PASS = "✅ PASS"
FAIL = "❌ FAIL"
INFO = "ℹ️ "

results = []

def test(name, condition, detail_pass, detail_fail, value=None):
    status = PASS if condition else FAIL
    detail = detail_pass if condition else detail_fail
    results.append((name, condition))
    val_str = f"  →  {value}" if value is not None else ""
    print(f"  {status}  {name}")
    print(f"          {detail}{val_str}")
    print()

print()
print("=" * 65)
print("  TESTS DE VALIDATION — 5 CORRECTIONS")
print("=" * 65)


# ══════════════════════════════════════════════════════════════
# CHARGEMENT DES FICHIERS
# ══════════════════════════════════════════════════════════════
FEATURES = "data/features/"
CLEANED  = "data/cleaned/"

try:
    tickets  = pd.read_csv(FEATURES + "tickets_features_spark.csv")
    commits  = pd.read_csv(CLEANED  + "commits_cleaned_spark.csv")
    mrs_df   = pd.read_csv(CLEANED  + "merge_requests_cleaned_spark.csv")
    print(f"  {INFO} Fichiers chargés : {len(tickets)} tickets | "
          f"{len(commits)} commits | {len(mrs_df)} MRs")
    print()
except FileNotFoundError as e:
    print(f"  ❌ Fichier manquant : {e}")
    print("     Lance d'abord : python scripts/02_feature_engineering_spark.py")
    exit(1)


# ══════════════════════════════════════════════════════════════
# FIX 1 — lead_time_hours : couverture élargie
# ══════════════════════════════════════════════════════════════
print("─" * 65)
print("  FIX 1 — lead_time_hours (statuts finaux élargis)")
print("─" * 65)

total        = len(tickets)
with_lead    = tickets["lead_time_hours"].notna().sum()
coverage_pct = round(with_lead / total * 100, 1)

# Avant : 16% (79/494) → après doit être > 30%
test(
    "Couverture lead_time > 30%",
    coverage_pct > 30,
    f"Couverture améliorée",
    f"Couverture encore trop faible (objectif > 30%)",
    value=f"{with_lead}/{total} tickets ({coverage_pct}%)"
)

# Vérifier que Closed/Done sont bien inclus
statuts_avec_lead = tickets[tickets["lead_time_hours"].notna()]["CurrentStatus"].value_counts()
print(f"  {INFO} Statuts avec lead_time_hours :")
for statut, count in statuts_avec_lead.items():
    print(f"          {statut:<30} : {count}")
print()


# ══════════════════════════════════════════════════════════════
# FIX 2 — nb_commits : jointure temporelle
# ══════════════════════════════════════════════════════════════
print("─" * 65)
print("  FIX 2 — nb_commits (jointure temporelle auteur + dates)")
print("─" * 65)

commits_non_zero  = (tickets["nb_commits"] > 0).sum()
commits_total_lié = tickets["nb_commits"].sum()
avg_commits       = round(tickets["nb_commits"].mean(), 2)

# Avant : ~5 commits liés → après doit être >> 5
test(
    "Commits liés > 50 tickets",
    commits_non_zero > 50,
    f"Jointure temporelle active",
    f"Jointure temporelle inefficace (vérifier colonne 'author')",
    value=f"{commits_non_zero} tickets ont des commits liés"
)

test(
    "Total commits liés > 500",
    commits_total_lié > 500,
    f"Volume de commits liés satisfaisant",
    f"Trop peu de commits liés",
    value=f"{int(commits_total_lié)} commits au total"
)

print(f"  {INFO} Moyenne commits / ticket : {avg_commits}")
print(f"  {INFO} Max commits sur un ticket : {int(tickets['nb_commits'].max())}")
print()


# ══════════════════════════════════════════════════════════════
# FIX 3 — cycle_time_hours : statuts In Process étendus
# ══════════════════════════════════════════════════════════════
print("─" * 65)
print("  FIX 3 — cycle_time_hours (statuts In Process étendus)")
print("─" * 65)

with_cycle    = tickets["cycle_time_hours"].notna().sum()
cycle_pct     = round(with_cycle / total * 100, 1)

# Avant : peu de valeurs car seulement "In Process" exact
test(
    "Cycle time calculé > 10% tickets",
    cycle_pct > 10,
    f"Statuts étendus fonctionnent",
    f"Cycle time toujours faible — vérifier les noms de statuts dans Jira",
    value=f"{with_cycle}/{total} tickets ({cycle_pct}%)"
)

# Cohérence : cycle_time doit être ≤ lead_time
both_valid = tickets[
    tickets["lead_time_hours"].notna() & tickets["cycle_time_hours"].notna()
]
if len(both_valid) > 0:
    incoherent = (both_valid["cycle_time_hours"] > both_valid["lead_time_hours"]).sum()
    test(
        "cycle_time ≤ lead_time (cohérence)",
        incoherent == 0,
        f"Toutes les valeurs sont cohérentes",
        f"{incoherent} tickets ont cycle_time > lead_time (anomalie)",
        value=f"vérifié sur {len(both_valid)} tickets"
    )
else:
    print(f"  {INFO} Pas assez de données pour tester la cohérence\n")


# ══════════════════════════════════════════════════════════════
# FIX 4 — nb_mrs : jointure temporelle
# ══════════════════════════════════════════════════════════════
print("─" * 65)
print("  FIX 4 — nb_mrs (jointure temporelle auteur + dates)")
print("─" * 65)

mrs_non_zero  = (tickets["nb_mrs"] > 0).sum()
mrs_total_lié = tickets["nb_mrs"].sum()

test(
    "MRs liées > 30 tickets",
    mrs_non_zero > 30,
    f"Jointure temporelle MRs active",
    f"Jointure temporelle MRs inefficace",
    value=f"{mrs_non_zero} tickets ont des MRs liées"
)

test(
    "avg_merge_time > 0 pour tickets avec MR",
    tickets[tickets["nb_mrs"] > 0]["avg_merge_time_hours"].mean() > 0,
    f"Temps de merge calculé correctement",
    f"avg_merge_time_hours toujours à 0",
    value=f"moy = {round(tickets[tickets['nb_mrs'] > 0]['avg_merge_time_hours'].mean(), 1)}h"
    if mrs_non_zero > 0 else "aucune MR liée"
)

print(f"  {INFO} Total MRs liées : {int(mrs_total_lié)}")
print()


# ══════════════════════════════════════════════════════════════
# FIX 5 — priorité exact > temporel (pas de doublons)
# ══════════════════════════════════════════════════════════════
print("─" * 65)
print("  FIX 5 — Priorité exact > temporel (intégrité)")
print("─" * 65)

# Vérifier qu'il n'y a pas de doublons de TicketKey
duplicates = tickets["TicketKey"].duplicated().sum()
test(
    "Pas de doublons de TicketKey",
    duplicates == 0,
    f"Intégrité des données respectée",
    f"{duplicates} doublons détectés — problème de jointure",
    value=f"{len(tickets)} tickets uniques"
)

# Fix 5 : vérifier que la jointure email produit des résultats cohérents
# (les commits MWBR ne matchent pas Jira DL — c'est normal, projets différents)
commits_email_matched = tickets[tickets["nb_commits"] > 0]
pct_with_commits = round(len(commits_email_matched) / len(tickets) * 100, 1)
test(
    "Jointure email : > 30% tickets ont des commits liés",
    pct_with_commits > 30,
    f"Jointure par email fonctionne correctement",
    f"Trop peu de tickets avec commits",
    value=f"{len(commits_email_matched)}/{len(tickets)} tickets ({pct_with_commits}%)"
)


# ══════════════════════════════════════════════════════════════
# RÉSUMÉ COMPARATIF AVANT / APRÈS
# ══════════════════════════════════════════════════════════════
print("=" * 65)
print("  COMPARAISON AVANT / APRÈS LES CORRECTIONS")
print("=" * 65)
print()
print(f"  {'Métrique':<35} {'AVANT':>10} {'APRÈS':>10}  {'Gain':>8}")
print(f"  {'─'*35} {'─'*10} {'─'*10}  {'─'*8}")

AVANT_LEAD    = 16.0
AVANT_COMMITS = 5
AVANT_MRS     = 0

gain_lead    = f"+{round(coverage_pct - AVANT_LEAD, 1)}%"
gain_commits = f"+{int(commits_total_lié) - AVANT_COMMITS}"
gain_mrs     = f"+{int(mrs_total_lié) - AVANT_MRS}"

print(f"  {'lead_time couverture':<35} {str(AVANT_LEAD)+'%':>10} {str(coverage_pct)+'%':>10}  {gain_lead:>8}")
print(f"  {'commits liés (total)':<35} {str(AVANT_COMMITS):>10} {str(int(commits_total_lié)):>10}  {gain_commits:>8}")
print(f"  {'MRs liées (total)':<35} {str(AVANT_MRS):>10} {str(int(mrs_total_lié)):>10}  {gain_mrs:>8}")
print()

# ── Résultat global ───────────────────────────────────────────
nb_pass = sum(1 for _, ok in results if ok)
nb_fail = sum(1 for _, ok in results if not ok)

print("=" * 65)
print(f"  RÉSULTAT : {nb_pass}/{len(results)} tests passés  |  {nb_fail} échecs")
print("=" * 65)

if nb_fail == 0:
    print()
    print("  🎉 Toutes les corrections fonctionnent !")
    print("     Tu peux lancer : python scripts/03_dora_metrics_spark.py")
else:
    print()
    print("  ⚠️  Corrections à revoir — relis les messages FAIL ci-dessus")
print()