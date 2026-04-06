"""
Script diagnostic : comprendre pourquoi les jointures échouent
Lance : python scripts/diagnose.py
"""
import pandas as pd

CLEANED  = "data/cleaned/"
FEATURES = "data/features/"

tickets = pd.read_csv(FEATURES + "tickets_features_spark.csv")
commits = pd.read_csv(CLEANED  + "commits_cleaned_spark.csv")
mrs_df  = pd.read_csv(CLEANED  + "merge_requests_cleaned_spark.csv")

print("=" * 60)
print("DIAGNOSTIC 1 — Noms Assignee dans Jira")
print("=" * 60)
assignees = tickets["Assignee"].dropna().unique()
print(f"Nombre d'assignees : {len(assignees)}")
print("Exemples :")
for a in assignees[:10]:
    print(f"   '{a}'")

print()
print("=" * 60)
print("DIAGNOSTIC 2 — Noms auteurs dans Commits")
print("=" * 60)
authors = commits["author"].dropna().unique()
print(f"Nombre d'auteurs commits : {len(authors)}")
print("Exemples :")
for a in authors[:10]:
    print(f"   '{a}'")

print()
print("=" * 60)
print("DIAGNOSTIC 3 — Noms auteurs dans MRs")
print("=" * 60)
for col in ["author_username", "author", "author_name"]:
    if col in mrs_df.columns:
        authors_mrs = mrs_df[col].dropna().unique()
        print(f"Colonne '{col}' — {len(authors_mrs)} auteurs :")
        for a in authors_mrs[:10]:
            print(f"   '{a}'")
        break

print()
print("=" * 60)
print("DIAGNOSTIC 4 — Intersection directe (sans traitement)")
print("=" * 60)
assignees_set = set(tickets["Assignee"].dropna().str.strip().str.lower())
authors_set   = set(commits["author"].dropna().str.strip().str.lower())
match_direct  = assignees_set & authors_set
print(f"Correspondances exactes (insensible casse) : {len(match_direct)}")
if match_direct:
    for m in list(match_direct)[:5]:
        print(f"   '{m}'")

print()
print("=" * 60)
print("DIAGNOSTIC 5 — Statuts Jira distincts (pour Fix 1)")
print("=" * 60)
statuts = tickets["CurrentStatus"].value_counts()
print(statuts.to_string())

print()
print("=" * 60)
print("DIAGNOSTIC 6 — ResolutionDate pour Closed/Done")
print("=" * 60)
for statut in ["Closed", "Done", "Resolved", "To Be Deployed"]:
    subset = tickets[tickets["CurrentStatus"] == statut]
    if len(subset) > 0:
        avec_date = subset["lead_time_hours"].notna().sum()
        print(f"   {statut:<25} : {len(subset)} tickets | {avec_date} avec lead_time")