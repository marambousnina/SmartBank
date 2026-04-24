"""
Script ML - Prédiction de retard des projets
Entraîne un Random Forest sur les tickets et agrège les prédictions par projet.
Output :
  - models/delay_model.pkl            : modèle sérialisé
  - models/delay_model_features.json  : liste des features attendues
  - data/predictions/project_delay_predictions.csv : résultats par projet
"""

import os
import json
import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score

# ── Chemins ──────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH   = os.path.join(BASE_DIR, "data", "features", "tickets_features_spark.csv")
MODEL_DIR   = os.path.join(BASE_DIR, "models")
PRED_DIR    = os.path.join(BASE_DIR, "data", "predictions")
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(PRED_DIR,  exist_ok=True)

# ── Chargement ────────────────────────────────────────────────────────────────
print("Chargement des données...")
df = pd.read_csv(DATA_PATH)
print(f"  {df.shape[0]} tickets, {df.shape[1]} colonnes")

# ── Features & cible ─────────────────────────────────────────────────────────
# Features numériques disponibles en cours de ticket (pas de fuite temporelle)
NUM_FEATURES = [
    "nb_status_changes",
    "time_blocked_hours",
    "time_in_review_hours",
    "time_in_qa_hours",
    "time_correction_review_hours",
    "time_correction_qa_hours",
    "nb_commits",
    "nb_mrs",
    "was_blocked",
    "is_bug",
    # risk_score exclu : derive directement de is_delayed (fuite de donnees)
    "nb_tickets_total",
    "nb_tickets_done",
    "completion_rate",
]

# Features catégorielles
CAT_FEATURES = ["IssueType"]   # Priority = toujours 'Medium' → inutile

TARGET = "is_delayed"

# ── Nettoyage ─────────────────────────────────────────────────────────────────
df_model = df[NUM_FEATURES + CAT_FEATURES + [TARGET, "ProjectKey", "TicketKey"]].copy()

# Remplir les NaN numériques par la médiane
for col in NUM_FEATURES:
    df_model[col] = pd.to_numeric(df_model[col], errors="coerce")
    df_model[col] = df_model[col].fillna(df_model[col].median())

# Encodage des catégorielles
encoders = {}
for col in CAT_FEATURES:
    le = LabelEncoder()
    df_model[col + "_enc"] = le.fit_transform(df_model[col].fillna("Unknown"))
    encoders[col] = le

FEATURE_COLS = NUM_FEATURES + [c + "_enc" for c in CAT_FEATURES]

X = df_model[FEATURE_COLS].values
y = df_model[TARGET].values

print(f"\nDistribution cible : {pd.Series(y).value_counts().to_dict()}")
print(f"Features utilisées ({len(FEATURE_COLS)}) : {FEATURE_COLS}")

# ── Entraînement ─────────────────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

model = RandomForestClassifier(
    n_estimators=200,
    max_depth=10,
    min_samples_leaf=5,
    class_weight="balanced",   # compense le déséquilibre 77/23
    random_state=42,
    n_jobs=-1,
)
model.fit(X_train, y_train)

# ── Évaluation ────────────────────────────────────────────────────────────────
y_pred  = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print("\n--- Rapport de classification ---")
print(classification_report(y_test, y_pred, target_names=["A temps", "En retard"]))
print("Matrice de confusion :")
print(confusion_matrix(y_test, y_pred))
print(f"ROC-AUC : {roc_auc_score(y_test, y_proba):.3f}")

# Cross-validation
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
cv_scores = cross_val_score(model, X, y, cv=cv, scoring="f1")
print(f"\nF1 cross-val (5 folds) : {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")

# ── Importance des features ───────────────────────────────────────────────────
importances = pd.Series(model.feature_importances_, index=FEATURE_COLS)
print("\n--- Top 10 features les plus importantes ---")
print(importances.sort_values(ascending=False).head(10).to_string())

# ── Prédictions sur tous les tickets ─────────────────────────────────────────
df_model["delay_proba"]    = model.predict_proba(X)[:, 1]
df_model["delay_predicted"] = model.predict(X)

# ── Agrégation par projet ─────────────────────────────────────────────────────
project_preds = df_model.groupby("ProjectKey").agg(
    nb_tickets           = ("TicketKey", "count"),
    nb_delayed_actual    = (TARGET, "sum"),
    nb_delayed_predicted = ("delay_predicted", "sum"),
    avg_delay_proba      = ("delay_proba", "mean"),
    max_delay_proba      = ("delay_proba", "max"),
).reset_index()

project_preds["delay_rate_actual_pct"]    = (project_preds["nb_delayed_actual"]    / project_preds["nb_tickets"] * 100).round(1)
project_preds["delay_rate_predicted_pct"] = (project_preds["nb_delayed_predicted"] / project_preds["nb_tickets"] * 100).round(1)
project_preds["avg_delay_proba"]          = (project_preds["avg_delay_proba"] * 100).round(1)
project_preds["max_delay_proba"]          = (project_preds["max_delay_proba"] * 100).round(1)

# Niveau de risque projet basé sur la probabilité moyenne
def risk_label(p):
    if p >= 70:   return "Critique"
    if p >= 50:   return "Élevé"
    if p >= 30:   return "Modéré"
    return "Faible"

project_preds["risk_level"]         = project_preds["avg_delay_proba"].apply(risk_label)
project_preds["project_will_delay"] = (project_preds["avg_delay_proba"] >= 50).astype(int)

print("\n--- Predictions par projet ---")
print(project_preds[["ProjectKey", "nb_tickets", "delay_rate_actual_pct",
                      "avg_delay_proba", "risk_level", "project_will_delay"]].to_string(index=False))

# ── Sauvegarde ────────────────────────────────────────────────────────────────
model_path    = os.path.join(MODEL_DIR, "delay_model.pkl")
features_path = os.path.join(MODEL_DIR, "delay_model_features.json")
pred_path     = os.path.join(PRED_DIR,  "project_delay_predictions.csv")

joblib.dump({"model": model, "encoders": encoders, "feature_cols": FEATURE_COLS}, model_path)
with open(features_path, "w") as f:
    json.dump(FEATURE_COLS, f, indent=2)

project_preds.to_csv(pred_path, index=False)

print(f"\nModèle sauvegardé       : {model_path}")
print(f"Features sauvegardées   : {features_path}")
print(f"Prédictions projets     : {pred_path}")
print("\nTerminé.")
