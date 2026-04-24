# -*- coding: utf-8 -*-
"""
Router FastAPI - Predictions de retard de projets
GET /predict/projects          -> predictions pour tous les projets (depuis CSV)
GET /predict/projects/{key}    -> prediction pour un projet specifique
POST /predict/ticket           -> prediction temps-reel pour un ticket custom
"""

import os
import json
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

# ── Chemins ───────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
MODEL_PKL = BASE_DIR / "models" / "delay_model.pkl"
PRED_CSV  = BASE_DIR / "data" / "predictions" / "project_delay_predictions.csv"

router = APIRouter(prefix="/api/predict", tags=["Predictions ML"])

# ── Chargement du modele (une seule fois au demarrage) ────────────────────────
_model_bundle = None

def get_model():
    global _model_bundle
    if _model_bundle is None:
        if not MODEL_PKL.exists():
            raise HTTPException(
                status_code=503,
                detail="Modele ML non trouve. Lancez scripts/04_ml_delay_prediction.py d'abord."
            )
        _model_bundle = joblib.load(MODEL_PKL)
    return _model_bundle


# ── Schema entree prediction temps-reel ───────────────────────────────────────
class TicketInput(BaseModel):
    nb_status_changes:           float = Field(0,   description="Nombre de changements de statut")
    time_blocked_hours:          float = Field(0.0, description="Heures passees en blocage")
    time_in_review_hours:        float = Field(0.0, description="Heures en review")
    time_in_qa_hours:            float = Field(0.0, description="Heures en QA")
    time_correction_review_hours:float = Field(0.0, description="Heures correction review")
    time_correction_qa_hours:    float = Field(0.0, description="Heures correction QA")
    nb_commits:                  float = Field(0,   description="Nombre de commits lies")
    nb_mrs:                      float = Field(0,   description="Nombre de MRs lies")
    was_blocked:                 int   = Field(0,   description="Ticket bloque (0/1)")
    is_bug:                      int   = Field(0,   description="Est un bug (0/1)")
    nb_tickets_total:            int   = Field(100, description="Total tickets du projet")
    nb_tickets_done:             int   = Field(0,   description="Tickets termines dans le projet")
    completion_rate:             float = Field(0.0, description="Taux de completion projet (%)")
    issue_type:                  str   = Field("Story", description="Type: Story | Task | Epic | Tache | Sous-tache")


# ── Endpoint 1 : toutes les predictions projet ────────────────────────────────
@router.get("/projects", summary="Predictions de retard pour tous les projets")
def predict_all_projects():
    """
    Retourne les predictions de retard pour chaque projet,
    basees sur le modele Random Forest entraine.
    """
    if not PRED_CSV.exists():
        raise HTTPException(
            status_code=503,
            detail="Fichier de predictions non trouve. Lancez scripts/04_ml_delay_prediction.py."
        )
    df = pd.read_csv(PRED_CSV)
    records = df.to_dict(orient="records")
    return {
        "total": len(records),
        "model": "RandomForest (92% accuracy, ROC-AUC 0.977)",
        "data": records
    }


# ── Endpoint 2 : prediction pour un projet specifique ────────────────────────
@router.get("/projects/{project_key}", summary="Prediction de retard pour un projet")
def predict_project(project_key: str):
    """
    Retourne la prediction de retard (probabilite, niveau de risque, verdict)
    pour le projet donne.
    """
    if not PRED_CSV.exists():
        raise HTTPException(
            status_code=503,
            detail="Fichier de predictions non trouve. Lancez scripts/04_ml_delay_prediction.py."
        )
    df = pd.read_csv(PRED_CSV)
    row = df[df["ProjectKey"].str.upper() == project_key.upper()]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Projet '{project_key}' non trouve.")

    r = row.iloc[0].to_dict()
    return {
        "project_key":             r["ProjectKey"],
        "nb_tickets":              int(r["nb_tickets"]),
        "delay_rate_actual_pct":   float(r["delay_rate_actual_pct"]),
        "delay_rate_predicted_pct":float(r["delay_rate_predicted_pct"]),
        "avg_delay_probability":   float(r["avg_delay_proba"]),
        "max_delay_probability":   float(r["max_delay_proba"]),
        "risk_level":              r["risk_level"],
        "project_will_delay":      bool(r["project_will_delay"]),
        "verdict": (
            "Ce projet sera probablement EN RETARD."
            if r["project_will_delay"]
            else "Ce projet devrait etre livre A TEMPS."
        ),
    }


# ── Endpoint 3 : prediction temps-reel pour un ticket custom ─────────────────
@router.post("/ticket", summary="Prediction temps-reel pour un ticket")
def predict_ticket(ticket: TicketInput):
    """
    Predit si un ticket sera en retard a partir de ses metriques actuelles.
    Utile pour une alerte proactive en cours de sprint.
    """
    bundle = get_model()
    model      = bundle["model"]
    encoders   = bundle["encoders"]
    feat_cols  = bundle["feature_cols"]

    # Encodage IssueType
    le = encoders.get("IssueType")
    known_classes = list(le.classes_)
    issue_type_val = ticket.issue_type if ticket.issue_type in known_classes else "Story"
    issue_type_enc = int(le.transform([issue_type_val])[0])

    values = [
        ticket.nb_status_changes,
        ticket.time_blocked_hours,
        ticket.time_in_review_hours,
        ticket.time_in_qa_hours,
        ticket.time_correction_review_hours,
        ticket.time_correction_qa_hours,
        ticket.nb_commits,
        ticket.nb_mrs,
        ticket.was_blocked,
        ticket.is_bug,
        ticket.nb_tickets_total,
        ticket.nb_tickets_done,
        ticket.completion_rate,
        issue_type_enc,
    ]

    X = np.array(values).reshape(1, -1)
    proba      = float(model.predict_proba(X)[0][1] * 100)
    prediction = int(model.predict(X)[0])

    def risk_label(p):
        if p >= 70:  return "Critique"
        if p >= 50:  return "Eleve"
        if p >= 30:  return "Modere"
        return "Faible"

    return {
        "delay_probability_pct": round(proba, 1),
        "will_be_delayed":       bool(prediction),
        "risk_level":            risk_label(proba),
        "verdict": (
            f"Ce ticket a {proba:.1f}% de risque d'etre EN RETARD."
            if prediction
            else f"Ce ticket devrait etre livre A TEMPS ({proba:.1f}% de risque)."
        ),
    }
