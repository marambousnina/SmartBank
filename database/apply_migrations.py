"""
SmartBank — Applique toutes les migrations SQL manquantes.
Usage :
    python database/apply_migrations.py
"""

import sys
from pathlib import Path
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_connection import get_engine

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

# Ordre d'application
MIGRATION_FILES = [
    "03_add_project_status.sql",
    "04_fix_missing_columns.sql",
]


def run_migrations():
    engine = get_engine()

    for filename in MIGRATION_FILES:
        filepath = MIGRATIONS_DIR / filename
        if not filepath.exists():
            print(f"[SKIP]  {filename} — fichier introuvable")
            continue

        sql = filepath.read_text(encoding="utf-8-sig")

        # Sépare les statements (séparateur = ligne vide après ;)
        statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]

        print(f"\n[>>] Application de {filename} ({len(statements)} statements)")
        with engine.begin() as conn:
            for stmt in statements:
                # Filtre les blocs purement commentaires
                lines = [l for l in stmt.splitlines() if l.strip() and not l.strip().startswith("--")]
                if not lines:
                    continue
                clean = "\n".join(lines)
                try:
                    conn.execute(text(clean))
                    print(f"  [OK]  {clean[:80].replace(chr(10), ' ')}…")
                except Exception as e:
                    err = str(e).split("\n")[0]
                    print(f"  [WARN] {clean[:60].replace(chr(10), ' ')}… → {err}")

    print("\n[DONE] Migrations terminées.\n")


if __name__ == "__main__":
    run_migrations()
