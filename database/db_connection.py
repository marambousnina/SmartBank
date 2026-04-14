"""
SmartBank Metrics - Module de connexion PostgreSQL
Utilise SQLAlchemy + psycopg2 avec gestion de configuration YAML.
"""

import os
import yaml
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass


# ── Résolution des chemins ──────────────────────────────────────────────────
ROOT_DIR    = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT_DIR / "config" / "config.yaml"


def _load_db_config() -> dict:
    """
    Charge la config DB depuis config.yaml, avec fallback sur variables d'env.
    Priorité: variables d'env (.env) > config.yaml > valeurs par défaut.
    """
    defaults = {
        "host":     "localhost",
        "port":     5432,
        "database": "smartbank_db",
        "user":     "smartbank_user",
        "password": "admin",
    }

    # Lecture depuis config.yaml
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8-sig", errors="ignore") as f:
            cfg = yaml.safe_load(f) or {}
        db_cfg = cfg.get("database", {})
        defaults.update({k: v for k, v in db_cfg.items() if v})

    # Override par variables d'environnement
    env_mapping = {
        "SMARTBANK_DB_HOST":     "host",
        "SMARTBANK_DB_PORT":     "port",
        "SMARTBANK_DB_NAME":     "database",
        "SMARTBANK_DB_USER":     "user",
        "SMARTBANK_DB_PASSWORD": "password",
    }
    for env_key, cfg_key in env_mapping.items():
        val = os.environ.get(env_key)
        if val:
            defaults[cfg_key] = int(val) if cfg_key == "port" else val

    return defaults


def get_engine(pool_size: int = 5, echo: bool = False):
    """
    Retourne un moteur SQLAlchemy avec connection pooling.

    Args:
        pool_size: Nombre de connexions dans le pool (défaut 5).
        echo: Si True, affiche les requêtes SQL (debug uniquement).
    """
    cfg = _load_db_config()
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    engine = create_engine(
        url,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=10,
        pool_pre_ping=True,  # Vérifie les connexions avant utilisation
        echo=echo,
    )
    return engine


def get_session(engine=None):
    """Retourne une session SQLAlchemy (pour des opérations ORM)."""
    if engine is None:
        engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def test_connection() -> bool:
    """
    Teste la connexion à la base de données.
    Retourne True si OK, False sinon.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print("[OK] Connexion PostgreSQL etablie")
            print(f"     Version: {version[:60]}")
            return True
    except Exception as e:
        print(f"[ERREUR] Connexion impossible: {str(e).encode('ascii', errors='replace').decode()}")
        return False


if __name__ == "__main__":
    test_connection()
