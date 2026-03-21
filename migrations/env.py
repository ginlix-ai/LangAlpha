"""Alembic environment configuration for langalpha.

Builds the database URL from DB_* environment variables (same as the rest
of the application).
"""

import os
from logging.config import fileConfig
from pathlib import Path
from urllib.parse import quote_plus

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

# Load .env from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Alembic Config object
config = context.config

# Only build from DB_* env vars when the URL hasn't been set programmatically
# (e.g. by the integration test fixture via set_main_option).
_PLACEHOLDER = "driver://user:pass@localhost/dbname"
current_url = config.get_main_option("sqlalchemy.url")
if not current_url or current_url == _PLACEHOLDER:
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    sslmode = "require" if "supabase.com" in db_host else "disable"

    database_url = (
        f"postgresql+psycopg://{quote_plus(db_user)}:{quote_plus(db_password)}"
        f"@{db_host}:{db_port}/{db_name}?sslmode={sslmode}"
    )
    # Escape % as %% for configparser (which treats % as interpolation syntax)
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))

# Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# No ORM metadata — langalpha uses raw psycopg3, not SQLAlchemy models.
# Migrations are written as raw SQL via op.execute().
target_metadata = None


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (generates SQL script)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (applies directly to database)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
