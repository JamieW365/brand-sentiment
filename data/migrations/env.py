import asyncio  # Required to run the async migration coroutine from a sync entrypoint
from logging.config import fileConfig

from sqlalchemy import pool
# async_engine_from_config creates an async engine from alembic.ini config —
# required because our project uses asyncpg, not a sync driver.
from sqlalchemy.ext.asyncio import async_engine_from_config, AsyncConnection

from alembic import context

# Base holds the metadata Alembic diffs against to autogenerate migrations.
# All models that inherit from Base are automatically included.
from shared.db.base import Base

# Import every model so it registers itself with Base.metadata before
# autogenerate runs. A missing import = a missing table in the migration.
from shared.db.models import Review  # noqa: F401

# Settings resolves the database URL from .env — same source of truth
# used by the application, so migrations and the app can never diverge.
from shared.config.settings import get_settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override the URL from alembic.ini with the one from settings.
# This means DATABASE_URL in .env is the single source of truth —
# no need to maintain a separate URL in alembic.ini.
config.set_main_option("sqlalchemy.url", get_settings().database_url)

# Pointing target_metadata at Base.metadata enables autogenerate —
# `alembic revision --autogenerate` will diff the live DB against
# all registered models and generate the migration script.
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations without a live DB connection.

    Outputs raw SQL rather than executing it — useful for generating
    migration scripts in CI environments without a running Postgres instance.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: AsyncConnection) -> None:
    """Apply migrations using an active synchronous connection.

    Alembic's migration runner is synchronous. run_sync() in the async
    context calls this function, bridging the async/sync boundary cleanly.
    """
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create an async engine and run migrations against it."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        # NullPool is correct for migrations — we don't want connection pooling
        # for a one-shot operation. Each migration run opens and closes cleanly.
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    # Dispose explicitly to close all connections after migrations complete.
    await connectable.dispose()


def run_migrations_online() -> None:
    """Entry point for online migrations — drives the async coroutine."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
