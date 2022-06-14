import os
import asyncpg

from minicli import cli, run, wrap

context = {}


@cli
async def init_db(drop=False, table=None):
    """Create the DB structure"""
    print("Initializing database...")
    if drop:
        for tbl in ['datasets', 'resources', 'organizations', 'reuses']:
            if table == tbl or not table:
                await context["conn"].execute(f"DROP TABLE IF EXISTS {tbl}")
    await context["conn"].execute(
        """
        CREATE TABLE IF NOT EXISTS datasets(
            id_table serial PRIMARY KEY,
            date DATE NOT NULL,
            access VARCHAR(10) NOT NULL,
            slug VARCHAR NOT NULL,
            id VARCHAR(24) NOT NULL,
            daily_views INTEGER NOT NULL,
            organization_id VARCHAR(24),
            UNIQUE(date, access, id)
        )
    """
    )
    await context["conn"].execute(
        """
        CREATE TABLE IF NOT EXISTS resources(
            id_table serial PRIMARY KEY,
            date DATE NOT NULL,
            access VARCHAR(10) NOT NULL,
            id UUID NOT NULL,
            daily_views INTEGER NOT NULL,
            dataset_id VARCHAR(24),
            UNIQUE(date, access, id)
        )
    """
    )
    await context["conn"].execute(
        """
        CREATE TABLE IF NOT EXISTS organizations(
            id_table serial PRIMARY KEY,
            date DATE NOT NULL,
            access VARCHAR(10) NOT NULL,
            slug VARCHAR NOT NULL,
            id VARCHAR(24) NOT NULL,
            daily_views INTEGER NOT NULL,
            UNIQUE(date, access, id)
        )
    """
    )
    await context["conn"].execute(
        """
        CREATE TABLE IF NOT EXISTS reuses(
            id_table serial PRIMARY KEY,
            date DATE NOT NULL,
            access VARCHAR(10) NOT NULL,
            slug VARCHAR NOT NULL,
            id VARCHAR(24) NOT NULL,
            daily_views INTEGER NOT NULL,
            UNIQUE(date, access, id)
        )
    """
    )


@wrap
async def cli_wrapper():
    dsn = os.getenv(
        "DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres"
    )
    context["conn"] = await asyncpg.connect(dsn=dsn)
    context["dsn"] = dsn
    yield
    await context["conn"].close()


if __name__ == "__main__":
    run()
