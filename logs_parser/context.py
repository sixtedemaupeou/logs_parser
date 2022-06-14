import os
import logging

import asyncpg

log = logging.getLogger("logs_parser")
context = {}


async def pool():
    if "pool" not in context:
        dsn = os.getenv(
            "DATABASE_URL",
            "postgres://postgres:postgres@localhost:5432/postgres",
        )
        context["pool"] = await asyncpg.create_pool(dsn=dsn, max_size=50)
    return context["pool"]
