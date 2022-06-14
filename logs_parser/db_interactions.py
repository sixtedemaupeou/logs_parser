import pandas as pd

import context

async def write_dataset_logs_to_db(df: pd.DataFrame) -> None:
    """Write dataset logs to DB
    Arguments:
        df {pd.DataFrame} -- dataset logs dataframe
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        values = ', '.join([f"""('{row.date}', {row.access}, '{row.slug}', '{row.id}', {row.views}, '{row.organization_id}')""" for _, row in df.iterrows()])

        q = f"""
                INSERT INTO datasets (date, access, slug, id, views, organization_id)
                VALUES {values}
                ON CONFLICT (date, access, id)
                DO UPDATE SET views = views + excluded.views;"""
        await connection.execute(q)


async def write_resource_logs_to_db(df: pd.DataFrame) -> None:
    """Write resource logs to DB
    Arguments:
        df {pd.DataFrame} -- resource logs dataframe
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        values = ', '.join([f"""('{row.date}', {row.access}, '{row.id}', {row.views}, '{row.dataset_id}')""" for _, row in df.iterrows()])

        q = f"""
                INSERT INTO resources (date, access, id, views, dataset_id)
                VALUES {values}
                ON CONFLICT (date, access, id)
                DO UPDATE SET views = views + excluded.views;"""
        await connection.execute(q)


async def write_organization_logs_to_db(df: pd.DataFrame) -> None:
    """Write organization logs to DB
    Arguments:
        df {pd.DataFrame} -- organization logs dataframe
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        values = ', '.join([f"""('{row.date}', {row.access}, '{row.slug}', '{row.id}', {row.views})""" for _, row in df.iterrows()])

        q = f"""
                INSERT INTO organizations (date, access, slug, id, views)
                VALUES {values}
                ON CONFLICT (date, access, id)
                DO UPDATE SET views = views + excluded.views;"""
        await connection.execute(q)


async def write_reuse_logs_to_db(df: pd.DataFrame) -> None:
    """Write reuse logs to DB
    Arguments:
        df {pd.DataFrame} -- reuse logs dataframe
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        values = ', '.join([f"""('{row.date}', {row.access}, '{row.id}', {row.views})""" for _, row in df.iterrows()])

        q = f"""
                INSERT INTO reuses (date, access, id, views)
                VALUES {values}
                ON CONFLICT (date, access, id)
                DO UPDATE SET views = views + excluded.views;"""
        await connection.execute(q)

