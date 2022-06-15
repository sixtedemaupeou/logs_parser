import pandas as pd

import logs_parser.context as context
from logs_parser.process_logs import format_and_count


async def write_dataset_logs_to_db(df: pd.DataFrame) -> None:
    """Write dataset logs to DB
    Arguments:
        df {pd.DataFrame} -- dataset logs dataframe
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        values = ', '.join([f"""('{row.date}', '{row.access}', '{row.slug}', '{row.id}', '{row.daily_views}', '{row.organization_id}')""" for _, row in df.iterrows()])

        q = f"""
                INSERT INTO datasets (date, access, slug, id, daily_views, organization_id)
                VALUES {values}
                ON CONFLICT (date, access, id)
                DO UPDATE SET daily_views = datasets.daily_views + EXCLUDED.daily_views;"""
        await connection.execute(q)


async def write_resource_logs_to_db(df: pd.DataFrame) -> None:
    """Write resource logs to DB
    Arguments:
        df {pd.DataFrame} -- resource logs dataframe
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        values = ', '.join([f"""('{row.date}', '{row.access}', '{row.id}', {row.daily_views}, '{row.dataset_id}')""" for _, row in df.iterrows()])

        q = f"""
                INSERT INTO resources (date, access, id, daily_views, dataset_id)
                VALUES {values}
                ON CONFLICT (date, access, id)
                DO UPDATE SET daily_views = resources.daily_views + EXCLUDED.daily_views;"""
        await connection.execute(q)


async def write_organization_logs_to_db(df: pd.DataFrame) -> None:
    """Write organization logs to DB
    Arguments:
        df {pd.DataFrame} -- organization logs dataframe
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        values = ', '.join([f"""('{row.date}', '{row.access}', '{row.slug}', '{row.id}', '{row.daily_views}')""" for _, row in df.iterrows()])

        q = f"""
                INSERT INTO organizations (date, access, slug, id, daily_views)
                VALUES {values}
                ON CONFLICT (date, access, id)
                DO UPDATE SET daily_views = organizations.daily_views + EXCLUDED.daily_views;"""
        await connection.execute(q)


async def write_reuse_logs_to_db(df: pd.DataFrame) -> None:
    """Write reuse logs to DB
    Arguments:
        df {pd.DataFrame} -- reuse logs dataframe
    """
    pool = await context.pool()
    async with pool.acquire() as connection:
        values = ', '.join([f"""('{row.date}', '{row.access}', '{row.id}', '{row.daily_views}')""" for _, row in df.iterrows()])

        q = f"""
                INSERT INTO reuses (date, access, id, daily_views)
                VALUES {values}
                ON CONFLICT (date, access, id)
                DO UPDATE SET daily_views = reuses.daily_views + EXCLUDED.daily_views;"""
        await connection.execute(q)


async def write_logs_to_table(df: pd.DataFrame, table: str) -> None:
    """Write logs to DB
    Arguments:
        df {pd.DataFrame} -- logs dataframe
        table {str} -- table name
    """
    if table == 'datasets':
        await write_dataset_logs_to_db(df)
    elif table == 'resources':
        await write_resource_logs_to_db(df)
    elif table == 'organizations':
        await write_organization_logs_to_db(df)
    elif table == 'reuses':
        await write_reuse_logs_to_db(df)
    else:
        raise ValueError(f'Unknown table {table}')


async def write_logs_to_db(df: pd.DataFrame):
    for category in ['reuses', 'organizations', 'datasets', 'resources']:
        category_df = format_and_count(df, category)
        if not category_df.empty:
            await write_logs_to_table(category_df, category)