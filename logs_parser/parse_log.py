import asyncio
import os
import pandas as pd

from logs_parser.db_interactions import write_logs_to_db
from logs_parser.minio_utils import download_from_minio, list_log_files
from logs_parser.process_logs import generate_logfile_df, clean_df, enrich_logs_df



def download_files(minio_folder: str) -> None:
    log_keys = list_log_files(minio_folder)
    for log_key in log_keys:
        download_from_minio(log_key, log_key)



async def record_daily_views(minio_folder: str) -> None:
    download_files(minio_folder)
    logs_folder = f"./{minio_folder}"
    all_log_files = os.listdir(logs_folder)
    df = pd.concat([generate_logfile_df(os.path.join(logs_folder, filename)) for filename in all_log_files], ignore_index=True)
    df = clean_df(df)
    df = enrich_logs_df(df)
    await write_logs_to_db(df)
    # TODO: Cleanup logs folder
