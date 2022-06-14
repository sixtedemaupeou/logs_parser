import asyncio
import os
import pandas as pd

from logs_parser.process_logs import format_and_count, generate_logfile_df, clean_df, enrich_logs_df, format_and_count
from logs_parser.db_interactions import write_logs_to_table

# log_keys = list_log_files("dev-02-logs")
# for log_key in log_keys:
#     print(log_key)
#     download_from_minio(log_key, log_key)

# log_key = log_keys[3]
INPUT_DIR = "./dev-02-logs"

# download_from_minio(log_key, log_key)
# random_filepath = os.path.join(INPUT_DIR, 'dev.data.gouv.fr.access.log.2.gz')
all_log_files = ['demo.data.gouv.fr.access.log.1'] # os.listdir(INPUT_DIR)
df = pd.concat([generate_logfile_df(os.path.join(INPUT_DIR, filename)) for filename in all_log_files], ignore_index=True)
df = clean_df(df)
df = enrich_logs_df(df)


async def write_logs_to_db(df: pd.DataFrame):
    for category in ['reuses', 'organizations', 'datasets', 'resources']:
        category_df = format_and_count(df, category)
        if not category_df.empty:
            await write_logs_to_table(category_df, category)


asyncio.run(write_logs_to_db(df), debug=True)
