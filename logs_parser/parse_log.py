import os
import pandas as pd

import context
from process_logs import format_and_count, generate_logfile_df, clean_df, enrich_logs_df, format_and_count
from db_interactions import write_dataset_logs_to_db, write_resource_logs_to_db, write_organization_logs_to_db, write_reuse_logs_to_db

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
print(df.columns)

reuse_df = format_and_count(df, 'reuse')
organization_df = format_and_count(df, 'organization')
dataset_df = format_and_count(df, 'dataset')
resource_df = format_and_count(df, 'resource')

write_dataset_logs_to_db(dataset_df)
write_resource_logs_to_db(resource_df)
write_organization_logs_to_db(organization_df)
write_reuse_logs_to_db(reuse_df)
