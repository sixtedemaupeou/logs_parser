import gzip
import os
import re

import pandas as pd
from minio_utils import list_log_files, download_from_minio
from dotenv import load_dotenv

load_dotenv()


def generate_logfile_df(logs_filepath: str) -> pd.DataFrame:
    """
    Generate a dataframe with the logs structured
    """
    arr = []
    lineformat = re.compile(r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<refferer>(\-)|(.+))["]) (["](?P<useragent>.+)["])""", re.IGNORECASE)
    if logs_filepath.endswith(".gz"):
        logfile = gzip.open(logs_filepath)
    else:
        logfile = open(logs_filepath)

    for l in logfile.readlines():
        try:
            l = l.decode('utf-8')
        except AttributeError:
            pass
        data = re.search(lineformat, l)
        if data:
            datadict = data.groupdict()
            arr.append(datadict)

    logfile.close()
    return pd.DataFrame(arr)


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the dataframe
    """
    # df = df.dropna(subset=["url"])
    df['url'] = df['url'].str.split('?').apply(lambda x: x[0])
    df['url'] = df['url'].remove_prefix("/")
    re.compile(r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<refferer>(\-)|(.+))["]) (["](?P<useragent>.+)["])""", re.IGNORECASE)
    return df



log_keys = list_log_files("dev-02-logs")
# for log_key in log_keys:
#     print(log_key)
#     download_from_minio(log_key, log_key)

log_key = log_keys[3]
INPUT_DIR = "./dev-02-logs"

download_from_minio(log_key, log_key)
random_filepath = os.path.join(INPUT_DIR, 'dev.data.gouv.fr.access.log.2.gz')
df = generate_logfile_df(random_filepath)
df = clean_df(df)
print(set(df['url']))


# print(df.columns)
# df = df[df['url'].str.contains('/search')]
# df['search'] = df['url'].apply(lambda x: x.split('&q=')[1].replace('%20',' ')[:-1] if len(x.split('&q=')) > 1 else None)
# print(df)
# print(df.url)
# print(df.columns)