from typing import Dict, List
import gzip
import os
import re
from datetime import datetime

import pandas as pd

from url_parsing import parse_url


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
    df['url'] = df['url'].str.split('?').apply(lambda x: x[0])
    df['url'] = df['url'].str.strip()
    return df


def enrich_logs_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich the dataframe with the category, category details and access method
    """
    additional_info = pd.DataFrame(list(df['url'].apply(parse_url)))
    df = pd.concat([df, additional_info], axis=1)
    df = df[~df['access'].isnull()].reset_index(drop=True)
    return df


def add_id_and_slug(df: pd.DataFrame, id_or_slug_colname: str, catalog_name: str) -> pd.DataFrame:
    catalog_df = pd.read_csv(os.path.join('./catalogs', catalog_name + '.csv'), delimiter=';')
    
    def get_id_slug(row: pd.Series) -> Dict[str, str]:
        try:
            return_dict = {}
            if 'slug' in catalog_df.columns:
                try:
                    slug = catalog_df[catalog_df['id'] == row[id_or_slug_colname]]['slug'].iloc[0]
                    id = row[id_or_slug_colname]
                except IndexError:
                    id = catalog_df[catalog_df['slug'] == row[id_or_slug_colname]]['id'].iloc[0]
                    slug = row[id_or_slug_colname]
                return_dict['slug'] = slug
            else:
                id = row[id_or_slug_colname]

            if 'organization_id' in catalog_df.columns:
                org_id = catalog_df[catalog_df['id'] == id]['organization_id'].iloc[0]
                return_dict['organization_id'] = org_id
            if 'dataset.id' in catalog_df.columns:
                dataset_id = catalog_df[catalog_df['id'] == id]['dataset.id'].iloc[0]
                return_dict['dataset_id'] = dataset_id

            return {'id': id, **return_dict}
        except IndexError:
            # Unsupported name. Eg: search
            return {}

    id_slug_df = pd.DataFrame(list(df.apply(get_id_slug, axis=1)))
    df_with_id_slug = pd.concat([df, id_slug_df], axis=1)
    df_with_id_slug.drop(columns=[id_or_slug_colname], inplace=True)
    if 'id' in df_with_id_slug.columns:
        df_with_id_slug = df_with_id_slug[~df_with_id_slug['id'].isnull()].reset_index(drop=True)
    print(df_with_id_slug)
    return df_with_id_slug


def group_by_date(df: pd.DataFrame, groupby: List[str]) -> pd.DataFrame:
    for col in groupby:
        if col not in df.columns:
            print(f'Missing column {col}')
            return pd.DataFrame(columns=['date']+groupby)
    df['date'] = df['dateandtime'].apply(lambda x: datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').date())
    return df[['date']+groupby].groupby(['date'] + groupby).size().sort_values(ascending=True).reset_index(name='count')


def format_and_count(df: pd.DataFrame, category: str) -> pd.DataFrame:
    identifier = {
        'reuse': 'reuse_id_or_slug',
        'organization': 'organization_id_or_slug',
        'dataset': 'dataset_id_or_slug',
        'resource': 'resource_id'
    }[category]
    df = df[df['category'] == category][['ipaddress', 'dateandtime', 'url', 'access', identifier]].reset_index(drop=True)
    df = add_id_and_slug(df, identifier, f'{category}s')
    df = group_by_date(df, ['access', 'id'])
    print(df)
    return df