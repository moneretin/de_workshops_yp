import json
from typing import Optional

import boto3
import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from pyarrow import fs


def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """
    Function returns dictionary with connection credentials

    :param conn_id: str with airflow connection id
    :return: Connection
    """
    conn = BaseHook.get_connection(conn_id)
    return conn


def init_s3_client(s3_host: str, s3_access_key: str, s3_secret_key: str) -> boto3.client:
    """
    Initialization S3 client

    :param s3_host: host
    :param s3_access_key: access key
    :param s3_secret_key: secret key
    return client: S3 client
    """
    client = boto3.client('s3', region_name='us-east-1', use_ssl=True, endpoint_url=s3_host,
                          aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
    return client


def get_currency_rate_info(source_currency: str, target_currency: str, is_initial: bool = False,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> dict:
    """
    Get currency rate information about pair

    :param source_currency: source currency
    :param target_currency: target currency
    :param is_initial: upload historical data. Default: False
    :param start_date: start date for uploading data. Default: None
    :param end_date: end date for uploading data. Default: None
    :return data: information about rate of pair
    """
    params = {'base': source_currency, 'symbols': target_currency}

    if is_initial:
        url: str = f'https://api.exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}'
    else:
        url: str = 'https://api.exchangerate.host/latest'

    try:
        response = requests.get(url, params=params)
        data = response.json()
    except Exception as ex:
        print(f'Unable get currency rate info. Error: {ex}')
        raise ex

    return data


def load_raw_currency_rate_to_s3(**kwargs) -> None:
    """
    Upload JSON file with currency rate to s3

    :param source_currency: source currency
    :param target_currency: target currency
    :param is_initial: upload historical data. Default: False
    :param start_date: start date for uploading data. Default: None
    :param end_date: end date for uploading data. Default: None
    :param conn_name: S3 connection
    :param bucket_prefix: Key prefix for uploading file
    :return
    """
    source_currency: str = kwargs.get('source_currency')
    target_currency: str = kwargs.get('target_currency')
    is_initial: bool = kwargs.get('is_initial', False)
    start_date: Optional[str] = kwargs.get('start_date', None)
    end_date: Optional[str] = kwargs.get('end_date', None)
    conn_name: str = kwargs.get('conn_name')
    bucket_prefix: str = kwargs.get('bucket_prefix')

    s3_conn = get_conn_credentials(conn_name)
    s3_host, s3_bucket, s3_access_key, s3_secret_key = s3_conn.host, s3_conn.schema, s3_conn.login, s3_conn.password
    s3_client = init_s3_client(s3_host, s3_access_key, s3_secret_key)

    raw_data = get_currency_rate_info(source_currency, target_currency, is_initial, start_date, end_date)
    converted_raw_data = json.dumps(raw_data)

    resp = s3_client.put_object(Bucket=s3_bucket, Key=f'{bucket_prefix}/file.json',
                                Body=converted_raw_data)
    status_code = resp.get('ResponseMetadata').get('HTTPStatusCode')
    if status_code == 200:
        print('File uploaded successfully')
    else:
        print('File not uploaded')


def is_bucket_key_exists(dag_setting: str) -> bool:
    """
    S3 sensor which check file by prefix

    :param dag_setting: DAG settings name
    :return: bool variable
    """
    dag_variables = Variable.get(dag_setting, deserialize_json=True)
    conn_name = dag_variables.get('connection_name')
    bucket_prefix = dag_variables.get('bucket_prefix_for_raw')
    s3_conn = get_conn_credentials(conn_name)

    s3_host, s3_bucket, s3_access_key, s3_secret_key = s3_conn.host, s3_conn.schema, s3_conn.login, s3_conn.password
    s3_client = init_s3_client(s3_host, s3_access_key, s3_secret_key)

    bucket_objects = s3_client.list_objects(Bucket=s3_bucket, Prefix=f'{bucket_prefix}/')
    if bucket_objects.get('Contents'):
        print(f'Data were found in {bucket_prefix}/')
        return True
    else:
        print(f'Data not found in {bucket_prefix}/')
        return False


def load_stg_currency_rate_to_s3(dag_setting: str) -> None:
    """
    Upload Parquet file with currency rate to s3

    :param dag_setting: DAG settings name
    :return
    """
    dag_variables = Variable.get(dag_setting, deserialize_json=True)
    conn_name = dag_variables.get('connection_name')
    bucket_prefix_for_raw_data = dag_variables.get('bucket_prefix_for_raw')
    bucket_prefix_for_parsed_data = dag_variables.get('bucket_prefix_for_parsed')
    is_initial = dag_variables.get('is_initial')

    s3_conn = get_conn_credentials(conn_name)
    s3_host, s3_bucket, s3_access_key, s3_secret_key = s3_conn.host, s3_conn.schema, s3_conn.login, s3_conn.password
    s3_client = init_s3_client(s3_host, s3_access_key, s3_secret_key)

    obj = s3_client.get_object(Bucket=s3_bucket, Key=f'{bucket_prefix_for_raw_data}/file.json')
    raw_object_from_s3 = json.loads(obj['Body'].read())

    parsed_data = []
    if not is_initial:
        parsed_data.append([raw_object_from_s3['date'], raw_object_from_s3['rates']['RUB']])
    else:
        for key, value in raw_object_from_s3['rates'].items():
            parsed_data.append([key, value['RUB']])

    s3_fs = fs.S3FileSystem(endpoint_override=s3_host, access_key=s3_access_key, secret_key=s3_secret_key)
    df = pd.DataFrame(parsed_data, columns=['date', 'rate'])
    df.to_parquet(f"{s3_bucket}/{bucket_prefix_for_parsed_data}/file.parquet", engine='pyarrow', filesystem=s3_fs)
