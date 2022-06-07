"""
###DAG грузит данные по валютной паре из API https://api.exchangerate.host/latest в S3

Что делают таски в этом DAG: \n
1. Прогружает данные из S3 (в каталог data/exchange_rate/raw/usd_rub) в жисонах \n
2. Сенсор, который чекает готовность данных \n
3. Бранч оператор, который выбирает между initial загрузкой и нет (зависит от переменной is_initial) \n
4. Обрабатывет данные и складывает в паркеты в S3 (в каталог data/exchange_rate/parsed/usd_rub) \n

P.S \n
Если initial прогрузка данных, то нужно поменять следующие переменные (граинцы дат включены): \n
IS_INITIAL - True \n
START_DATE - Начальная дата \n
END_DATE - Конечная дата \n
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator

from utils.currency_rate import load_raw_currency_rate_to_s3, is_bucket_key_exists, load_stg_currency_rate_to_s3

DAG_ID = os.path.basename(__file__).split('.')[0]
# Можно так оставить переменные
# SOURCE_CURRENCY: str = 'USD'
# TARGET_CURRENCY: str = 'RUB'
# CONNECTION_NAME: str = 's3_conn'
# IS_INITIAL: bool = False
# START_DATE: Optional[str] = None
# END_DATE: Optional[str] = None

# Можно воспользоваться сущностью Varibale в Airflow
variables = Variable.set(key="s3_load_currency_rate",
                         value={"source_currency": "USD", "target_currency": "RUB", "connection_name": "s3_conn",
                                "bucket_prefix_for_raw": "data/exchange_rate/raw/usd_rub",
                                "bucket_prefix_for_parsed": "data/exchange_rate/parsed/usd_rub", "is_initial": False,
                                "start_date": None, "end_date": None},
                         serialize_json=True)
dag_variables = Variable.get("s3_load_currency_rate", deserialize_json=True)

with DAG(dag_id=DAG_ID, schedule_interval=None, start_date=datetime(2022, 5, 22)) as dag:
    dag.doc_md = __doc__

    load_raw_data_to_s3 = PythonOperator(
        task_id="load_raw_data_to_s3",
        python_callable=load_raw_currency_rate_to_s3,
        op_kwargs={'source_currency': dag_variables.get('source_currency'),
                   'target_currency': dag_variables.get('target_currency'),
                   'is_initial': dag_variables.get('is_initial'),
                   'start_date': dag_variables.get('start_date'),
                   'end_date': dag_variables.get('end_date'),
                   'conn_name': dag_variables.get('connection_name'),
                   'bucket_prefix': dag_variables.get('bucket_prefix_for_raw')},
        retries=1
    )

    check_file_in_s3 = ShortCircuitOperator(
        task_id="check_file_in_s3",
        python_callable=is_bucket_key_exists,
        op_kwargs={'dag_setting': 's3_load_currency_rate'}
    )

    choose_branch = BranchPythonOperator(
        task_id='choose_branch',
        python_callable=lambda: 'load_initial_stg_data_to_s3' if dag_variables.get('is_initial') is True
        else 'load_stg_data_to_s3'
    )

    load_initial_stg_data_to_s3 = PythonOperator(
        task_id="load_initial_stg_data_to_s3",
        python_callable=load_stg_currency_rate_to_s3,
        op_kwargs={'dag_setting': 's3_load_currency_rate'}
    )

    load_stg_data_to_s3 = PythonOperator(
        task_id="load_stg_data_to_s3",
        python_callable=load_stg_currency_rate_to_s3,
        op_kwargs={'dag_setting': 's3_load_currency_rate'}
    )

    load_raw_data_to_s3 >> check_file_in_s3 >> choose_branch >> [load_initial_stg_data_to_s3, load_stg_data_to_s3]
