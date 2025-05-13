from datetime import datetime
from os.path import join
from re2 import split
from loguru import logger

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys
sys.path.append('/home/imedzhidli/airflow')

from utils.config_reader import read_config
from utils.default_args import get_default_dag_args, get_sql_basepath
from utils.script_executor import transfer_postgresql_to_greenplum

config = read_config(__file__)
logger.info("Read config successful")


sql_path = get_sql_basepath(config["file_dag_name"])
scr0 = join(sql_path, 'truncate_gp_source_unowned.sql')
scr1 = join(sql_path, 'insert_gp_source_unowned.sql')

with DAG(
    dag_id=config['dag_name'],
    description=config['description'],
    start_date=datetime.strptime(config['first_start'], '%Y-%m-%d-%H-%M-%S'),
    schedule_interval=None if config['scheduler'] == 'None' else config['scheduler'],
    tags=split(r'[,\s]+', config['tags']),
    catchup=False,
    default_args=get_default_dag_args(config['owner'])
) as dag:
    start = EmptyOperator(
        task_id='start',
    )

    task_insert_gp_source_unowned = PythonOperator(
        task_id='insert_gp_source_unowned',
        python_callable=transfer_postgresql_to_greenplum,
        op_kwargs={'sql_clean_script_path': scr0,
                   'sql_script_path': scr1,
                   'greenplum_table_name': 'source_unowned',
                   }
    )

    end = EmptyOperator(
        task_id='end',
    )

start >> \
    task_insert_gp_source_unowned >> \
    end
