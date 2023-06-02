import os
import requests
import logging
from datetime import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.apache.hive.operators.hive import HiveOperator

BASE_ENDPOINT = ('CSSEGISandData/COVID-19/master/'
                 'csse_covid_19_data/csse_covid_19_daily_reports/')
HIVE_TABLE = 'COVID_RESULTS'


def download_covid_data(**kwargs):
    # Создали объект хука (нашего созданного коннекта)
    conn = BaseHook.get_connection(kwargs['conn_id'])
    # url к которому будет обращаться даг для скачивания
    url = conn.host + kwargs['endpoint'] + kwargs['exec_date'] + '.csv'
    logging.info(f'Sending get request to COVID19 repository by url {url}')
    # результат запроса
    response = requests.get(url)
    if response.status_code == 200:
        # папка с файлом, где мы будем хранить результаты
        save_path = ("/home/alexey/airflow_practice/"
                     f"airflow_data/{kwargs['exec_date']}.csv")
        logging.info(f'Successfully get data. Saving to file: {save_path}')
        with open(save_path, 'w') as f:
            f.write(response.text)  # сохраняем текст ответа в файлик
    else:
        raise ValueError(f'Unable get data url: {url}')


def check_if_table_exists(**kwargs):
    # имя таблицы в lower регистр, т.к. hive вернёт название таблицы
    # в нижнем регистре
    table = kwargs['table'].lower()
    conn = HiveCliHook(hive_cli_conn_id=kwargs['conn_id'])
    logging.info(f"Checking if hive table {table} exists.")
    # запрос к hive вернёт название таблицы в нижнем регистре
    query = f"show tables in defaults like '{table}';"
    logging.info(f"Running query: {query}")
    # непосредственно отправка запроса, и сохраняем ответ
    result = conn.run_cli(hql=query)
    if 'OK' in result:
        if table in result:  # здесь реализуется ветвление в Airflow
            logging.info((f"Table {table} exists. Proceed with adding"
                          " new partition."))
            return 'load_to_hive'

        else:
            logging.info((f"Table {table} does not exists. Proceed with"
                         " hive table creation."))
            return 'create_hive_table'
    else:
        raise RuntimeError('Hive returned not OK status while running '
                           f'query: {query}')


default_args = {
    'owner': 'student',  # владелец
    'email': 'student@some-university.edu',  # почта владельца
    'email_on_retry': False,  # уведомление при переотправке писем
    'email_on_failure': False,  # уведомление об ошибке при отправке писем
    'retries': 1,  # сколько перезапусков можно после первой ошибки при запуске
    'retry_delay': 60,  # через сколько секунд перезапускать даг
    'depends_on_past': False,  # зависимость от предыдущих запусков дага
    'start_date': datetime(2022, 5, 25),
    'end_date': datetime(2023, 6, 15)  # необязательный параметр
}


with DAG(
    dag_id='covid_daily_data',
    tags=['pratice', 'daily', 'covid'],
    description='Ежедневная загрузка данных о COVID-19',
    # это cron-выражение, которое означает - 0 часов 7 минут каждого дня
    schedule_interval='0 7 * * *',
    # максимум 1 даг ран параллельно
    max_active_runs=1,
    # разрешили запускать до 4 задач параллельно внутри каждого даг рана
    concurrency=4,
    default_args=default_args,
    # true это значение по умолчанию, означает что будут подсчитаны данные
    # за прошлые дни (т.к. дата старта в прошлом)
    catchup=True,
    # мы можем определить кастомную фукнцию, которую сможем использовать
    # внутри тела дага
    user_defined_macros={
        'convert_date': lambda dt: dt.strftime('%m-%d-%Y')
    }
) as main_dag:

    # ПЛАН ELT

    # EXTRACT
    # 0. Получить дату за которую надо забирать данные
    # темплейт, который позволяет получить текущую дату исполнения Dag-а
    EXEC_DATE = '{{ convert_date(execution_date) }}'

    # 1. Проверить доступность API и данных
    check_if_data_available = HttpSensor(
        task_id='check_if_data_available',  # название проверки в UI
        http_conn_id='covid_api',  # Наш созданный коннект
        # итоговый эндпоинт на каждый день
        endpoint=f'{BASE_ENDPOINT}{EXEC_DATE}.csv',
        # интервал с которым наш сенсор оператор будет пытаться
        # отправить запрос
        poke_interval=60,
        # 10 минут пауза перед следующим запросом после неудачного
        timeout=600,
        # если False, то статус будет "провален", если True, то "пропущен"
        # т.е. допустимо не получить данные
        soft_fail=False,
        # такой режим при паузе отдаёт ресурсы и засыпает, при 'poke'
        # режиме не отдаёт
        mode='reschedule'
    )

    # 2. Загрузить данные (в локальную файловую систему на сервер airflow)
    download_data = PythonOperator(
        task_id='download_data',
        # функция, которая по сути и загрузит все данные
        python_callable=download_covid_data,
        # аргументы, которые мы передадим в функцию 'download_covid_data'
        op_kwargs={
            'conn_id': 'covid_api',
            'endpoint': BASE_ENDPOINT,
            'exec_date': EXEC_DATE
        }
    )

    # LOAD
    # 3. Переместить эти данные в HDFS. После нужно почистить за
    #     собой файл(-ы) на сервере Airflow

    move_data_to_hdfs = BashOperator(
        task_id='move_data_to_hdfs',
        bash_command="""
            hdfs dfs -mkdir -p /covid_data/csv
            hdfs dfs -copyFromLocal /home/alexey/airflow_practice/airflow_data/{exec_date}.csv /covid_data/csv
            rm /airflow_data/{exec_date}.csv
        """.format(exec_date=EXEC_DATE)
    )

    # TRANSFORM
    # 4. Обработать данные - Spark Job, который будет агрегировать
    #    данные на уровне стран и считать статистику
    process_data = SparkSubmitOperator(
        task_id='process_data',
        # ниже папка откуда будем забирать наш скрипт для обработки
        application=os.path.join(
            main_dag.folder,
            'scripts/covid_data_processing.py'
        ),
        # соединение, которые мы создали заранее в UI
        conn_id='spark_conn',
        # Имя таски, которая содержит dag_id
        name=f'{main_dag.dag_id}_process_data',
        # application_args пропускается через шаблонизатор и EXEC_DATE
        # отдаёт нужную нам дату
        application_args=[
            '--exec_data', EXEC_DATE
        ]
    )

    # 5. Создать HIVE таблицу поверх обработанных данных
    #   5.1 Проверить, есть ли нужная нам таблица в Hive
    check_if_hive_table_exists = BranchPythonOperator(
        task_id='check_if_hive_table_exists',
        python_callable=check_if_table_exists,
        op_kwargs={
            'table': HIVE_TABLE,
            'conn_id': 'hive_conn'
        }
    )
    #   5.2 Если таблица отсутствует, то нам нужно её создать
    # данное название должно совпадать с возвратом из функции
    # check_if_table_exists()
    create_hive_table = HiveOperator(
        task_id='create_hive_table',
        hive_cli_conn_id='hive_conn',
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS default.{table}(
                country_region STRING,
                total_confirmed INT,
                total_deaths INT,
                fatality_ratio DOUBLE,
                world_case_pct DOUBLE,
                world_death_pct DOUBLE
            )
            PARTITIONED BY (exec_date STRING)
            ROW FORMAT DELIMETED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '/covid_data/results';
        """.format(table=HIVE_TABLE)
    )

    #   5.3 Если таблица есть, то пропустить создание таблицы и перейти
    #       к обновлению списка портиций
    # данное название должно совпадать с возвратом из функции
    # check_if_table_exists()
    load_to_hive = HiveOperator(
        task_id='load_to_hive',
        hive_cli_conn_id='hive_conn',
        hql=f"MSCK REPAIR TABLE default.{HIVE_TABLE};",
        # недефолтный trigger_rule. По умолчанию для запуска данного
        # оператора должны быть выполнены оба предыдущих оператора:
        # check_if_hive_table_exists и create_hive_table
        # но нам достаточно одного из них
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # тут мы описываем последовательность всех процессов
    check_if_data_available >> download_data >> move_data_to_hdfs >> process_data >> check_if_hive_table_exists
    # в последней задаче у нас ветвление на ещё 2 задачи
    check_if_hive_table_exists >> [create_hive_table, load_to_hive]
    # Если мы создаём таблицу, то все равно надо загрузить данные
    create_hive_table >> load_to_hive
