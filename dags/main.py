import requests
import chardet
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

file_name = 'russian_houses.csv'
url = 'https://www.dropbox.com/scl/fi/3t8eabjq2kn74vtsgqkj1/russian_houses.csv?rlkey=tm58tp1msy09wxu0zrlhjwsd1&st=hg127tps&dl=1'


def download_from_dropbox():
    response = requests.get(url, allow_redirects=True)

    if response.status_code == 200:
        with open(file_name, 'wb') as file:
            file.write(response.content)
        with open(file_name, 'rb') as temp:
            result = chardet.detect(temp.read(1024))
            original_encoding = result['encoding']
        with open(file_name, 'r', encoding=original_encoding) as temp:
            file_content = temp.read()
        with open(file_name, 'w', encoding='utf-8') as final:
            final.write(file_content)
            print(f"Файл сохранен в {file_name}")
    else:
        print(f'Код ошибки: {response.status_code}')

def query_clickhouse(**kwargs):
    response = requests.get('http://clickhouse_user:8123/?query=SELECT%20version()')
    if response.status_code == 200:
        print(f"ClickHouse version: {response.text}")
    else:
        print(f"Failed to connect to ClickHouse, status code: {response.status_code}")


def data_to_clickhouse(df, table):
    pandas_df = df.toPandas()

    clickhouse_url = f"http://localhost:8123/?query=INSERT INTO russian_houses.{table} FORMAT JSONEachRow"

    data_to_insert = []
    for index, row in pandas_df.iterrows():
        data_to_insert.append(row.to_dict())

    response = requests.post(clickhouse_url, data=json.dumps(data_to_insert))

    if response.status_code == 200:
        print(f"Данные успешно загружены в таблицу {table}!")
    else:
        print(f"Ошибка при загрузке данных: {response.text}")


def data_process():
    spark = SparkSession.builder.appName('russian_houses').getOrCreate()

    df = spark.read.csv(file_name, header=True, inferSchema=True)
    print(f'Количество строк в файле: {df.count()}')

    df = df.withColumn('maintenance_year',
                       when(col('maintenance_year').rlike(r'(\d{4})'),
                            regexp_extract(col('maintenance_year'), r'(\d{4})', 1)).otherwise(None).cast('int')) \
        .withColumn('square',
                    when(col('square') == '-', None).otherwise(regexp_replace(col('square'), ' ', '')).cast('float')) \
        .withColumn('population',
                    when(col('population') == '-', None).otherwise(col('population').cast('int'))) \
        .withColumn('communal_service_id',
                    when(col('communal_service_id') == '-', None).otherwise(col('communal_service_id').cast('int')))

    df = df.where((col('maintenance_year') <= 2025) & (col('maintenance_year') > 0))
    df = df.dropna().drop_duplicates()
    data_to_clickhouse(df, 'houses')

    print(f'Количество строк после очистки: {df.count()}')

    med_avg_years = df.select(median('maintenance_year').cast('int').alias('med_year'),
                              round(avg('maintenance_year')).cast('int').alias('avg_year'))
    print('Медианный и средний год постройки')
    med_avg_years.show()
    data_to_clickhouse(med_avg_years, 'med_avg_years')

    top10_regions = df.select('region', 'house_id') \
        .groupBy('region') \
        .agg(count('house_id').alias('num_of_houses')) \
        .orderBy(desc('num_of_houses')) \
        .limit(10)
    print('Топ 10 регионов по количеству зданий')
    top10_regions.show()
    data_to_clickhouse(top10_regions, 'top10_regions')

    top10_cities = df.select('locality_name', 'house_id') \
        .groupBy('locality_name') \
        .agg(count('house_id').alias('num_of_houses')) \
        .orderBy(desc('num_of_houses')) \
        .limit(10)
    print('Топ 10 городов по количеству зданий')
    top10_cities.show()
    data_to_clickhouse(top10_cities, 'top10_cities')

    max_min_square_by_region = df.groupBy('region') \
        .agg(max(col('square')).alias('max_square'),
             min(col('square')).alias('min_square')) \
        .orderBy(asc('region'))
    print('Максимальная и минимальная площадь в каждом регионе')
    max_min_square_by_region.show()
    data_to_clickhouse(max_min_square_by_region, 'max_min_square_by_region')

    houses_by_decade = df.select('maintenance_year', 'house_id') \
        .withColumn('decade', floor(col('maintenance_year') / 10) * 10) \
        .groupBy('decade') \
        .agg(count('house_id').alias('num_of_houses')) \
        .orderBy('decade')
    print('Количество зданий по десятилетиям')
    houses_by_decade.show()
    data_to_clickhouse(houses_by_decade, 'houses_by_decade')

    spark.stop()


def clickhouse_analyze():
    query = '''SELECT * FROM russian_houses.houses 
               WHERE square > 60
               ORDER BY square DESC
               LIMIT 25'''

    clickhouse_url = f"http://localhost:8123/?query={query}"
    response = requests.post(clickhouse_url)

    if response.status_code == 200:
        for row in response.text.split('\n'):
            print(row)
    else:
        print(f"Ошибка запроса: {response.status_code}")


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 5, 11),
    'owner': 'airflow'
}

dag = DAG(
    'main',
    default_args=default_args,
    description='Pet project dag',
    schedule_interval='@once'
)

download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_from_dropbox,
    dag=dag
)

connection_check_task = PythonOperator(
    task_id='connection_check_task',
    python_callable=query_clickhouse,
    dag=dag
)

data_process_task = PythonOperator(
    task_id='data_process_task',
    python_callable=data_process,
    dag=dag
)

analyze_task = PythonOperator(
    task_id='analyze_task',
    python_callable=clickhouse_analyze,
    dag=dag
)

download_task >> connection_check_task >> data_process_task >> analyze_task
