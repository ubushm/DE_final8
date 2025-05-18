import requests
import chardet
import logging

file_name = '/opt/airflow/dags/russian_houses.csv'
url = 'https://www.dropbox.com/scl/fi/3t8eabjq2kn74vtsgqkj1/russian_houses.csv?rlkey=tm58tp1msy09wxu0zrlhjwsd1&st=hg127tps&dl=1'

logging.basicConfig(level=logging.INFO)

def download_from_dropbox():
    response = requests.get(url, allow_redirects=True, stream=True)
    
    # Логируем статус-код и ответ
    logging.info(f"Response status code: {response.status_code}")
    logging.info(f"Response headers: {response.headers}")
    response.raise_for_status()
    total_size = int(response.headers.get('Content-Length', 0))
    logging.info(f"Total size: {total_size} bytes")
    # Можно распечатать часть содержимого для диагностики
    if response.status_code == 200:
        # Логируем первые 100 байт контента
        logging.info(f"Response content preview: {response.content[:100]}")
        response.raise_for_status()
        total_size = int(response.headers.get('Content-Length', 0))
        logging.info(f"Total size: {total_size} bytes")
        
        with open(file_name, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # фильтр для исключения пустых чанков
                    file.write(chunk)
        logging.info("Файл успешно скачан.")
        
        # Определение кодировки файла
        with open(file_name, 'rb') as temp:
            result = chardet.detect(temp.read(1024))
            original_encoding = result['encoding']
            logging.info(f"Detected encoding: {original_encoding}")
        with open(file_name, 'r', encoding=original_encoding) as temp:
            file_content = temp.read()
        with open(file_name, 'w', encoding='utf-8') as final:
            final.write(file_content)
        logging.info(f"Файл перекодирован в utf-8 и сохранен как {file_name}")
    else:
        logging.error(f'Ошибка при скачивании файла: код {response.status_code}')


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 5, 11),
    'owner': 'airflow'
}

dag = DAG(
    'test',
    default_args=default_args,
    description='obvel',
    schedule_interval='@once'
)

download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_from_dropbox,
    dag=dag
)