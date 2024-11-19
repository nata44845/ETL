import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import re


def hw7_get_temp(**kwargs):
    city = "Krasnoyarsk"
    url = f"https://goweather.herokuapp.com/weather/{city}" 
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json().get('temperature')

def hw7_check_temp(ti):
    temp = ti.xcom_pull(task_ids='hw7_get_temp')
    print(f'Temperature now is {temp}')
    temp = round(float(re.search(r'[+|-]?\d+', temp).group()))
    if temp >= 15:
        return 'hw7_warm'
    else:
        return 'hw7_cold'


with DAG(
        'hw_7_dag',
        start_date=datetime(2024, 11, 1),
        schedule_interval='0 12 * * *',
        catchup=False
) as dag:

    hw7_get_temp = PythonOperator(
        task_id='hw7_get_temp',
        python_callable=hw7_get_temp,
    )

    hw7_check_temp = BranchPythonOperator(
        task_id='hw7_check_temp',
        python_callable=hw7_check_temp,
    )

    hw7_warm = BashOperator(
        task_id='hw7_warm',
        bash_command='echo "Тепло"',
    )

    hw7_cold = BashOperator(
        task_id='hw7_cold',
        bash_command='echo "Холодно"',
    )

    hw7_get_temp >> hw7_check_temp >> [hw7_warm, hw7_cold]