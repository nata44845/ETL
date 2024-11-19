import datetime
import os
import requests
import pendulum
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator

os.environ["no_proxy"] = "*"

default_args = {
	'owner': 'Nata',
    'depends_on_past': False,	
	'start_date': pendulum.datetime(year=2022, month=6, day=1).in_timezone('Europe/Moscow'),
	'email': ['krppost@yandex.ru'],	
	'email_on_failure': False,	
	'email_on_retry': False,	
	'retries': 0,	
	'retry_delay': timedelta(minutes=5)
}

#DAG
@dag(
    dag_id = "wether-telegram",
    shedule = "@once",
    start_date = pendulum.datetime(2023,1,1, tz="UTC"),
	catchup = False,
	dargun_timeout = datetime.timedelta(minutes=60)
)

def WetherETL():
	send_message_telegram = TelegramOperator(
        task_id = "send_message_telegram",
        token = "",
        chat_id = '',
        text = f"Weather in Krasnoyarsk \n"\
        "Yandex: {{ ti.xcom_pull(task_ids)=['yandex_weather'], keys='weather')[0] }} degrees \n"\
        "Open Weather: {{ ti.xcom_pull(task_ids=['open_weather'], keys='open_weather')[0] degrees}} "
	)

	@task(task_id = "yandex_wether")

	def get_yandex_wether(**kwargs):
		ti = kwargs['ti']
        url = "https://api.weather.yandex.ru/v2/informers/?lat=55.75396&lon=37.620393"
    
		payload = [] 
		headers = {

		}
	
		response = request.request("GET")
		print("test")

	
task1 = BashOperator(
    task_id = 'Hello',
    bash_command = 'python3 /home/airflow/dags/sem6/sem6_1.py',
    dag = dag1
)


#DAG2
dag2 = DAG('sem6_2',
	default_args = default_args,
	description = 'seminar 6',
	catchup = False,
	schedule_interval = '0 */6 * * *')

task2 = BashOperator(
    task_id = 'World',
    bash_command = 'python3 /home/airflow/dags/sem6/sem6_2.py',
    dag = dag2
)