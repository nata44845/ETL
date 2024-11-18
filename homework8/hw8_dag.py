from datetime import datetime, timedelta
from airflow.decorators import dag, task
import json
from sqlalchemy import create_engine
import pendulum
import pandas as pd

# 2. Создайте новый dag;
@dag(
    dag_id="hw_8_dag",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def hw8_dag():
# 1. Скачайте файлы boking.csv, client.csv и hotel.csv;
# 3. Создайте три оператора для получения данных и загрузите файлы. Передайте дата фреймы в оператор трансформации;
    @task
    def hw8_get_bookings():
        bookings = pd.read_csv("/home/airflow/dags/homework8/booking.csv")
        return bookings.to_json()

    @task
    def hw8_get_clients():
        clients = pd.read_csv("/home/airflow/dags/homework8/client.csv")
        return clients.to_json()

    @task
    def hw8_get_hotels():
        hotels = pd.read_csv("/home/airflow/dags/homework8/hotel.csv")
        return hotels.to_json()

# 4. Создайте оператор который будет трансформировать данные:
# — Объедините все таблицы в одну;
# — Приведите даты к одному виду;
# — Удалите невалидные колонки;
# — Приведите все валюты к одной;
    @task
    def hw8_transform(**kwargs):
        ti = kwargs['ti']
        xcom_bookings = ti.xcom_pull(task_ids="hw8_get_bookings")
        xcom_hotels = ti.xcom_pull(task_ids="hw8_get_hotels")
        xcom_clients = ti.xcom_pull(task_ids="hw8_get_clients")

        data_dict = json.loads(xcom_bookings)
        booking = pd.DataFrame(data_dict)

        data_dict = json.loads(xcom_hotels)
        hotel = pd.DataFrame(data_dict)
        
        data_dict = json.loads(xcom_clients)
        client = pd.DataFrame(data_dict)
        
        client['age'].fillna(client['age'].mean(), inplace = True)
        client['age'] = client['age'].astype(int)

        # объединить booking и client
        data = pd.merge(booking, client, on='client_id')
        data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)

        # объединить booking, client & hotel
        data = pd.merge(data, hotel, on='hotel_id')
        data.rename(columns={'name': 'hotel_name'}, inplace=True)

        # Приведите даты к одному виду
        data['booking_date'] = data['booking_date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))

        # Приведите все валюты к одной 
        data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
        data.currency.replace("EUR", "GBP", inplace=True)

        # Удалите невалидные колонки
        data = data.drop('address', axis=1)

        return data.to_json()

# 5. Создайте оператор загрузки в базу данных;
    @task
    def hw8_load_data(**kwargs):
        ti = kwargs['ti']
        con=create_engine("mysql://root:root@localhost:3306/spark")
        xcom_data = ti.xcom_pull(task_ids="hw8_transform")
        data_dict = json.loads(xcom_data)
        data = pd.DataFrame(data_dict)
        data.to_sql('booking_hotel_client',con,schema='spark',if_exists='replace',index=False)

    [hw8_get_bookings(), hw8_get_hotels(), hw8_get_clients()] >> hw8_transform() >> hw8_load_data()


# 6. Запустите dag.
dag = hw8_dag()