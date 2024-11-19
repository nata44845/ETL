## Домашняя работа 6

1. Создайте новый граф. Добавьте в него BashOperator, который будет генерировать рандомное число и печатать его в
консоль.

2. Создайте PythonOperator, который генерирует рандомное число, возводит его в квадрат и выводит в консоль исходное число и результат.

3. Сделайте оператор, который отправляет запрос к https://goweather.herokuapp.com/weather/"location" (вместо location используйте ваше местоположение).

4. Задайте последовательный порядок выполнения операторов.


https://cloud.mail.ru/public/s2Lz/7FrokxZuW

Ubuntu_2004.2021.825.0_x64.appx

Для просмотра MobaXterm

- WSL 3.8.10-0ubuntu1~20.04.12
- airflow 2.2.5
- sqlalchemy 1.3.34 (идет с airflow)
- pandas 1.3.5

sudo service mysql start

airflow scheduler & airflow webserver -p 8089

Соединение для сайта погоды

![API](weather_api.png)

DAG

![DAG](dag.png)

log1

![log1](log1.png)

log2

![log1](log2.png)

log3

![log1](log3.png)

log4

![log1](log4.png)