spark-excel

https://mavenlibs.com/jar/file/com.crealytics/spark-excel_2.12

https://repo.mavenlibs.com/maven/com/crealytics/spark-excel_2.12/3.4.1_0.19.0/spark-excel_2.12-3.4.1_0.19.0.jar?utm_source=mavenlibs.com

mysql-connector

https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.21


Положить в spark-3.4.3-bin-hadoop3\jars


Добавить в соединения &serverTimezone=UTC

cd C:\Nata\GeekBrains\gb-git\ETL\seminar1

chcp 65001 && spark-shell -i s1_4.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"

chcp 65001 && spark-shell -i s1_5.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"

