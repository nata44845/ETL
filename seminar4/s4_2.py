import pyspark, time, platform, sys, os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import inspect, create_engine
from pandas.io import sql
import warnings, matplotlib

warnings.filterwarnings("ignore")
t0 = time.time()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

con = create_engine("mysql://root:root@localhost/spark_4")

spark = SparkSession.builder.appName("sem4").getOrCreate()

if 1==2:
        sql.execute("""drop table if exists spark_4.etl_4_2a""", con)
        sql.execute("""CREATE TABLE if not exists spark_4.etl_4_2a (
                num INT(10) NULL DEFAULT NULL,
                mon DATE NULL DEFAULT NULL,
                sum FLOAT NULL DEFAULT NULL,
                debt_sum FLOAT NULL DEFAULT NULL,
                persent_sum FLOAT NULL DEFAULT NULL,
                debt_left FLOAT NULL DEFAULT NULL,
                percent_summary FLOAT NULL DEFAULT NULL,
                debt_summary FLOAT NULL DEFAULT NULL
                )
                COLLATE='utf8mb4_0900_ai_ci'
                ENGINE=InnoDB""", con)

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("num").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("d4_1.xlsx").limit(1000)\
        .withColumn("percent_summary", sum1(col("percent_sum")).over(w))\
        .withColumn("debt_summary", sum1(col("debt_sum")).over(w))

df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark_4?user=root&password=root\
                                &sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'&jdbcCompliantTruncation=false")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_4_2a")\
        .mode("overwrite").save()


df2 = df1.toPandas()
# Get current axis 
ax = plt.gca()
ax.ticklabel_format(style='plain')
# bar plot
df2.plot(kind='line', 
        x='num', 
        y='debt_summary', 
        color='green', ax=ax)
df2.plot(kind='line', 
        x='num', 
        y='percent_summary', 
        color='red', ax=ax)

# show the plot
plt.legend(['долг_86689', 'проценты_86689','долг_120000', 'проценты_120000','долг_150000', 'проценты_150000'])
plt.show()
exit()

sql.execute("""drop table if exists spark_4.etl_4_2b""",con)
sql.execute("""CREATE TABLE if not exists spark_4.etl_4_2b (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATE NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df3 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("d4_2.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
df3.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_4_2b")\
        .mode("append").save()
df4 = df3.toPandas()
ax.ticklabel_format(style='plain')
# bar plot
df4.plot(kind='line', 
        x='№', 
        y='долг', 
        color='blue', ax=ax)
df4.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='cyan', ax=ax)

sql.execute("""drop table if exists spark.etl_4_2c""",con)
sql.execute("""CREATE TABLE if not exists spark.d4_3 (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATE NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df5 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "true")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("d4_3.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
df5.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_4_2c")\
        .mode("append").save()
df6 = df5.toPandas()
ax.ticklabel_format(style='plain')
# bar plot
df6.plot(kind='line', 
        x='№', 
        y='долг', 
        color='magenta', ax=ax)
df6.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='yellow', ax=ax)



spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))