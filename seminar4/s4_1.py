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

con = create_engine("mysql://root:root@localhost/spark")

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("sem4").getOrCreate()

columns = ["id", "category_id", "rate", "title", "author"]

data = [("1", "1", "5", "java", "author1"), 
        ("2", "1", "5", "scala", "author2"), 
        ("3", "1", "5", "python", "author3")
    ]

if 1==1:
    df = spark.createDataFrame(data, columns)
    df.withColumn("id", col("id").cast("int"))\
    .withColumn("category_id", col("category_id").cast("int"))\
    .withColumn("rate", col("rate").cast("int"))\
    .withColumn("dt", current_timestamp())\
    .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")\
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_4_1a")\
    .mode("overwrite").save()

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
        .load("sem4.xlsx")\
        .withColumn("dt", current_timestamp())\
        .where(col("title")=="news")\
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root&serverTimezone=UTC")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_4_1a")\
        .mode("append").save()
