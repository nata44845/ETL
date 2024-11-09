/*
Задание 1
Скачайте датасет. Проанализируйте его на наличие пропусков используя pandas.
Напишите в чат какие пропущенные значения вы обнаружили и причины их появления.

C:\Nata\GeekBrains\gb-git\ETL\seminar2
chcp 65001 && spark-shell -i C:\Nata\GeekBrains\gb-git\ETL\seminar2\s2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val t1 = System.currentTimeMillis()
if(1==1){
    var df1 = spark.read.option("delimiter",",")
        .option("header", "true")
        .option("useHeader", "false")
        .csv("s2_data.csv")

    df1 = df1
        // Заголовки
        // .select(df1.columns.map(x => col(x).as(x.toUpperCase())):_*)
        // .selectExpr(df1.columns.map(c => c.toUpperCase): _*)
        // Данные
        //.applymap(lambda: s: s.upper() if type(s) == str else s)
        .withColumn("children",col("children").cast("int"))
        .withColumn("days_employed",col("days_employed").cast("int"))
        .withColumn("total_income",col("total_income").cast("float"))
        .withColumn("purpose_category",
            when(col("purpose").like("%авто%"), "операции с автомобилем")
            when(col("purpose").like("%недвиж%")||col("purpose").like("%жил%"), "операции с недвижимостью")
            when(col("purpose").like("%образ%"), "получение образования")
            when(col("purpose").like("%свадь%"), "проведение свадьбы")
        )
        .withColumn("total_income2",
            when(col("total_income").isNotNull,col("total_income"))
            .otherwise(avg("total_income").over(Window.partitionBy("income_type").orderBy("income_type")))
        )
        .withColumn("total_income2",col("total_income2").cast("float"))
        .dropDuplicates()

    df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root&serverTimezone=UTC")
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_2_1")
    .mode("overwrite").save()

    // df1.show(5)

    val s = df1.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
    val df2 = df1.agg(s.head, s.tail:_*)
    val t = df2.columns.map(c => df2.select(lit(c).alias("col_name"), col(c).alias("null_count")))
    val df_agg_col = t.reduce((df1,df2) => df1.union(df2))
    df_agg_col.show()

	println("task 1")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
