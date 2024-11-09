/*
C:\Nata\GeekBrains\gb-git\ETL\seminar3
chcp 65001 && spark-shell -i s3.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val t1 = System.currentTimeMillis()
if(1==1){
    var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true")
        .option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("sem3.xlsx")

    df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root&serverTimezone=UTC")
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_3")
    .mode("overwrite").save()

    df1.show(5)

    val query = """
        SELECT ID_тикета, from_unixtime(status_time) status_time, 
        (LEAD(status_time) OVER(PARTITION BY ID_тикета ORDER BY status_time)-status_time)/3600 Длительность,
        CASE WHEN Статус IS NULL THEN @PREV1 ELSE @PREV1 := Статус END Статус, 
        CASE WHEN Группа IS NULL THEN @PREV2 ELSE @PREV2 := Группа END Группа, Назначение FROM
        (SELECT ID_тикета, status_time, Статус,
        IF (ROW_NUMBER() OVER(PARTITION BY ID_тикета ORDER BY status_time) = 1 AND Назначение IS NULL, '',  Группа) Группа, Назначение FROM
        (SELECT DISTINCT a.objectID ID_тикета, a.restime status_time, st.Статус, gr.Группа, gr.Назначение,
        (SELECT @PREV1:=''), (SELECT @PREV2:='')
        FROM (SELECT DISTINCT objectId, restime FROM etl_3
        WHERE fieldname IN ('gname2','status')) a
        LEFT JOIN 
        (SELECT DISTINCT objectID, restime, fieldvalue Статус FROM etl_3
        WHERE fieldname IN ('status')) st
        ON a.objectId=st.objectid AND a.restime=st.restime
        LEFT JOIN 
        (SELECT DISTINCT objectID, restime, fieldvalue Группа, 1 Назначение FROM etl_3
        WHERE fieldname IN ('gname2')) gr
        ON a.objectId=gr.objectid AND a.restime=gr.restime) b1) b2
    """

    spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root&serverTimezone=UTC")
    .option("driver", "com.mysql.cj.jdbc.Driver").option("query", query)
    .load()
    .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root&serverTimezone=UTC")
    .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "etl_3_final")
    .mode("overwrite").save()

	println("task 0")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
