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
etl_3_final