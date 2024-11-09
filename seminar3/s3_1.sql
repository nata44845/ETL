SELECT ID_тикета, Статус, SUM(Длительность) Дл FROM etl_3_final
GROUP BY ID_тикета, Статус
ORDER BY 1,2;


SELECT DISTINCT ID_тикета, Статус, SUM(Длительность)
OVER(PARTITION BY ID_тикета, Статус) Дл FROM etl_3_final 
ORDER BY 1,2;