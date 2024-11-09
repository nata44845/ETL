CREATE TABLE if not exists spark.etl_4_2a (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATE NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_general_ci'
ENGINE=INNODB