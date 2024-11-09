# 1. Создайте таблицу movies с полями movies_type, director, year_of_issue, length_in_minutes, rate.
CREATE TABLE IF NOT EXISTS spark.etl_hw4_movies (
	movies_type INT NULL DEFAULT NULL,
	director TIMESTAMP NULL DEFAULT NULL,
	year_of_issue INT NULL DEFAULT NULL,
	length_in_minutes INT NULL DEFAULT NULL,
	rate  INT NULL DEFAULT NULL,
	`Назначение` INT NULL DEFAULT NULL
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB;

# 2. Сделайте таблицы для горизонтального партицирования по году выпуска (до 1990, 1990 -2000, 2000- 2010, 2010-2020, после 2020).
CREATE TABLE IF NOT EXISTS spark.etl_hw4_movies_before_1990(
  CHECK(year_of_issue<1990)
  ) INHERITS (spark.etl_hw4_movies);