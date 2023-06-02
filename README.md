# airflow_practice
Пример реализации потока данных с помощью оркестратора Airflow с использованием операторов Python, Bash, Spark и Hive с перемещением данных на кластер Hadoop.

Данный проект создан написан по итогам курсам <code>[Разработка и оркестрация ETL процессов в Airflow](https://fpmi-edu.ru/airflow)</code> от МФТИ

Цель потока - собрать данные по заболеваниям Covid-19 в режиме Daily, с обработкой и сохранением на Hadoop кластере.

Dag выполняет следующие операции:
1. Проверяет доступность API и данных
2. Загружает данные в локальную файловую систему Airflow
3. Перемещает эти данные в HDFS и удаляет из локальной файловой системы
4. Обрабатывает данные с помощью Spark, который агрегирует данные на уровне стран и считает статистику
5. Создаёт HIVE таблицу поверх обработанных данных. Если уже создана, добавляет следующую партицию