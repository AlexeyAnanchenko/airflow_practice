import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window


# делаем скрипт, который будет читать сырые данные, обрабатывать их, агрегировать

def main(spark, exec_date):
    read_path = f'/covid_data/csv/{exec_date}'
    save_path = f'/covid_data/results/exec_date={exec_date}'

    w = Window.partitionBy()

    df = (
        spark.read.option('header', True).csv(read_path)
        # получится файл сгруппированный по странам
        .groupBy(F.col('country_region'))
        # с общей суммой заражения в стране
        .agg(F.sum(F.col('confirmed').cast(T.IntegerType())).alias('total_confirmed'),
             # c общей суммой смертей в стране
             F.sum(F.col('deaths').cast(T.IntegerType())).alias('total_deaths'))
             # статистика, смертей после заражения
        .withColumn('fatality_ratio', F.col('total_deaths') / F.col('total_confirmed'))
        # статистика, процент заражений от всего мира
        .withColumn('world_case_pct', F.col('total_confirmed') / F.sum(F.col('total_confirmed')).over(w))
        # статистика, процент смертей от всего мира
        .withColumn('world_death_pct', F.col('total_deaths') / F.sum(F.col('total_deaths')).over(w))
    )

    # это значит перезаписывать один и тот же файл при перезапуске
    df.repartition(1).write.mode('overwrite').format('csv').save(save_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # позволит знать какой по дате файлик забирать, чтобы обрабатывать
    parser.add_argument('--exec_date', required=True, type=str, help='Execution data')
    args = parser.parse_args()

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    try:
        main(spark, args.exec_date)
    finally:
        spark.stop()