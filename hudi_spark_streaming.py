from functools import reduce

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Hudi Spark Streaming Basics") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

quickstart_utils = sc._jvm.org.apache.hudi.QuickstartUtils
data_gen = quickstart_utils.DataGenerator()

inserts = quickstart_utils.convertToStringList(data_gen.generateInserts(10))

table_name = "hudi_trips_cow"
base_path = "path/to/output/data/streaming_data"
streaming_table_name = "hudi_trips_cow_streaming"
base_streaming_path = f"{base_path}/hudi_trips_cow_streaming"
checkpoint_location = f"{base_path}/checkpoints/hudi_trips_cow_streaming"


def create_df():
    df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    return df


def create_table():
    df = create_df()

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'partitionpath',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    df.write \
        .format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(base_path)


def query_data():
    spark.readStream \
        .format("hudi") \
        .load(base_path) \
        .writeStream \
        .format("console") \
        .trigger(once=True) \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


def streaming_write():
    hudi_streaming_options = {
        'hoodie.table.name': streaming_table_name,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'partitionpath',
        'hoodie.datasource.write.table.name': streaming_table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    df = spark.readStream \
        .format("hudi") \
        .load(base_path)

    df.writeStream \
        .format("hudi") \
        .options(**hudi_streaming_options) \
        .outputMode("append") \
        .option("path", base_streaming_path) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(once=True) \
        .start() \
        .awaitTermination()


def point_in_time_query():
    begin_time = "000"
    ordered_rows: list[Row] = spark.read \
        .format("hudi") \
        .load(base_path) \
        .select(col("_hoodie_commit_time").alias("commit_time")) \
        .orderBy(col("commit_time")) \
        .collect()

    commits: list[Any] = list(map(lambda row: row[0], ordered_rows))
    print(commits)
    end_time = commits[0]

    point_in_time_read_options = {
        "hoodie.datasource.query.type": "incremental",
        "hoodie.datasource.read.end.instanttime": end_time,
        "hoodie.datasource.read.begin.instanttime": begin_time
    }

    trips_point_in_time_df = spark.read \
        .format("hudi") \
        .options(**point_in_time_read_options) \
        .load(base_path)

    trips_point_in_time_df.show(truncate=False)


def soft_delete():
    original_df = spark.read \
        .format("hudi") \
        .load(base_path)

    print(original_df.count())
    print(original_df
          .where(col("rider") != None)
          .count())

    soft_delete_df = original_df.limit(2)

    meta_columns = ["_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
                    "_hoodie_partition_path", "_hoodie_file_name"]
    excluded_columns = meta_columns + ["ts", "uuid", "partitionpath"]

    # Prepare the list of columns to nullify
    columns_to_nullify = [column for column in soft_delete_df.columns if column not in excluded_columns]

    # Nullify the columns in the DataFrame
    # if you don't want to preserve the dataType, you could skip the cast part
    nullified_df = reduce(lambda df, col: df.withColumn(col, lit(None).cast(soft_delete_df.schema[col].dataType)),
                          columns_to_nullify, soft_delete_df)

    # Display the resulting DataFrame with nullified columns
    nullified_df.show()

    hudi_soft_delete_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'partitionpath',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    nullified_df.write \
        .format("hudi") \
        .options(**hudi_soft_delete_options) \
        .mode("append") \
        .save(base_path)

    hudi_trips_snapshot_df = spark.read \
        .format("hudi") \
        .load(base_path)

    # This should return the same total count as before
    print(hudi_trips_snapshot_df.count())

    # This should return (total - 2) count as two records are updated with nulls
    print(hudi_trips_snapshot_df
          .where(col("rider").isNotNull())
          .count())


def hard_delete():
    hudi_trips_snapshot_df = spark.read \
        .format("hudi") \
        .load(base_path) \
        .select(col("uuid"), col("partitionpath"))

    hudi_trips_snapshot_df.show()

    tbd_df = hudi_trips_snapshot_df.limit(2)
    tbd_df.show()

    # print(hard_delete_df.collect())  # shows the df in list[Row]
    # deletes = list(map(lambda row: (row[0], row[1]), tbd_df.collect()))
    # hard_delete_df_1 = spark.sparkContext.parallelize(deletes).toDF(['uuid', 'partitionpath']).withColumn('ts',
    #                                                                                                       lit(0.0))
    # hard_delete_df_1.show()

    # instead of going through the DF-> RDD -> DF, simply add a column in your existing dataframe
    hard_delete_df_2 = tbd_df.withColumn("ts", lit(0.0))
    hard_delete_df_2.show()

    # issue deletes
    hudi_hard_delete_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'partitionpath',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'delete',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    hard_delete_df_2.write \
        .format("hudi") \
        .options(**hudi_hard_delete_options) \
        .mode("append") \
        .save(base_path)

    after_deletes_hudi_trips_snapshot_df = spark.read \
        .format("hudi") \
        .load(base_path) \
        .select(col("uuid"), col("partitionpath"))

    after_deletes_hudi_trips_snapshot_df.show()


if __name__ == '__main__':
    # create_table()
    query_data()
    # streaming_write()
    # point_in_time_query()
    # soft_delete()
    # hard_delete()
