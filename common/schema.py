from pyspark.sql.types import *

synthetic_data_schema = StructType([
    StructField("key", StringType(), True),
    StructField("ts", LongType(), True),
    StructField("level", StringType(), True),
    StructField("severity", IntegerType(), True),
    StructField("double_field", DoubleType(), True),
    StructField("float_field", FloatType(), True),
    StructField("int_field", IntegerType(), True),
    StructField("long_field", LongType(), True),
    StructField("boolean_field", BooleanType(), True),
    StructField("string_field", StringType(), True),
    StructField("bytes_field", BinaryType(), True),
    StructField("decimal_field", DecimalType(20, 2), True),
    StructField("nested_record",
                StructType([
                    StructField("nested_int", IntegerType(), True),
                    StructField("level", StringType(), True)]),
                True),
    StructField("map_field", MapType(StringType(), StringType()), True),
    StructField("array_field", ArrayType(StringType()), True),
    StructField("date_now", DateType(), True),
    StructField("timestamp_now", TimestampType(), True),
    StructField("string_field_for_partition", StringType(), True),
    StructField("date_field_for_partition", DateType(), True),
])
