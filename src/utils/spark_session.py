from pyspark.sql import SparkSession

def get_spark_session(app_name="Olist_Project"):
    return SparkSession.builder\
    .appName(app_name) \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.warehouse.dir", "E:/work/Python/DE_project/ecommerce-bigdata-dwh/spark-warehouse") \
    .getOrCreate()


