import os
import yaml
import findspark
from pyspark.sql import SparkSession

def get_spark_session():
    # Đọc cấu hình từ file yaml
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    cfg = config['spark_config']

    # Chỉ thiết lập các biến môi trường và findspark khi chạy trên Windows (Local)
    if os.name == 'nt':
        hadoop_bin = os.path.join(cfg['hadoop_home'], "bin")
        os.environ["PATH"] += os.pathsep + hadoop_bin

        if hasattr(os, "add_dll_directory"):
            os.add_dll_directory(hadoop_bin)

        os.environ["JAVA_HOME"] = cfg['java_home']
        os.environ["HADOOP_HOME"] = cfg['hadoop_home']
        
        findspark.init(cfg['spark_home'])

    # --- ĐỊNH NGHĨA PHIÊN BẢN POSTGRES DRIVER ---
    # Spark sẽ tự động tải thư viện này từ internet về trong lần chạy đầu tiên
    postgres_jar = "org.postgresql:postgresql:42.7.1"

  # Xây dựng SparkSession nâng cao
    spark = SparkSession.builder \
        .appName('Olist_Ecommerce_DWH') \
        .config("spark.jars.packages", postgres_jar) \
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC") \
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.warehouse.dir", cfg['warehouse_dir']) \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .getOrCreate()

    # Cấu hình Log chỉ hiện lỗi để màn hình sạch sẽ
    spark.sparkContext.setLogLevel("ERROR")

    return spark