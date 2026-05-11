import os
from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark_session


def process_raw_to_bronze():
    
    spark = get_spark_session()

    tables = [
        "olist_order_customers_dataset",
        "olist_order_items_dataset",
        "olist_order_payments_dataset",
        "olist_order_reviews_dataset",
        "olist_orders_dataset",
        "olist_products_dataset",
        "olist_sellers_dataset",
        "olist_products_category_name_translation"
        
    ]
    
    print("Bắt đầu quá trình chuyển đổi từ Raw sang Bronze")

    for table in tables:
        input_path = f"data/raw/{table}.csv"
        output_path = f"data/bronze/{table}"
        print(f"Đang xử lý bảng {table}")
        try:
            df = spark.read.csv(input_path, header = True, inferSchema = True)

            df.write.mode("overwrite").parquet(output_path)
            print(f"Lưu thành công {table} vào Bronze Layer\n")
        except Exception as e:
            print(f"Lỗi {table}: {str(e)}")

    print("Đã hoàn tất quá trình Raw -> Bronze")
    spark.stop()
    
if __name__ == "__main__":
    process_raw_to_bronze()