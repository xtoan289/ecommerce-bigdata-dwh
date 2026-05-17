from src.utils.spark_session import get_spark_session
from pyspark.sql.functions import (
    col, date_format, datediff, year, month, 
    dayofmonth, quarter, expr, sequence, to_date, explode
)
import sys

def load_to_postgres(df, table_name):
    """Hàm nạp dữ liệu vào PostgreSQL - Đã sửa lỗi Host để chạy trong Docker"""
    print(f"   -> Đang nạp dữ liệu vào bảng SQL: {table_name}")
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://ecommerce_postgres:5432/airflow") \
            .option("dbtable", table_name) \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "5000") \
            .mode("overwrite") \
            .save()
        print(f"   -> [Thành công] Bảng {table_name}")
    except Exception as e:
        print(f"   -> [Lỗi] Không thể nạp bảng {table_name}: {e}")

def process_silver_to_gold():
    # 1. Khởi tạo Spark Session
    spark = get_spark_session()
    print("=== Bắt đầu tổng hợp dữ liệu: Silver -> Gold (Star Schema) ===")

    try:
        # 2. Đọc dữ liệu Silver (Đảm bảo đường dẫn chính xác trong Docker)
        print("Đang đọc dữ liệu từ Silver Layer...")
        df_orders = spark.read.parquet("data/silver/olist_orders_dataset")
        df_items = spark.read.parquet("data/silver/olist_order_items_dataset")
        df_products = spark.read.parquet("data/silver/olist_products_dataset")
        df_trans = spark.read.parquet("data/silver/product_category_name_translation")

        # 3. Tạo bảng Dim_Date
        print("Đang tạo Dim_Date...")
        dim_date = spark.sql("""
            SELECT explode(sequence(to_date('2016-01-01'), to_date('2020-12-31'), interval 1 day)) as full_date
        """).select(
            date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("full_date"),
            year(col("full_date")).alias("year"),
            month(col("full_date")).alias("month"),
            quarter(col("full_date")).alias("quarter"),
            date_format(col("full_date"), "EEEE").alias("day_of_week")
        )
        load_to_postgres(dim_date, "dim_date")

        # 4. Tạo bảng Dim_Products (Join với bảng dịch tiếng Anh)
        print("Đang tạo Dim_Products...")
        dim_products = df_products.join(df_trans, "product_category_name", "left") \
            .select(
                "product_id",
                col("product_category_name_english").alias("category_name"),
                "product_weight_g"
            ).fillna("others", subset=["category_name"])
        load_to_postgres(dim_products, "dim_products")

        # 5. Tạo bảng Fact_Sales
        print("Đang tạo Fact_Sales...")
        # Lọc bỏ các bản ghi không có ngày giao hàng để tính delivery_days chính xác
        fact_sales = df_orders.alias("o").join(df_items.alias("i"), "order_id", "inner") \
            .withColumn("delivery_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
            .select(
                "order_id", 
                "customer_id", 
                "product_id", 
                "seller_id",
                date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("date_key"),
                "price", 
                "freight_value",
                col("delivery_days").cast("int")
            ).fillna(0, subset=["delivery_days"])
        
        load_to_postgres(fact_sales, "fact_sales")

        print("Toàn bộ dữ liệu đã được đẩy vào Postgres!")

    except Exception as e:
        print(f"Lỗi trong quá trình xử lý: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_silver_to_gold()