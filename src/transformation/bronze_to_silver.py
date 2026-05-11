from src.utils.spark_session import get_spark_session
from pyspark.sql.functions import avg, col, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType

def process_bronze_to_silver():
    # 1. Khởi tạo Spark từ Utils
    spark = get_spark_session()
    print("Bắt đầu làm sạch dữ liệu và chuyển từ Bronze -> Silver")

    # --- 1. Xử lý bảng Đơn hàng (Orders) ---
    print("Đang xử lý: Bảng đơn hàng (orders)...")
    df_orders = spark.read.parquet("data/bronze/olist_orders_dataset")
    date_columns = ["order_purchase_timestamp", "order_approved_at", 
                    "order_delivered_carrier_date", "order_delivered_customer_date", 
                    "order_estimated_delivery_date"]
    for c in date_columns:
        df_orders = df_orders.withColumn(c, to_timestamp(col(c)))
    df_orders.dropDuplicates(["order_id"]).write.mode("overwrite").parquet("data/silver/olist_orders_dataset")
    print("Đã hoàn thành làm sạch bảng Orders.")

    # --- 2. Xử lý bảng Khách hàng (Customers) ---
    print("Đang xử lý: Bảng khách hàng (customers)...")
    spark.read.parquet("data/bronze/olist_customers_dataset") \
        .dropDuplicates(["customer_id"]).write.mode("overwrite").parquet("data/silver/olist_customers_dataset")
    print("Đã hoàn thành làm sạch bảng Customers.")

    # --- 3. Xử lý bảng Chi tiết đơn hàng (Order Items) ---
    print("Đang xử lý: Bảng chi tiết sản phẩm đơn hàng (order_items)...")
    spark.read.parquet("data/bronze/olist_order_items_dataset") \
        .withColumn("price", col("price").cast(DoubleType())) \
        .withColumn("freight_value", col("freight_value").cast(DoubleType())) \
        .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"))) \
        .dropDuplicates().write.mode("overwrite").parquet("data/silver/olist_order_items_dataset")
    print("Đã hoàn thành làm sạch bảng Order Items.")

    # --- 4. Xử lý bảng Thanh toán (Order Payments) ---
    print("Đang xử lý: Bảng thanh toán (order_payments)...")
    spark.read.parquet("data/bronze/olist_order_payments_dataset") \
        .withColumn("payment_value", col("payment_value").cast(DoubleType())) \
        .dropDuplicates().write.mode("overwrite").parquet("data/silver/olist_order_payments_dataset")
    print("Đã hoàn thành làm sạch bảng Order Payments.")

    # --- 5. Xử lý bảng Sản phẩm (Products) ---
    print("Đang xử lý: Bảng sản phẩm (products)...")
    spark.read.parquet("data/bronze/olist_products_dataset") \
        .dropDuplicates(["product_id"]).write.mode("overwrite").parquet("data/silver/olist_products_dataset")
    print("Đã hoàn thành làm sạch bảng Products.")

    # --- 6. Xử lý bảng Người bán (Sellers) ---
    print("Đang xử lý: Bảng người bán (sellers)...")
    spark.read.parquet("data/bronze/olist_sellers_dataset") \
        .dropDuplicates(["seller_id"]).write.mode("overwrite").parquet("data/silver/olist_sellers_dataset")
    print("Đã hoàn thành làm sạch bảng Sellers.")

    # --- 7. Xử lý bảng Đánh giá (Reviews) ---
    print("Đang xử lý: Bảng đánh giá (reviews)...")
    spark.read.parquet("data/bronze/olist_order_reviews_dataset") \
        .withColumn("review_score", col("review_score").cast(IntegerType())) \
        .withColumn("review_creation_date", to_timestamp(col("review_creation_date"))) \
        .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp"))) \
        .dropDuplicates(["review_id"]).write.mode("overwrite").parquet("data/silver/olist_order_reviews_dataset")
    print("Đã hoàn thành làm sạch bảng Reviews.")

    # --- 8. Xử lý bảng Dịch thuật ngành hàng (Category Translation) ---
    print("Đang xử lý: Bảng dịch thuật ngành hàng (category_translation)...")
    spark.read.parquet("data/bronze/product_category_name_translation") \
        .dropDuplicates(["product_category_name"]).write.mode("overwrite").parquet("data/silver/product_category_name_translation")
    print("Đã hoàn thành làm sạch bảng Category Translation.")

    # # --- 9. Bảng Địa lý (Geolocation) - Tạm thời comment để tối ưu tốc độ ---
    # print("Đang xử lý: Bảng tọa độ địa lý (geolocation)...")
    # df_geo = spark.read.parquet("data/bronze/olist_geolocation_dataset")
    # df_geo_clean = df_geo.groupBy("geolocation_zip_code_prefix") \
    #     .agg(avg("geolocation_lat").alias("latitude"), avg("geolocation_lng").alias("longitude"))
    # df_geo_clean.write.mode("overwrite").parquet("data/silver/olist_geolocation_dataset")
    # print("Đã hoàn thành làm sạch bảng Geolocation.")
    

    print("Dữ liệu đã được làm sạch và lưu tại Silver Layer.")
    spark.stop()

if __name__ == "__main__":
    process_bronze_to_silver()