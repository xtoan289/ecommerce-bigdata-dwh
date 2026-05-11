from src.utils.spark_session import get_spark_session
from pyspark.sql.functions import col, sum as _sum

def process_silver_to_gold():
    spark = get_spark_session()
    print("Bắt đầu tổng hợp dữ liệu từ lớp Silver sang lớp Gold")

    try:
        # 1. Đọc các bảng từ lớp Silver
        print("Đang đọc dữ liệu từ lớp Silver...")
        df_orders = spark.read.parquet("data/silver/olist_orders_dataset")
        df_items = spark.read.parquet("data/silver/olist_order_items_dataset")
        df_payments = spark.read.parquet("data/silver/olist_order_payments_dataset")

        # 2. Tạo bảng Fact Sales (Kết hợp Orders và Items)
        print("Đang tạo bảng Fact Sales (Join Orders và Items)...")
        # Join để lấy thông tin ngày mua và giá tiền sản phẩm
        fact_sales = df_orders.join(df_items, "order_id", "inner") \
            .select(
                "order_id", 
                "customer_id", 
                "product_id", 
                "seller_id",
                "order_purchase_timestamp",
                "price",
                "freight_value"
            )

        # 3. Ghi dữ liệu bảng Fact ra lớp Gold
        print("Đang lưu bảng Fact Sales xuống lớp Gold...")
        fact_sales.write.mode("overwrite").parquet("data/gold/fact_sales")
        print("Đã hoàn thành bảng Fact Sales.")

        # 4. Tạo bảng Dimension Products (Kết hợp Products và Translation)
        print("Đang tạo bảng Dimension Products...")
        df_products = spark.read.parquet("data/silver/olist_products_dataset")
        df_trans = spark.read.parquet("data/silver/product_category_name_translation")

        dim_products = df_products.join(df_trans, "product_category_name", "left") \
            .select(
                "product_id",
                "product_category_name_english",
                "product_weight_g",
                "product_length_cm"
            )
        
        dim_products.write.mode("overwrite").parquet("data/gold/dim_products")
        print("Đã hoàn thành bảng Dimension Products.")

    except Exception as e:
        print(f"Lỗi: {e}")

    print("Đã hoàn thành: Dữ liệu đã sẵn sàng để làm Dashboard!")
    spark.stop()

if __name__ == "__main__":
    process_silver_to_gold()