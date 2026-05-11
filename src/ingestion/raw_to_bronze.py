import os
import sys
import glob

# Lấy đường dẫn gốc của project (thư mục ecommerce-bigdata-dwh)
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Thêm base_dir vào sys.path để có thể import từ src
if base_dir not in sys.path:
    sys.path.append(base_dir)

from src.utils.spark_session import get_spark_session

def process_raw_to_bronze():
    """
    Quét toàn bộ các file .csv trong thư mục data/raw và chuyển sang 
    định dạng .parquet trong thư mục data/bronze.
    """
    # Khởi tạo Spark Session
    spark = get_spark_session("Raw_to_Bronze_Ingestion")

    raw_dir = os.path.join(base_dir, "data", "raw")
    bronze_dir = os.path.join(base_dir, "data", "bronze")

    # Kiểm tra xem thư mục bronze đã tồn tại chưa, nếu chưa thì tạo mới
    if not os.path.exists(bronze_dir):
        os.makedirs(bronze_dir)

    print(f"Bắt đầu quét thư mục raw: {raw_dir}")
    print(f"Thư mục đích bronze: {bronze_dir}\n")
    
    # Lấy danh sách tất cả các file .csv trong thư mục raw
    csv_files = glob.glob(os.path.join(raw_dir, "*.csv"))
    
    if not csv_files:
        print("Không tìm thấy file .csv nào trong thư mục raw.")
        return

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        table_name = os.path.splitext(file_name)[0]
        
        print(f"Đang xử lý file: {file_name}...")
        
        try:
            # Đọc file CSV bằng PySpark
            # Sử dụng header=True để lấy dòng đầu làm tên cột
            # inferSchema=True để tự động nhận dạng kiểu dữ liệu
            # escape='"' giúp xử lý đúng các trường có dấu phẩy bên trong dấu ngoặc kép
            df = spark.read.csv(
                file_path, 
                header=True, 
                inferSchema=True, 
                escape='"'
            )
            
            # Đường dẫn lưu trữ parquet cho mỗi bảng (lưu thành 1 thư mục riêng cho từng bảng)
            output_path = os.path.join(bronze_dir, table_name)
            
            # Ghi dữ liệu ra định dạng parquet ở lớp bronze
            # Chế độ "overwrite" để ghi đè dữ liệu nếu thư mục đã tồn tại
            df.write.mode("overwrite").parquet(output_path)
            
            print(f"  -> Đã lưu thành công bảng {table_name} sang Parquet.\n")
            
        except Exception as e:
            print(f"  -> [LỖI] Xảy ra lỗi khi xử lý file {file_name}: {e}\n")

    print("Hoàn tất quá trình chuyển đổi từ Raw sang Bronze!")

if __name__ == "__main__":
    process_raw_to_bronze()
