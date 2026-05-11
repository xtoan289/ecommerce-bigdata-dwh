# Xây dựng Data Warehouse cho nền tảng thương mại điện tử Olist(Brazil) với Hệ sinh thái Big Data.

## 1. Mô tả dự án
Dự án này tập trung vào việc xây dựng một hệ thống lưu trữ và xử lý dữ liệu (Data Warehouse) theo kiến trúc Medallion (Bronze - Silver - Gold). Mục tiêu là chuyển đổi dữ liệu thô từ các tệp CSV của Olist thành các bảng dữ liệu tinh gọn, tối ưu cho việc phân tích doanh thu, hành vi khách hàng và hiệu quả giao hàng.

## 2. Kiến trúc dữ liệu (Data Architecture)
* Quy trình xử lý dữ liệu qua 3 lớp:
    - Lớp Bronze (Raw Data): Lưu trữ dữ liệu thô dưới định dạng Parquet để tối ưu tốc độ đọc/ghi. Dữ liệu ở đây giữ nguyên 100% so với gốc.
    - Lớp Silver (Cleaned Data): Dữ liệu đã được làm sạch, xử lý giá trị thiếu (Null), loại bỏ trùng lặp và chuẩn hóa kiểu dữ liệu (ngày tháng, số thực).
    - Lớp Gold (Business Level): Dữ liệu được tổ chức theo mô hình Star Schema (Sơ đồ sao) với các bảng Fact và Dimension, sẵn sàng cho việc báo cáo (Dashboard).


## 3. Công nghệ sử dụng (Tech Stack)

- Ngôn ngữ chính: Python & PySpark.
- Xử lý dữ liệu lớn: Apache Spark 3.5.x.
- Lưu trữ: Apache Hadoop 3.3.6 (Winutils).
- Quản lý cấu hình: YAML.
- Định dạng lưu trữ: Apache Parquet.

## 4. Cây thư mục tạm thời của dự án
```
├── data/               # Dữ liệu qua 4 giai đoạn xử lý
│   ├── raw/            # Dữ liệu gốc (CSV)
│   ├── bronze/         # Dữ liệu thô (Parquet)
│   ├── silver/         # Dữ liệu sạch, chuẩn hóa kiểu
│   └── gold/           # Dữ liệu tổng hợp (Fact/Dim)
├── src/                # Mã nguồn xử lý chính
│   ├── ingestion/      # Nạp dữ liệu (CSV -> Bronze)
│   │   └── raw_to_bronze.py
│   ├── transformation/ # Quy trình biến đổi dữ liệu (ETL)
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   └── utils/          # Hàm hỗ trợ và cấu hình Spark
│       ├── spark_session.py
│       └── helpers.py
├── config.yaml         # Cấu hình hệ thống (Đường dẫn, App Name)
├── .gitignore          # Loại bỏ các file nặng khỏi Git
└── README.md           # Tài liệu hướng dẫn dự án
```


## câu lệnh chạy để kiểm tra và xử lý

-   chạy ingestion: 
    python src/ingestion/raw_to_bronze.py
    
-   chạy bronze to silver: 
    python src/transformation/bronze_to_silver.py

-   chạy silver to gold: 
    python src/transformation/silver_to_gold.py
