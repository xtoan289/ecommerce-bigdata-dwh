# Data Dictionary - E-commerce Data Warehouse

## 1. Table: fact_sales
Bảng sự kiện lưu trữ thông tin chi tiết từng dòng sản phẩm trong đơn hàng.

| Cột | Kiểu dữ liệu | Giải thích |
| :--- | :--- | :--- |
| order_id | VARCHAR | Mã định danh duy nhất của đơn hàng |
| product_id | VARCHAR | Mã định danh sản phẩm |
| date_key | INT | Khóa ngày (định dạng YYYYMMDD) để nối với dim_date |
| price | FLOAT | Giá của sản phẩm |
| delivery_days | INT | Số ngày từ lúc đặt hàng đến lúc nhận hàng thực tế |

## 2. Table: dim_products
Bảng chiều thông tin sản phẩm.

| Cột | Kiểu dữ liệu | Giải thích |
| :--- | :--- | :--- |
| product_id | VARCHAR | Khóa chính sản phẩm |
| category_name | VARCHAR | Tên ngành hàng (đã dịch sang tiếng Anh) |
| product_weight_g| FLOAT | Trọng lượng sản phẩm (gam) |

## 3. Table: dim_date
Bảng chiều thời gian giúp phân tích đa chiều.

| Cột | Kiểu dữ liệu | Giải thích |
| :--- | :--- | :--- |
| date_key | INT | Khóa chính (Ví dụ: 20260512) |
| full_date | DATE | Định dạng ngày đầy đủ |
| quarter | INT | Quý trong năm (1-4) |
| day_of_week | VARCHAR | Tên thứ trong tuần (Monday, Tuesday...) |