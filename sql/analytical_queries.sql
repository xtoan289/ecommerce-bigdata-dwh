-- 1. Top 10 ngành hàng có doanh thu cao nhất
SELECT 
    p.category_name, 
    ROUND(SUM(f.price)::numeric, 2) as total_revenue
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- 2. Xu hướng doanh thu theo từng tháng và quý
SELECT 
    d.year, 
    d.quarter, 
    d.month, 
    SUM(f.price) as monthly_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY 1, 2, 3
ORDER BY 1, 3;

-- 3. Thời gian giao hàng trung bình theo từng tháng
-- Giúp đánh giá hiệu quả logictics sau khi làm sạch dữ liệu
SELECT 
    d.year, 
    d.month, 
    ROUND(AVG(f.delivery_days), 2) as avg_delivery_time
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.delivery_days > 0
GROUP BY 1, 2
ORDER BY 1, 2;