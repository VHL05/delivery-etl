SELECT 
    EXTRACT(YEAR FROM order_purchase_timestamp) AS order_year,
    EXTRACT(MONTH FROM order_purchase_timestamp) AS order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(price + freight_value), 2) AS total_revenue,
    SUM(is_delayed) AS delayed_orders,
    ROUND((SUM(is_delayed) / COUNT(DISTINCT order_id)) * 100, 2) AS delay_rate_percent
FROM 
    `delivery-507606.delivery_dw.fact_delivery`
GROUP BY 
    order_year, order_month
ORDER BY 
    order_year DESC, order_month DESC;


SELECT 
    c.customer_state,
    COUNT(f.order_id) AS total_deliveries,
    SUM(f.is_delayed) AS delayed_deliveries,
    ROUND((SUM(f.is_delayed) / COUNT(f.order_id)) * 100, 2) AS delay_rate_percent,
    ROUND(AVG(f.delivery_duration_hours), 2) AS avg_delivery_hours
FROM 
    `delivery-507606.delivery_dw.fact_delivery` f
JOIN 
    `delivery-507606.delivery_dw.dim_customer` c
    ON f.customer_id = c.customer_id
GROUP BY 
    c.customer_state
HAVING 
    total_deliveries > 1000 -- Chỉ xét các bang có lượng đơn lớn để đảm bảo tính thống kê
ORDER BY 
    delay_rate_percent DESC
LIMIT 10;


SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(f.order_id) AS total_orders,
    ROUND(SUM(f.price), 2) AS total_revenue,
    ROUND(AVG(f.delivery_duration_hours), 2) AS avg_delivery_hours,
    SUM(f.is_delayed) AS delayed_orders
FROM 
    `delivery-507606.delivery_dw.fact_delivery` f
JOIN 
    `delivery-507606.delivery_dw.dim_seller` s
    ON f.seller_id = s.seller_id
GROUP BY 
    s.seller_id, s.seller_city, s.seller_state
ORDER BY 
    total_revenue DESC
LIMIT 10;



SELECT 
    p.product_category_name,
    COUNT(f.order_id) AS items_sold,
    ROUND(AVG(p.product_weight_g), 2) AS avg_weight_g,
    ROUND(AVG(p.product_volume), 2) AS avg_volume_cm3,
    ROUND(AVG(f.delivery_duration_hours), 2) AS avg_delivery_hours,
    ROUND((SUM(f.is_delayed) / COUNT(f.order_id)) * 100, 2) AS delay_rate_percent
FROM 
    `delivery-507606.delivery_dw.fact_delivery` f
JOIN 
    `delivery-507606.delivery_dw.dim_product` p
    ON f.product_id = p.product_id
WHERE 
    p.product_category_name IS NOT NULL
GROUP BY 
    p.product_category_name
HAVING 
    items_sold > 500
ORDER BY 
    avg_delivery_hours DESC
LIMIT 15;