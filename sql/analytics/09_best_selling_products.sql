-- 9. Best-selling products by units (revenue as tie-breaker).
SELECT p.product_id,
       p.product_name,
       p.category,
       sum(f.quantity)            AS units_sold,
       count(DISTINCT f.order_id) AS orders,
       sum(f.net_amount)          AS net_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_product p USING (product_key)
WHERE f.is_revenue
GROUP BY p.product_id, p.product_name, p.category
ORDER BY units_sold DESC, net_revenue DESC
LIMIT 10;
