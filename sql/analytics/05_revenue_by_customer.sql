-- 5. Revenue by customer.
SELECT c.customer_id,
       c.full_name,
       c.country_code,
       count(DISTINCT f.order_id) AS orders,
       sum(f.net_amount)          AS net_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_customer c USING (customer_key)
WHERE f.is_revenue
GROUP BY c.customer_id, c.full_name, c.country_code
ORDER BY net_revenue DESC;
