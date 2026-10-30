-- 7. Average order value overall and by country.
WITH orders AS (
    SELECT f.order_id, c.country_code, sum(f.net_amount) AS order_net
    FROM analytics.fact_sales f
    JOIN analytics.dim_customer c USING (customer_key)
    WHERE f.is_revenue
    GROUP BY f.order_id, c.country_code
)
SELECT coalesce(country_code, 'ALL') AS country_code,
       count(*)                      AS orders,
       round(avg(order_net), 2)      AS average_order_value
FROM orders
GROUP BY ROLLUP (country_code)
ORDER BY country_code NULLS FIRST;
