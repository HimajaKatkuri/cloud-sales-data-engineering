-- 4. Revenue by product and category share.
SELECT p.product_id,
       p.product_name,
       p.category,
       sum(f.quantity)   AS units_sold,
       sum(f.net_amount) AS net_revenue,
       round(100.0 * sum(f.net_amount) / sum(sum(f.net_amount)) OVER (PARTITION BY p.category), 1)
           AS pct_of_category_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_product p USING (product_key)
WHERE f.is_revenue
GROUP BY p.product_id, p.product_name, p.category
ORDER BY net_revenue DESC;
