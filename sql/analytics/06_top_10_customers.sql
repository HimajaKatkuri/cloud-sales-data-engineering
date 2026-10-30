-- 6. Top 10 customers by lifetime revenue (ties share a rank).
SELECT rank() OVER (ORDER BY lifetime_revenue DESC) AS revenue_rank,
       customer_id,
       full_name,
       country_code,
       total_orders,
       lifetime_revenue
FROM analytics.dim_customer
WHERE total_orders > 0
ORDER BY lifetime_revenue DESC, customer_id
LIMIT 10;
