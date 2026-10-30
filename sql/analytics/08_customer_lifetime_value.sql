-- 8. Customer lifetime value: revenue, tenure and order frequency per customer,
--    plus a distribution of customers into CLV bands.
SELECT customer_id,
       full_name,
       total_orders,
       lifetime_revenue,
       round(lifetime_revenue / nullif(total_orders, 0), 2)   AS avg_order_value,
       first_order_date,
       last_order_date,
       last_order_date - first_order_date                     AS customer_tenure_days,
       CASE
           WHEN lifetime_revenue >= 2000 THEN 'high'
           WHEN lifetime_revenue >= 500  THEN 'medium'
           WHEN lifetime_revenue > 0     THEN 'low'
           ELSE 'no revenue'
       END                                                    AS clv_band
FROM analytics.dim_customer
ORDER BY lifetime_revenue DESC, customer_id;
