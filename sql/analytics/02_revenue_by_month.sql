-- 2. Revenue by month, with month-over-month change.
WITH monthly AS (
    SELECT date_trunc('month', sales_date)::date AS month,
           sum(order_count)                      AS orders,
           sum(net_revenue)                      AS net_revenue
    FROM analytics.fact_daily_sales
    GROUP BY 1
)
SELECT month,
       orders,
       net_revenue,
       round(100.0 * (net_revenue - lag(net_revenue) OVER (ORDER BY month))
             / nullif(lag(net_revenue) OVER (ORDER BY month), 0), 1) AS mom_change_pct
FROM monthly
ORDER BY month;
