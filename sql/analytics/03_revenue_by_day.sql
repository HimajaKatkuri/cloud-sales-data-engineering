-- 3. Revenue by day with a 7-day moving average (reads the pre-aggregated daily fact).
SELECT sales_date,
       order_count,
       net_revenue,
       round(avg(net_revenue) OVER (ORDER BY sales_date
                                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS net_revenue_7d_avg
FROM analytics.fact_daily_sales
ORDER BY sales_date;
