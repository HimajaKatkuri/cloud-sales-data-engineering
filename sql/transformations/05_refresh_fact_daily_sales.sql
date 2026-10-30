-- Recompute only the days touched by this batch.

DELETE FROM analytics.fact_daily_sales d
USING tmp_affected_dates a
WHERE d.sales_date = a.sales_date;

INSERT INTO analytics.fact_daily_sales (
    sales_date, order_count, units_sold, unique_customers,
    gross_revenue, discount_amount, net_revenue, average_order_value, updated_at
)
SELECT f.order_date,
       count(DISTINCT f.order_id),
       sum(f.quantity),
       count(DISTINCT f.customer_key),
       sum(f.gross_amount),
       sum(f.discount_amount),
       sum(f.net_amount),
       round(sum(f.net_amount) / count(DISTINCT f.order_id), 2),
       now()
FROM analytics.fact_sales f
JOIN tmp_affected_dates a ON a.sales_date = f.order_date
WHERE f.is_revenue
GROUP BY f.order_date;
