-- 1. Total recognised revenue (orders in status completed/shipped).
SELECT count(DISTINCT order_id)  AS revenue_orders,
       sum(quantity)             AS units_sold,
       sum(gross_amount)         AS gross_revenue,
       sum(discount_amount)      AS total_discounts,
       sum(net_amount)           AS net_revenue
FROM analytics.fact_sales
WHERE is_revenue;
