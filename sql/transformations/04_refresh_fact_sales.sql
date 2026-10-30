-- Delete-and-insert for affected orders only. Re-running the same batch produces the
-- same rows (idempotent), and status changes (e.g. pending -> completed) flow through.

DELETE FROM analytics.fact_sales f
USING tmp_affected_orders a
WHERE f.order_id = a.order_id;

INSERT INTO analytics.fact_sales (
    order_item_id, order_id, order_date, customer_key, product_key, order_status,
    is_revenue, quantity, unit_price, gross_amount, discount_amount, net_amount, batch_id
)
SELECT i.order_item_id,
       i.order_id,
       (o.order_date AT TIME ZONE 'UTC')::date,
       dc.customer_key,
       dp.product_key,
       o.order_status,
       o.order_status IN ('completed', 'shipped'),
       i.quantity,
       i.unit_price,
       i.gross_amount,
       i.discount_amount,
       i.net_amount,
       :batch_id
FROM core.order_items i
JOIN tmp_affected_orders a ON a.order_id = i.order_id
JOIN core.orders o ON o.order_id = i.order_id
JOIN analytics.dim_customer dc ON dc.customer_id = o.customer_id
JOIN analytics.dim_product  dp ON dp.product_id  = i.product_id;
