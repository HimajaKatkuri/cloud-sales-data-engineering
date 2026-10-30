-- Incremental refresh, step 0: work out what this batch touched.
-- The loader stamps every inserted/updated core row with the current batch_id, so
-- "rows with batch_id = :batch_id" is exactly the set of changes. Everything downstream
-- recomputes only these keys instead of rebuilding the warehouse.
-- Temp tables are dropped automatically at COMMIT/ROLLBACK.

CREATE TEMP TABLE tmp_affected_orders ON COMMIT DROP AS
SELECT order_id FROM core.orders      WHERE batch_id = :batch_id
UNION
SELECT order_id FROM core.order_items WHERE batch_id = :batch_id;

CREATE TEMP TABLE tmp_affected_customers ON COMMIT DROP AS
SELECT customer_id FROM core.customers WHERE batch_id = :batch_id
UNION
SELECT o.customer_id
FROM core.orders o
JOIN tmp_affected_orders a USING (order_id)
UNION
-- customers previously linked to an affected order (in case the order was re-assigned)
SELECT d.customer_id
FROM analytics.fact_sales f
JOIN tmp_affected_orders a USING (order_id)
JOIN analytics.dim_customer d ON d.customer_key = f.customer_key;

CREATE TEMP TABLE tmp_affected_products ON COMMIT DROP AS
SELECT product_id FROM core.products WHERE batch_id = :batch_id
UNION
SELECT i.product_id
FROM core.order_items i
JOIN tmp_affected_orders a USING (order_id);

-- Days whose daily aggregate must be recomputed: the old date of each affected order
-- (from the fact table, before it is rewritten) and its current date.
CREATE TEMP TABLE tmp_affected_dates ON COMMIT DROP AS
SELECT DISTINCT f.order_date AS sales_date
FROM analytics.fact_sales f
JOIN tmp_affected_orders a USING (order_id)
UNION
SELECT DISTINCT (o.order_date AT TIME ZONE 'UTC')::date
FROM core.orders o
JOIN tmp_affected_orders a USING (order_id);
