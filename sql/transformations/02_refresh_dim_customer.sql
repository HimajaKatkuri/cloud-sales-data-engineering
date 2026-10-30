-- SCD Type 1 upsert of affected customers, with lifetime metrics recomputed from history.
-- Recognised revenue = orders in status completed or shipped.

INSERT INTO analytics.dim_customer AS d (
    customer_id, full_name, email, country_code, city, signup_date,
    first_order_date, last_order_date, total_orders, lifetime_revenue, updated_at
)
SELECT c.customer_id,
       c.first_name || ' ' || c.last_name,
       c.email,
       c.country_code,
       c.city,
       c.signup_date,
       min((o.order_date AT TIME ZONE 'UTC')::date) FILTER (WHERE o.item_count > 0),
       max((o.order_date AT TIME ZONE 'UTC')::date) FILTER (WHERE o.item_count > 0),
       -- orders whose lines were all quarantined have no revenue yet; don't count them
       count(o.order_id) FILTER (WHERE o.item_count > 0),
       coalesce(sum(o.net_amount), 0),
       now()
FROM core.customers c
JOIN tmp_affected_customers a USING (customer_id)
LEFT JOIN core.orders o
       ON o.customer_id = c.customer_id
      AND o.order_status IN ('completed', 'shipped')
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.country_code, c.city, c.signup_date
ON CONFLICT (customer_id) DO UPDATE
SET full_name        = EXCLUDED.full_name,
    email            = EXCLUDED.email,
    country_code     = EXCLUDED.country_code,
    city             = EXCLUDED.city,
    signup_date      = EXCLUDED.signup_date,
    first_order_date = EXCLUDED.first_order_date,
    last_order_date  = EXCLUDED.last_order_date,
    total_orders     = EXCLUDED.total_orders,
    lifetime_revenue = EXCLUDED.lifetime_revenue,
    updated_at       = now();
