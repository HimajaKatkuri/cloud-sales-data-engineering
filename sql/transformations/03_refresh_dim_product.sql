-- SCD Type 1 upsert of affected products.

INSERT INTO analytics.dim_product AS d (
    product_id, product_name, category, list_price, is_active, updated_at
)
SELECT p.product_id, p.product_name, p.category, p.list_price, p.is_active, now()
FROM core.products p
JOIN tmp_affected_products a USING (product_id)
ON CONFLICT (product_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    category     = EXCLUDED.category,
    list_price   = EXCLUDED.list_price,
    is_active    = EXCLUDED.is_active,
    updated_at   = now();
