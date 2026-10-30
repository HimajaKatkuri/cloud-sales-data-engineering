-- Order totals are derived from ALL of an order's lines in the warehouse, not just the
-- lines in this batch, so late-arriving or corrected lines produce the right total.

UPDATE core.orders o
SET item_count      = coalesce(t.item_count, 0),
    gross_amount    = coalesce(t.gross_amount, 0),
    discount_amount = coalesce(t.discount_amount, 0),
    net_amount      = coalesce(t.net_amount, 0)
FROM tmp_affected_orders a
LEFT JOIN (
    SELECT i.order_id,
           count(*)               AS item_count,
           sum(i.gross_amount)    AS gross_amount,
           sum(i.discount_amount) AS discount_amount,
           sum(i.net_amount)      AS net_amount
    FROM core.order_items i
    JOIN tmp_affected_orders x USING (order_id)
    GROUP BY i.order_id
) t ON t.order_id = a.order_id
WHERE o.order_id = a.order_id;
