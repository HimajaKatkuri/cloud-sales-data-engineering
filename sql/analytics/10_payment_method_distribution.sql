-- 10. Payment method distribution (captured payments only).
SELECT payment_method,
       count(*)                                           AS payments,
       sum(amount)                                        AS amount_captured,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_of_payments,
       round(100.0 * sum(amount) / sum(sum(amount)) OVER (), 1) AS pct_of_amount
FROM core.payments
WHERE payment_status = 'captured'
GROUP BY payment_method
ORDER BY amount_captured DESC;
