-- Star schema for reporting.
-- Dimensions use surrogate keys (identity columns) so facts stay stable even if a natural
-- key's format changes upstream. Dimensions are SCD Type 1 (latest attributes win);
-- SCD Type 2 history is listed under "Future improvements" in the README.

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id        VARCHAR(10)    NOT NULL UNIQUE,
    full_name          VARCHAR(201)   NOT NULL,
    email              VARCHAR(255)   NOT NULL,
    country_code       CHAR(2)        NOT NULL,
    city               VARCHAR(100),
    signup_date        DATE           NOT NULL,
    first_order_date   DATE,
    last_order_date    DATE,
    total_orders       INTEGER        NOT NULL DEFAULT 0,  -- recognised-revenue orders
    lifetime_revenue   NUMERIC(14, 2) NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dim_customer_country ON analytics.dim_customer (country_code);

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id         VARCHAR(10)    NOT NULL UNIQUE,
    product_name       VARCHAR(200)   NOT NULL,
    category           VARCHAR(100)   NOT NULL,
    list_price         NUMERIC(12, 2) NOT NULL,
    is_active          BOOLEAN        NOT NULL,
    updated_at         TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dim_product_category ON analytics.dim_product (category);

-- Grain: one row per order line.
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    order_item_id      VARCHAR(12)    PRIMARY KEY,
    order_id           VARCHAR(12)    NOT NULL,
    order_date         DATE           NOT NULL,  -- UTC calendar day
    customer_key       BIGINT         NOT NULL REFERENCES analytics.dim_customer (customer_key),
    product_key        BIGINT         NOT NULL REFERENCES analytics.dim_product (product_key),
    order_status       VARCHAR(20)    NOT NULL,
    is_revenue         BOOLEAN        NOT NULL,  -- status counts as recognised revenue
    quantity           INTEGER        NOT NULL,
    unit_price         NUMERIC(12, 2) NOT NULL,
    gross_amount       NUMERIC(14, 2) NOT NULL,
    discount_amount    NUMERIC(14, 2) NOT NULL,
    net_amount         NUMERIC(14, 2) NOT NULL,
    batch_id           VARCHAR(40)    NOT NULL,
    loaded_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_sales_date     ON analytics.fact_sales (order_date);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customer ON analytics.fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_product  ON analytics.fact_sales (product_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_order    ON analytics.fact_sales (order_id);

-- Grain: one row per UTC day with recognised revenue. Pre-aggregated for dashboards.
CREATE TABLE IF NOT EXISTS analytics.fact_daily_sales (
    sales_date           DATE           PRIMARY KEY,
    order_count          INTEGER        NOT NULL,
    units_sold           INTEGER        NOT NULL,
    unique_customers     INTEGER        NOT NULL,
    gross_revenue        NUMERIC(14, 2) NOT NULL,
    discount_amount      NUMERIC(14, 2) NOT NULL,
    net_revenue          NUMERIC(14, 2) NOT NULL,
    average_order_value  NUMERIC(14, 2) NOT NULL,
    updated_at           TIMESTAMPTZ    NOT NULL DEFAULT now()
);
