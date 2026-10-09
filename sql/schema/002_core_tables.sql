-- Core (cleaned) entities.
-- Every table carries lineage columns:
--   source_updated_at : the source system's updated_at (drives incremental loads and upserts)
--   batch_id          : the pipeline run that last wrote the row
--   loaded_at         : when that write happened

CREATE TABLE IF NOT EXISTS core.customers (
    customer_id        VARCHAR(10)   PRIMARY KEY,
    first_name         VARCHAR(100)  NOT NULL,
    last_name          VARCHAR(100)  NOT NULL,
    email              VARCHAR(255)  NOT NULL CHECK (position('@' IN email) > 1),
    country_code       CHAR(2)       NOT NULL,
    city               VARCHAR(100),
    signup_date        DATE          NOT NULL,
    source_updated_at  TIMESTAMPTZ   NOT NULL,
    batch_id           VARCHAR(40)   NOT NULL,
    loaded_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_customers_email ON core.customers (lower(email));
CREATE INDEX IF NOT EXISTS ix_customers_batch ON core.customers (batch_id);

CREATE TABLE IF NOT EXISTS core.products (
    product_id         VARCHAR(10)    PRIMARY KEY,
    product_name       VARCHAR(200)   NOT NULL,
    category           VARCHAR(100)   NOT NULL,
    list_price         NUMERIC(12, 2) NOT NULL CHECK (list_price >= 0),
    is_active          BOOLEAN        NOT NULL,
    source_updated_at  TIMESTAMPTZ    NOT NULL,
    batch_id           VARCHAR(40)    NOT NULL,
    loaded_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_products_batch ON core.products (batch_id);

CREATE TABLE IF NOT EXISTS core.orders (
    order_id           VARCHAR(12)    PRIMARY KEY,
    customer_id        VARCHAR(10)    NOT NULL REFERENCES core.customers (customer_id),
    order_date         TIMESTAMPTZ    NOT NULL,
    order_status       VARCHAR(20)    NOT NULL
        CHECK (order_status IN ('pending', 'completed', 'shipped', 'cancelled', 'refunded')),
    currency           CHAR(3)        NOT NULL,
    -- Totals are derived from order_items by sql/transformations/01_refresh_order_totals.sql
    item_count         INTEGER        NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    gross_amount       NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (gross_amount >= 0),
    discount_amount    NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    net_amount         NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (net_amount >= 0),
    source_updated_at  TIMESTAMPTZ    NOT NULL,
    batch_id           VARCHAR(40)    NOT NULL,
    loaded_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_orders_customer   ON core.orders (customer_id);
CREATE INDEX IF NOT EXISTS ix_orders_order_date ON core.orders (order_date);
CREATE INDEX IF NOT EXISTS ix_orders_batch      ON core.orders (batch_id);

CREATE TABLE IF NOT EXISTS core.order_items (
    order_item_id      VARCHAR(12)    PRIMARY KEY,
    order_id           VARCHAR(12)    NOT NULL REFERENCES core.orders (order_id),
    product_id         VARCHAR(10)    NOT NULL REFERENCES core.products (product_id),
    quantity           INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price         NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    discount_amount    NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    gross_amount       NUMERIC(14, 2) NOT NULL CHECK (gross_amount >= 0),
    net_amount         NUMERIC(14, 2) NOT NULL CHECK (net_amount >= 0),
    source_updated_at  TIMESTAMPTZ    NOT NULL,
    batch_id           VARCHAR(40)    NOT NULL,
    loaded_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT ck_order_items_discount CHECK (discount_amount <= gross_amount),
    CONSTRAINT ck_order_items_net CHECK (net_amount = gross_amount - discount_amount)
);
CREATE INDEX IF NOT EXISTS ix_order_items_order   ON core.order_items (order_id);
CREATE INDEX IF NOT EXISTS ix_order_items_product ON core.order_items (product_id);
CREATE INDEX IF NOT EXISTS ix_order_items_batch   ON core.order_items (batch_id);

CREATE TABLE IF NOT EXISTS core.payments (
    payment_id         VARCHAR(12)    PRIMARY KEY,
    order_id           VARCHAR(12)    NOT NULL REFERENCES core.orders (order_id),
    payment_date       TIMESTAMPTZ    NOT NULL,
    payment_method     VARCHAR(20)    NOT NULL
        CHECK (payment_method IN ('credit_card', 'debit_card', 'paypal', 'bank_transfer', 'gift_card')),
    amount             NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    payment_status     VARCHAR(20)    NOT NULL
        CHECK (payment_status IN ('captured', 'failed', 'refunded')),
    source_updated_at  TIMESTAMPTZ    NOT NULL,
    batch_id           VARCHAR(40)    NOT NULL,
    loaded_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_payments_order ON core.payments (order_id);
CREATE INDEX IF NOT EXISTS ix_payments_date  ON core.payments (payment_date);
