CREATE TABLE order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(id),
    product_id VARCHAR(100) NOT NULL,
    name VARCHAR(200),
    unit_price NUMERIC(19, 2) NOT NULL,
    quantity INTEGER NOT NULL
);

CREATE INDEX idx_order_items_order ON order_items(order_id);