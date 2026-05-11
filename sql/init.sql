-- Source table that Debezium will capture
CREATE TABLE IF NOT EXISTS orders (
    id         BIGSERIAL PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    price      NUMERIC(10,2),
    order_date   DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Enable logical replication publication for Debezium pgoutput plugin
SELECT pg_create_logical_replication_slot('debezium_orders', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_orders'
);

-- Seed data
INSERT INTO orders (name, price) VALUES
    ('Widget A', 9.99),
    ('Widget B', 19.99),
    ('Widget C', 4.50);
