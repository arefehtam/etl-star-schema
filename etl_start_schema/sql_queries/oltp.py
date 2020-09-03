# DROP TABLES

product_table_drop = "DROP TABLE IF EXISTS product"
customer_table_drop = "DROP TABLE IF EXISTS customer"
order_info_table_drop = "DROP TABLE IF EXISTS order_info"
order_line_table_drop = "DROP TABLE IF EXISTS order_line"

# CREATE TABLES

product_table_create = ("""
CREATE TABLE IF NOT EXISTS product (
    id int NOT NULL,
    name varchar NOT NULL,
    available_quantity int NOT NULL,
    location_id int NOT NULL,
    date timestamptz NOT NULL
);
""")

customer_table_create = ("""
CREATE TABLE IF NOT EXISTS customer (
    uniq_id  int SERIAL, 
    id varchar NOT NULL,
    name varchar NOT NULL,
    address varchar,
    city varchar NOT NULL,
    date timestamptz NOT NULL
);
""")

order_info_table_create = ("""
CREATE TABLE IF NOT EXISTS order_info (
    id bigint PRIMARY KEY,
    customer_id varchar NOT NULL,
    order_date timestamptz NOT NULL 
);
""")

order_line_table_create = ("""
CREATE TABLE IF NOT EXISTS order_line (
    id       bigint NOT NULL,
    order_id bigint NOT NULL,
    product_id int NOT NULL,
    quantity   int NOT NULL,
    unit_price float4 NOT NULL
);
""")

# INSERT RECORDS

product_table_insert = ("""
INSERT INTO product (
    id,
    name,
    available_quantity,
    location_id,
    date) VALUES (%s, %s, %s, %s, %s)
""")

customer_table_insert = ("""
INSERT INTO customer (
    id,
    name,
    address,
    city,
    date) VALUES (%s, %s, %s, %s, %s)
""")

order_info_table_insert = ("""
INSERT INTO order_info (
    id,
    customer_id,
    order_date) VALUES (%s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
""")

order_line_table_insert = ("""
INSERT INTO order_line (
    id,
    order_id,
    product_id,
    quantity,
    unit_price) VALUES (%s, %s, %s, %s, %s)
""")

# TRANSFER DATA

product_table_transfer = ("""
SELECT id, name, available_quantity FROM (
    SELECT id, name, available_quantity, count(*) FROM product GROUP BY id, name, available_quantity
) p
""")

product_location_transfer = ("""
SELECT id, location_id FROM product
""")

customer_table_transfer = ("""
SELECT id, name, address, city, date FROM customer 
""")

order_table_transfer = ("""
SELECT o.id , o.order_id , o.product_id, oi.customer_id, uniq_id, oi.city, oi."date" as order_date FROM order_line o 
    INNER JOIN (
        SELECT id, customer_id, date, (
            WITH temp as (
                SELECT city, MIN(EXTRACT(EPOCH FROM (DATE_TRUNC('hour', oi."date" ::timestamp) - DATE_TRUNC('hour', date::timestamp)))) AS distance
                FROM customer
                WHERE  (id = oi.customer_id AND (EXTRACT(EPOCH FROM (DATE_TRUNC('hour', oi."date" ::timestamp) - DATE_TRUNC('hour', date::timestamp)))) >= 0)
                group by city
        )
        SELECT uniq_id, city FROM temp  WHERE distance  = (SELECT MIN(distance) FROM temp)
    ) FROM order_info oi
)  oi on o.order_id = oi.id 
""")

# QUERY LISTS

create_table_queries = [product_table_create, customer_table_create, order_info_table_create, order_line_table_create]

drop_table_queries = [product_table_drop, customer_table_drop, order_info_table_drop, order_line_table_drop]

