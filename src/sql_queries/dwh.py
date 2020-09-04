# DROP TABLES

dim_product_table_drop = "DROP TABLE IF EXISTS dim_product"
product_location_drop = "DROP TABLE IF EXISTS product_location"
dim_customer_profile_table_drop = "DROP TABLE IF EXISTS dim_customer_profile"
dim_order_date_table_drop = "DROP TABLE IF EXISTS dim_order_date"
fact_order_table_drop = "DROP TABLE IF EXISTS fact_order"

# CREATE TABLES

dim_product_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_product (
    id int PRIMARY KEY,
    name varchar NOT NULL,
    available_quantity int NOT NULL
);
""")

product_location_table_create = ("""
CREATE TABLE IF NOT EXISTS product_location (
    id serial PRIMARY KEY,
    product_id int NOT NULL,
    location_id int NOT NULL,
    date timestamptz NOT NULL
); 
""")

dim_customer_profile_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_customer_profile (
    id int PRIMARY KEY,
    customer_id varchar NOT NULL,
    name varchar,
    address varchar,
    city varchar,
    date timestamptz NOT NULL 
);
""")

dim_order_date_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_order_date (
    order_id bigint PRIMARY KEY,
    order_date timestamptz NOT NULL,
    hour int NOT NULL,
    day int NOT NULL,
    weekday int NOT NULL,
    month int NOT NULL,
    week int NOT NULL,
    year int NOT NULL
);
CREATE INDEX order_date_ind ON dim_order_date ((order_date::timestamptz));
CREATE INDEX order_id_date_ind ON dim_order_date USING btree (order_id, ((order_date::timestamptz)) DESC);
CREATE INDEX day_ind ON dim_order_date USING btree (day);
CREATE INDEX month_ind ON dim_order_date USING btree (month);
CREATE INDEX year_ind ON dim_order_date USING btree (year);
""")

fact_order_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_order (
    order_id bigint NOT NULL,
    product_id int NOT NULL,
    customer_id varchar NOT NULL,
    customer_profile_id int,
    city varchar,
    quantity int NOT NULL,
    unit_price float4 NOT NULL,
    total_price float4 NOT NULL,
    date timestamptz NOT NULL,
    CONSTRAINT fact_order_pk PRIMARY KEY (order_id, product_id, customer_id)
);
""")

# INSERT RECORDS

dim_product_table_insert = ("""
INSERT INTO dim_product (
    id,
    name,
    available_quantity) VALUES (%s, %s, %s)
""")

product_location_table_insert = ("""
INSERT INTO product_location (
    product_id,
    location_id,
    date ) VALUES (%s, %s, %s)
""")

dim_customer_profile_table_insert = ("""
INSERT INTO  dim_customer_profile (
    id,
    customer_id,
    name,
    address,
    city,
    date) VALUES (%s, %s, %s, %s, %s, %s);
""")

dim_order_date_table_insert = ("""
INSERT INTO dim_order_date (
    order_id,
    order_date,
    hour,
    day,
    weekday,
    month,
    week,
    year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

fact_order_table_insert = ("""
INSERT INTO fact_order (
    order_id,
    product_id,
    customer_id,
    customer_profile_id,
    city,
    quantity,
    unit_price,
    total_price,
    date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
""")


# QUERY LISTS

dim_fact_create_table_queries = [
    dim_product_table_create,
    product_location_table_create,
    dim_customer_profile_table_create,
    dim_order_date_table_create,
    fact_order_table_create
]

dim_fact_drop_table_queries = [
    dim_product_table_drop,
    product_location_drop,
    dim_customer_profile_table_drop,
    dim_order_date_table_drop,
    fact_order_table_drop
]
