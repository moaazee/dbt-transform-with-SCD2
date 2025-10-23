USE DATABASE SALES_DB;
USE SCHEMA RAW_DATA;

CREATE OR REPLACE TABLE manufacturers (
    manufacturer_id NUMBER PRIMARY KEY,
    manufacturer_name VARCHAR NOT NULL,
    country VARCHAR,
    contact_email VARCHAR,
    phone VARCHAR,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE OR REPLACE TABLE products (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    manufacturer_id NUMBER NOT NULL,
    category VARCHAR NOT NULL,
    unit_cost FLOAT NOT NULL,
    unit_price FLOAT NOT NULL,
    sku VARCHAR UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (manufacturer_id) REFERENCES manufacturers(manufacturer_id)
);

CREATE OR REPLACE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip_code VARCHAR,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE OR REPLACE TABLE inventory (
    inventory_id NUMBER PRIMARY KEY,
    product_id NUMBER NOT NULL,
    quantity_on_hand NUMBER NOT NULL DEFAULT 0,
    low_stock_threshold NUMBER NOT NULL DEFAULT 10,
    last_restocked_date DATE,
    warehouse_location VARCHAR NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE OR REPLACE TABLE sales (
    sale_id NUMBER PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    sale_date DATE NOT NULL,
    total_amount FLOAT NOT NULL,
    status VARCHAR NOT NULL,
    payment_method VARCHAR NOT NULL,
    invoice_number VARCHAR UNIQUE NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE OR REPLACE TABLE sales_line_items (
    sale_item_id NUMBER PRIMARY KEY,
    sale_id NUMBER NOT NULL,
    product_id NUMBER NOT NULL,
    quantity NUMBER NOT NULL,
    unit_price FLOAT NOT NULL,
    line_total_amount FLOAT,
    FOREIGN KEY (sale_id) REFERENCES sales(sale_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
