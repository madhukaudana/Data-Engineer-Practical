USE delivery;

CREATE TABLE customers (
customer_id BIGINT,
customer_name VARCHAR(255),
PRIMARY KEY (customer_id)
);


CREATE TABLE orders (
order_id BIGINT,
customer_id BIGINT,
total_amount DECIMAL(10,2),
order_date DATETIME,
PRIMARY KEY (order_id),
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

select * from customers;

SELECT customer_id
FROM orders
WHERE customer_id IS NULL;


select * from orders
where total_amount<=0;


select count(*) from customers;

select count(*) from orders;

CREATE TABLE customer_data (
    id BIGINT,
    total_orders INT,
    total_revenue FLOAT,
    repeat_customer TINYINT,
    customer_id BIGINT,
    PRIMARY KEY (id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)	
);


select * from customer_data;

