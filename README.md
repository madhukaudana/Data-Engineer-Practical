# Data-Engineer-Practical

## Introduction

This repository contains the code for the assignment of the Data Engineer position. This assignment have 3 parts:

1. Data Preparation: Imports customer and order data from CSV files into a MySQL database.
2. Streamlit Dashboard: Provides a web-based dashboard to analyze and visualize order data.
3. Machine Learning Model: Predicts repeat customers based on order count and revenue, using a Random Forest classifier.


## Requirements

- Python 3.9+
- MySQL 8.0
- Python Libraries:
    - pandas
    - sqlalchemy
    - streamlit
    - scikit-learn
    - pymysql
    - altair
    - matplotlib

* Install the required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```
  
## Instructions

# Step 1: MySQL database setup

1. Clone the repository:
    ```bash
    git clone https://github.com/madhukaudana/Data-Engineer-Practical.git
    ```
   
2. Create a MySQL database:
    ```sql
    CREATE DATABASE delevery;
    ```
   
3. Create tables in the database:
    ```sql
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
    ```
   
# Step 2: Data Preparation

Use the part 1 folder ETL_database.py script to import customer and order data from CSV files into the MySQL database.
Place customers.csv and orders.csv in the same folder as the script.

```bash
python part_1\data_import_preparation.py
```

Step 3: Streamlit Dashboard

The Streamlit dashboard provides data filtering, visualization, and summary metrics. 
Run the following command to start the dashboard:

```bash
streamlit run part_2\streamlit_app.py
```

Dashboard Components:

- Filters: Filter orders by date range, total spent amount, and order count.
- Summary Metrics: Displays total revenue, number of unique customers, and total orders.
- Charts:
      Bar chart showing total revenue by customer.
      Line chart showing revenue over time.


# Step 4: Machine Learning Model

The machine learning model predicts repeat customers based on order count and revenue.

Create table for the pre-processed data:

```sql
    USE delivery;
    
    CREATE TABLE customer_data (
    id BIGINT,
    total_orders INT,
    total_revenue FLOAT,
    repeat_customer TINYINT,
    customer_id BIGINT,
    PRIMARY KEY (id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)	
);
```

Run following command to data preparation for train the model:

```bash
python part_3\data_preparation.py
```

Run the following command to train the model and predict repeat customers:

```bash
python part_3\predict_repeat_customers.py
```