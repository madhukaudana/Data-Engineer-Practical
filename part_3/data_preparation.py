import pandas as pd
from sqlalchemy import create_engine, BigInteger, Float, Integer, String
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_USER = 'root'
DB_PASSWORD = 'Muw0okm_PL<'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'delivery'

DATABASE_URL = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DATABASE_URL)


def detect_and_remove_outliers_iqr(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    logger.info(f"Outlier bounds for '{column}': {lower_bound} to {upper_bound}")
    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]


def create_customer_data_table():
    with engine.connect() as connection:
        customers_df = pd.read_sql("SELECT customer_id, customer_name FROM customers", connection)
        orders_df = pd.read_sql("SELECT customer_id, total_amount FROM orders", connection)

    # Data Cleaning
    customers_df.drop_duplicates(inplace=True)
    orders_df.drop_duplicates(inplace=True)

    logger.info(f"Customers DataFrame shape: {customers_df.shape}")
    logger.info(f"Orders DataFrame shape: {orders_df.shape}")

    # Check for and remove outliers in the orders data
    logger.info("Removing outliers from 'total_amount'...")
    original_order_count = orders_df.shape[0]
    orders_df = detect_and_remove_outliers_iqr(orders_df, 'total_amount')
    removed_outliers_count = original_order_count - orders_df.shape[0]
    logger.info(f"Number of outliers removed from orders data: {removed_outliers_count}")
    logger.info(f"Orders DataFrame shape after outlier removal: {orders_df.shape}")

    # Aggregate orders data
    orders_aggregated = orders_df.groupby('customer_id').agg(
        total_orders=('total_amount', 'size'),
        total_revenue=('total_amount', 'sum')
    ).reset_index()

    # Merge aggregated orders with customers data
    customer_data_df = pd.merge(customers_df, orders_aggregated, on='customer_id', how='left')

    # Fill missing values for customers with zero orders
    customer_data_df['total_orders'] = customer_data_df['total_orders'].fillna(0).astype(int)
    customer_data_df['total_revenue'] = customer_data_df['total_revenue'].fillna(0)

    # Log customers with zero orders
    zero_order_count = customer_data_df[customer_data_df['total_orders'] == 0].shape[0]
    logger.info(f"Number of customers with zero orders: {zero_order_count}")

    repeat_threshold = 1
    customer_data_df['repeat_customer'] = customer_data_df['total_orders'].apply(lambda x: 1 if x > repeat_threshold else 0)

    logger.info(f"Final customer_data DataFrame shape: {customer_data_df.shape}")
    logger.info(f"customer_data DataFrame preview:\n{customer_data_df.head()}")

    # Data Validation before loading
    if customer_data_df.isnull().any().any():
        logger.warning("There are null values in the final customer_data DataFrame. Please check your data.")

    # Load data into the customer_data table
    try:
        with engine.begin() as connection:
            customer_data_df.to_sql(
                'customer_data', con=connection, if_exists='replace', index=False,
                dtype={
                    'customer_id': BigInteger(),
                    'customer_name': String(255),
                    'total_orders': Integer(),
                    'total_revenue': Float(),
                    'repeat_customer': Integer()
                }
            )
        logger.info("customer_data table created and loaded successfully.")
    except Exception as e:
        logger.error(f"Error creating customer_data table: {e}")


if __name__ == "__main__":
    create_customer_data_table()
