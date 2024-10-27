import pandas as pd
from sqlalchemy import create_engine, BigInteger, Float, String, Date
from sqlalchemy.exc import SQLAlchemyError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_USER = 'root'
DB_PASSWORD = 'Muw0okm_PL<'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'delivery'

DATABASE_URL = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

customers_expected_types = {
    'customer_id': 'Int64',
    'customer_name': 'string'
}

orders_expected_types = {
    'order_id': 'Int64',
    'customer_id': 'Int64',
    'total_amount': 'float64',
    'order_date': 'datetime64[ns]'
}


def load_csv(file_path, columns, rename_map):
    df = pd.read_csv(file_path, usecols=columns)
    df.rename(columns=rename_map, inplace=True)
    return df


def validate_and_clean(df, expected_types, unique_column=None):
    """Validates and cleans data to match expected data types."""
    logger.info("Starting data validation and cleaning.")

    for column, expected_type in expected_types.items():
        # Check for missing values and fill if necessary
        if df[column].isnull().any():
            logger.warning(f"Missing values in column '{column}'. Filling with NaN.")
            df[column] = df[column].fillna(value=pd.NA)

        # Validate and convert types
        if not pd.api.types.is_dtype_equal(df[column].dtype, expected_type):
            logger.warning(f"Type mismatch for column '{column}'. Converting to {expected_type}.")
            df[column] = df[column].astype(expected_type)

    # Specifically for orders: Remove negative values from total_amount
    if 'total_amount' in df.columns:
        if (df['total_amount'] < 0).any():
            logger.warning("Removing negative values from 'total_amount'.")
            df = df[df['total_amount'] >= 0]

    # Remove orders without a valid customer_id
    if 'customer_id' in df.columns:
        initial_count = df.shape[0]
        df = df[df['customer_id'].notna() & (df['customer_id'] != '')]
        removed_count = initial_count - df.shape[0]
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} orders without a valid customer_id.")

    # Ensure unique constraints on specified column
    if unique_column and df[unique_column].duplicated().any():
        logger.warning(f"Duplicate values found in '{unique_column}'. Dropping duplicates.")
        df.drop_duplicates(subset=unique_column, inplace=True)

    logger.info("Data validation and cleaning completed.")
    return df


def load_to_database(df, table_name, dtype_map, chunksize=500):
    """Loads data into MySQL database in chunks."""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.begin() as connection:
            df.to_sql(table_name, con=connection, if_exists='replace', index=False, dtype=dtype_map,
                      chunksize=chunksize)
            logger.info(f"{table_name.capitalize()} data loaded successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Database loading error for {table_name}: {e}")


def run_etl():
    customers_df = load_csv(
        '../customers.csv',
        columns=['customer_id', 'name'],
        rename_map={'customer_id': 'customer_id', 'name': 'customer_name'}
    )
    orders_df = load_csv(
        '../order.csv',
        columns=['id', 'customer_id', 'total_amount', 'created_at'],
        rename_map={'id': 'order_id', 'customer_id': 'customer_id', 'total_amount': 'total_amount',
                    'created_at': 'order_date'}
    )

    # Validate and clean customers data
    customers_df = validate_and_clean(customers_df, customers_expected_types, unique_column='customer_id')

    # Validate and clean orders data
    orders_df = validate_and_clean(orders_df, orders_expected_types, unique_column='order_id')

    # Import data in chunks
    load_to_database(
        customers_df,
        'customers',
        dtype_map={'customer_id': BigInteger(), 'customer_name': String(255)}
    )
    load_to_database(
        orders_df,
        'orders',
        dtype_map={'order_id': BigInteger(), 'customer_id': BigInteger(), 'total_amount': Float(), 'order_date': Date()}
    )


if __name__ == "__main__":
    run_etl()
