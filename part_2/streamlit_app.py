import pandas as pd
import streamlit as st
from dashboard import StreamlitDashboard

# Database credentials
DB_USER = 'root'
DB_PASSWORD = 'Muw0okm_PL<'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'delivery'

dashboard = StreamlitDashboard(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

connection = dashboard.connect_to_db()

if connection:
    customers_df, orders_df = dashboard.load_data()
    if customers_df is not None and orders_df is not None:
        merged_df = pd.merge(orders_df, customers_df, on="customer_id")
        merged_df["order_date"] = pd.to_datetime(merged_df["order_date"])

        # Apply filters
        filtered_df = dashboard.display_filters(merged_df)

        # Display dashboard
        dashboard.display_dashboard(filtered_df)

    dashboard.close_connection()
else:
    st.error("Unable to connect to the database.")
