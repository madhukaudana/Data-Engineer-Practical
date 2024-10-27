import pandas as pd
import altair as alt
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


class DelivergateDashboard:
    def __init__(self, db_user, db_password, db_host, db_port, db_name):
        self.db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        self.connection = None

    # database connection
    def connect_to_db(self):
        try:
            engine = create_engine(self.db_url)
            self.connection = engine.connect()
            return self.connection
        except SQLAlchemyError as e:
            st.error(f"Database connection failed: {e}")
            return None

    # Load customers and orders data from MySQL database
    def load_data(self):

        if not self.connection:
            return None, None

        customers_query = "SELECT * FROM customers"
        orders_query = "SELECT * FROM orders"
        customers_df = pd.read_sql(customers_query, self.connection)
        orders_df = pd.read_sql(orders_query, self.connection)
        return customers_df, orders_df

    # Display sidebar filters
    def display_filters(self, merged_df):
        st.sidebar.header("Filters")

        # Verify if necessary columns exist in the DataFrame
        required_columns = ["order_date", "total_amount", "customer_id", "order_id"]
        missing_columns = [col for col in required_columns if col not in merged_df.columns]
        if missing_columns:
            st.error(f"Missing required columns: {', '.join(missing_columns)}")
            return merged_df

        # Convert order_date to datetime if it isn't already
        if not pd.api.types.is_datetime64_any_dtype(merged_df["order_date"]):
            try:
                merged_df["order_date"] = pd.to_datetime(merged_df["order_date"])
            except Exception as e:
                st.error(f"Error converting 'order_date' to datetime: {e}")
                return merged_df

        try:
            st.sidebar.subheader("Date Filter")
            start_date = st.sidebar.date_input("Start Date", value=merged_df["order_date"].min().date())
            end_date = st.sidebar.date_input("End Date", value=merged_df["order_date"].max().date())

            if start_date > end_date:
                st.error("Start date must be less than or equal to end date.")
                return merged_df

            date_filtered_df = merged_df[
                (merged_df["order_date"] >= pd.to_datetime(start_date)) &
                (merged_df["order_date"] <= pd.to_datetime(end_date))
                ]
        except Exception as e:
            st.error(f"Error applying date filter: {e}")
            return merged_df

        # Total Spend Filter
        try:
            # the slider maximum based on the filtered date range
            max_total_amount = int(date_filtered_df["total_amount"].max()) if not date_filtered_df.empty else 0

            st.sidebar.markdown("<h5 style='font-size: 16px;'>Filter by Total Spent Amount</h5>", unsafe_allow_html=True)

            total_spent_filter = st.sidebar.slider(
                "Min - Max",
                min_value=0,
                max_value=max_total_amount,
                value=0,
            )

            spend_filtered_df = date_filtered_df[
                date_filtered_df["total_amount"] >= total_spent_filter
                ]
        except Exception as e:
            st.error(f"Error applying total spend filter: {e}")
            return date_filtered_df

        # Orders Count Filter (Dropdown)
        try:
            unique_customer_counts = spend_filtered_df.groupby("customer_id").size()
            max_order_count = unique_customer_counts.max() if not unique_customer_counts.empty else 0

            st.sidebar.markdown("<h5 style='font-size: 16px;'>Filter by Number of Orders</h5>", unsafe_allow_html=True)

            min_orders = st.sidebar.selectbox(
                "Number of Orders",
                options=list(range(1, max_order_count + 1)),
                index=0
            )

            final_filtered_df = spend_filtered_df[
                spend_filtered_df.groupby("customer_id")["order_id"].transform("count") >= min_orders
                ]
        except Exception as e:
            st.error(f"Error applying order count filter: {e}")
            return spend_filtered_df

        return final_filtered_df

    def display_dashboard(self, filtered_df):
        st.header("Filtered Data Table")
        st.dataframe(filtered_df)

        # Summary Metrics
        st.subheader("Summary Metrics")
        st.write(f"**Total Revenue:** ${filtered_df['total_amount'].sum()}")
        st.write(f"**Unique Customers:** {filtered_df['customer_id'].nunique()}")
        st.write(f"**Total Orders:** {filtered_df['order_id'].nunique()}")

        # Bar Chart - Top 10 Customers by Revenue
        st.subheader("Top 10 Customers by Revenue")
        top_customers = filtered_df.groupby("customer_id")["total_amount"].sum().nlargest(10).reset_index()
        bar_chart = alt.Chart(top_customers).mark_bar().encode(
            x=alt.X("customer_id", sort="-y", title="Customer ID"),
            y=alt.Y("total_amount", title="Total Revenue"),
            tooltip=["customer_id", "total_amount"]
        )
        st.altair_chart(bar_chart, use_container_width=True)

        # Line Chart - Revenue Over Time
        st.subheader("Revenue Over Time")
        revenue_over_time = filtered_df.set_index("order_date").resample("W")["total_amount"].sum().reset_index()
        line_chart = alt.Chart(revenue_over_time).mark_line().encode(
            x=alt.X("order_date", title="Date"),
            y=alt.Y("total_amount", title="Total Revenue"),
            tooltip=["order_date", "total_amount"]
        )
        st.altair_chart(line_chart, use_container_width=True)

    def close_connection(self):
        if self.connection:
            self.connection.close()
