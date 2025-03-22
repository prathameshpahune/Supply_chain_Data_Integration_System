import logging
import pandas as pd
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from kpi import (
    load_fact_data,
    load_train_data,
    calculate_kpis,
    top_selling_products,
    average_delivery_time,
    monthly_sales,
    segment_sales,
    create_date_dimension
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to fetch data from MySQL
def fetch_data_from_mysql(query):
    """
    Fetch data from MySQL database.
    """
    try:
        DATABASE_URL = "mysql+pymysql://username:password@host/database_name"
        engine = create_engine(DATABASE_URL)
        data = pd.read_sql(query, engine)
        logging.info(f"Data fetched successfully for query: {query}")
        return data
    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        raise

# Function to load fact data
def load_fact_data():
    """
    Load fact data from MySQL.
    """
    try:
        query = "SELECT * FROM fact_sales;"
        fact_df = fetch_data_from_mysql(query)
        logging.info("Fact data loaded successfully.")
        return fact_df
    except Exception as e:
        logging.error(f"Error loading fact data: {e}")
        raise

# Function to load customer data
def load_train_data():
    """
    Load training data (Customer Dimension) from MySQL.
    """
    try:
        query = "SELECT * FROM dim_customer;"
        dim_customer = fetch_data_from_mysql(query)
        logging.info("Training data (dim_customer) loaded successfully.")
        return dim_customer
    except Exception as e:
        logging.error(f"Error loading training data (dim_customer): {e}")
        raise

# Function to calculate KPIs
def calculate_kpis(fact_df):
    """
    Calculate KPIs like total sales, total orders, and average sales/order.
    """
    try:
        total_sales = fact_df['Sales'].sum()
        total_orders = fact_df['Order_ID'].nunique()
        avg_sales = total_sales / total_orders if total_orders != 0 else 0
        logging.info(f"KPIs calculated successfully: Total Sales: {total_sales}, Total Orders: {total_orders}, Avg Sales: {avg_sales}")
        return total_sales, total_orders, avg_sales
    except Exception as e:
        logging.error(f"Error calculating KPIs: {e}")
        raise

# Function to display the Streamlit pages
def main():
    st.set_page_config(page_title="Sales Dashboard", layout="wide")
    
    # Create a sidebar for navigation between pages
    page = st.sidebar.radio("Select a page:", ["Home", "KPI Overview", "Top Products", "Sales Trends", "Sales by Segment"])

    try:
        if page == "Home":
            show_home_page()
        elif page == "KPI Overview":
            show_kpi_page()
        elif page == "Top Products":
            show_top_products_page()
        elif page == "Sales Trends":
            show_sales_trends_page()
        elif page == "Sales by Segment":
            show_sales_by_segment_page()

    except Exception as e:
        logging.error(f"Error in page rendering: {e}")
        st.error("An error occurred while loading the page. Please try again later.")

# Home Page Function
def show_home_page():
    try:
        st.title("📊 Sales Dashboard")
        st.markdown("Welcome to the Sales Dashboard. Use the sidebar to navigate between different pages.")
        logging.info("Home page displayed successfully.")
    except Exception as e:
        logging.error(f"Error displaying Home page: {e}")
        st.error("An error occurred while displaying the Home page.")

# KPI Overview Page Function
def show_kpi_page():
    try:
        st.title("📊 KPI Overview")
        fact_df = load_fact_data()

        total_sales, total_orders, avg_sales = calculate_kpis(fact_df)

        st.markdown("### Key Performance Indicators")
        col1, col2, col3 = st.columns(3)
        col1.metric("💰 Total Sales", f"${total_sales:,.2f}")
        col2.metric("🛒 Total Orders", total_orders)
        col3.metric("📦 Avg Sales/Order", f"${avg_sales:,.2f}")

        logging.info("KPI page displayed successfully.")
    except Exception as e:
        logging.error(f"Error displaying KPI Overview page: {e}")
        st.error("An error occurred while displaying the KPI Overview page.")

# Top Products Page Function
def show_top_products_page():
    try:
        st.title("🏆 Top Selling Products")
        original_df = load_train_data()

        top_products = top_selling_products(original_df)
        st.table(top_products)

        fig, ax = plt.subplots()
        sns.barplot(data=top_products, x='Sales', y='Product_Name', palette='viridis', ax=ax)
        ax.set_title('Top 5 Products by Sales')
        ax.set_xlabel('Sales ($)')
        ax.set_ylabel('Product Name')
        st.pyplot(fig)

        logging.info("Top products page displayed successfully.")
    except Exception as e:
        logging.error(f"Error displaying Top Products page: {e}")
        st.error("An error occurred while displaying the Top Products page.")

# Sales Trends Page Function
def show_sales_trends_page():
    try:
        st.title("📈 Sales Trends")
        fact_df = load_fact_data()

        monthly = monthly_sales(fact_df)

        fig, ax = plt.subplots()
        ax.plot(monthly['Month'], monthly['Sales'], marker='o', linestyle='-', color='green')
        ax.set_title('Monthly Sales Trend')
        ax.set_xlabel('Month')
        ax.set_ylabel('Sales')
        plt.xticks(rotation=45)
        st.pyplot(fig)

        logging.info("Sales trends page displayed successfully.")
    except Exception as e:
        logging.error(f"Error displaying Sales Trends page: {e}")
        st.error("An error occurred while displaying the Sales Trends page.")

# Sales by Segment Page Function
def show_sales_by_segment_page():
    try:
        st.title("👥 Sales by Customer Segment")
        dim_customer = pd.read_csv('dim_customer.csv')  # Assuming dim_customer is a CSV file for this example
        fact_df = load_fact_data()

        segment = segment_sales(fact_df, dim_customer)
        st.dataframe(segment.style.format({"Sales": "${:,.2f}"}))

        fig, ax = plt.subplots()
        sns.barplot(data=segment, x='Segment', y='Sales', palette='pastel', ax=ax)
        ax.set_title('Sales by Customer Segment')
        ax.set_xlabel('Segment')
        ax.set_ylabel('Sales ($)')
        st.pyplot(fig)

        logging.info("Sales by Segment page displayed successfully.")
    except Exception as e:
        logging.error(f"Error displaying Sales by Segment page: {e}")
        st.error("An error occurred while displaying the Sales by Segment page.")

# Run the app
if __name__ == "__main__":
    main()
