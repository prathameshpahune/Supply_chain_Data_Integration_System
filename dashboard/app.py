import logging
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
from kpi import (
    avg_sales_per_month,
    load_tables,
    calculate_core_kpis,
    sales_by_year,
    weekend_sales,
    top_states_sales,
    customer_count_segment,
    monthly_sales_trend,
    sales_by_quarter,
    top_5_products
)

# Configure logging
logging.basicConfig(filename='../etl_process.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load data once for all pages
try:
    fact_df, dim_customer, dim_date, dim_product, dim_region = load_tables()
    logging.info("Data loaded successfully.")
except Exception as e:
    logging.error(f"Error loading data: {e}")
    st.error("Failed to load data.")

# Page: Home
def show_home_page():
    try:
        st.title("🏠 Sales Dashboard")
        st.markdown("Welcome to the ***Home Page***. Use the sidebar to navigate through different reports and insights.")
        st.subheader("🔍 Sample Data Preview")

        # Display Fact Table 
        st.write("Fact Table")
        st.dataframe(fact_df.head().T)  # Transpose for row-wise display

        # Display Customer Dimension 
        st.write("Customer Dimension")
        st.dataframe(dim_customer.head().T)  # Transpose for row-wise display

        st.write("Date Dimension")
        st.dataframe(dim_date.head().T)

        st.write("Product Dimension")
        st.dataframe(dim_product.head().T)

        st.write("Region Dimension")
        st.dataframe(dim_region.head().T)

        logging.info("Home page rendered.")
    except Exception as e:
        logging.error(f"Error in Home page: {e}")
        st.error("Error loading Home page.")

def show_diagram():
    try:
        image = "../etl.png"
        st.image(image, caption="Database ERD Diagram", use_container_width=True)
    except Exception as e:
        st.error(f"Error loading image: {e}")

def show_kpi_page():
    try:
        st.title("📈 KPI Overview")

        # Calculate core KPIs
        total_sales, total_orders, avg_sales = calculate_core_kpis(fact_df)

        # Get average monthly sales and monthly breakdown
        avg_sales_m = avg_sales_per_month(fact_df, dim_date)

        # Set your target sales (can be based on avg monthly or set manually)
        target_sales = avg_sales_m  

        st.subheader("🔢 Key Performance Indicators")
        col1, col2, col3 = st.columns(3)
        col1.metric("💰 Total Sales", f"${total_sales:,.2f}")
        col2.metric("🛒 Total Orders", total_orders)
        col3.metric("📦 Avg Sales/Order", f"${avg_sales:,.2f}")

        st.metric("📅 Avg Sales/Month", f"${avg_sales_m:,.2f}")

        # Sales Target Progress
        st.markdown("---")
        st.subheader("🎯 Sales Target Progress")

        progress = total_sales / target_sales
        progress_percent = min(progress, 1.0) * 100  # Cap at 100%

        st.progress(progress_percent / 100)

        if total_sales >= target_sales:
            st.success(f"Great! You've met your target of   ${ target_sales : ,.2f} with sales of   ${  total_sales : ,.2f}.")
        else:
            st.warning(f"Current sales: ${  total_sales : ,.2f}. Target: ${  target_sales  : ,.2f}. Keep pushing to reach your goal!")

        # Simple KPI insights
        st.markdown("---")
        st.subheader("📊 KPI Insights")
        st.markdown(f"""
        - **Total Sales**: You're currently at **${total_sales:,.2f}**.
        - **Target Sales**: Your goal is **${target_sales:,.2f}**.
        - You're at **{progress_percent:.1f}%** of your goal.
        """)

        logging.info("KPI Overview page with monthly trend chart rendered.")
    except Exception as e:
        logging.error(f"Error in KPI Overview: {e}")
        st.error("Error loading KPI Overview.")

# Page: Top Products
def show_top_products_page():
    try:
        st.title("🏆 Top 5 Selling Products")
        top_products = top_5_products(fact_df, dim_product)

        st.subheader("📊 Product Sales Table")
        st.dataframe(top_products)

        st.subheader("📈 Sales Chart")
        fig, ax = plt.subplots()
        sns.barplot(data=top_products, x='Sales', y='Product_Name', palette='magma', ax=ax)
        ax.set_title('Top 5 Products by Sales')
        ax.set_xlabel('Sales ($)')
        ax.set_ylabel('Product')
        st.pyplot(fig)
        logging.info("Top Products page rendered.")
    except Exception as e:
        logging.error(f"Error in Top Products page: {e}")
        st.error("Error loading Top Products page.")

# Page: Sales Trends
def show_sales_trends_page():
    try:
        st.title("📊 Sales Trends")

        # Monthly Sales
        st.subheader("📅 Monthly Sales Trend")
        monthly_trend = monthly_sales_trend(fact_df, dim_date)
        fig1, ax1 = plt.subplots(figsize=(20, 5))
        sns.lineplot(data=monthly_trend, x='Month_Year', y='Sales', marker='o', ax=ax1)
        ax1.set_title('Monthly Sales')
        ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)
        st.pyplot(fig1)

        # Quarterly Sales
        st.subheader("📆 Quarterly Sales")
        q_sales = sales_by_quarter(fact_df, dim_date)
        fig2, ax2 = plt.subplots(figsize=(20, 5))
        sns.barplot(data=q_sales, x='Quarter_Year', y='Sales', palette='Blues_d', ax=ax2)
        ax2.set_title('Quarterly Sales')
        ax2.set_xlabel('Quarter-Year')
        ax2.set_ylabel('Sales')
        ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45)
        st.pyplot(fig2)

        # Yearly Sales
        st.subheader("🗓️ Yearly Sales")
        yearly = sales_by_year(fact_df, dim_date)
        fig3, ax3 = plt.subplots(figsize=(20, 5))
        sns.barplot(data=yearly, x='Year', y='Sales', palette='coolwarm', ax=ax3)
        ax3.set_title('Yearly Sales')
        st.pyplot(fig3)

        # Weekend vs Weekday Pie Chart
        st.subheader("📆 Weekend vs Weekday Sales")
        weekend_data = weekend_sales(fact_df, dim_date)
        fig4, ax4 = plt.subplots(figsize=(5, 5))
        ax4.pie(weekend_data['Sales'], labels=weekend_data['Type'], autopct='%1.1f%%', colors=['#66b3ff', '#ff9999'])
        ax4.set_title('Weekend vs Weekday Sales')
        st.pyplot(fig4)

        logging.info("Sales Trends page rendered.")
    except Exception as e:
        logging.error(f"Error in Sales Trends page: {e}")
        st.error("Error loading Sales Trends page.")

# Page: Sales by Segment
def show_sales_by_segment_page():
    try:
        st.title("👥 Sales by Segment")

        # Top States
        st.subheader("📍 Top 5 States by Sales")
        top_states = top_states_sales(fact_df, dim_region)
        st.dataframe(top_states)
        fig, ax = plt.subplots(figsize=(20, 7))
        sns.barplot(data=top_states, x='Sales', y='State', palette='viridis', ax=ax)
        ax.set_title('Top 5 States by Sales')
        st.pyplot(fig)

        # Customer Segment
        st.subheader("👤 Customer Segment Distribution")
        segment_count = customer_count_segment(dim_customer)
        fig2, ax2 = plt.subplots(figsize=(20, 7))
        sns.barplot(data=segment_count, x='Segment', y='Customer_Count', palette='pastel', ax=ax2)
        ax2.set_title('Customer Count by Segment')
        st.pyplot(fig2)

        logging.info("Sales by Segment page rendered.")
    except Exception as e:
        logging.error(f"Error in Sales by Segment page: {e}")
        st.error("Error loading Sales by Segment page.")


# Main navigation
def main():
    st.set_page_config(page_title="Sales Dashboard", layout="wide")
    page = st.sidebar.radio("Select a page:", ["Home", "ERD Diagram", "KPI Overview", "Top Products", "Sales Trends", "Sales by Segment"])

    try:
        if page == "Home":
            show_home_page()
        elif page == "ERD Diagram":
            show_diagram()
        elif page == "KPI Overview":
            show_kpi_page()
        elif page == "Top Products":
            show_top_products_page()
        elif page == "Sales Trends":
            show_sales_trends_page()
        elif page == "Sales by Segment":
            show_sales_by_segment_page()
    except Exception as e:
        logging.error(f"Error rendering page: {e}")
        st.error("An error occurred while loading the page. Please try again later.")

# Run the app
if __name__ == "__main__":
    main()
