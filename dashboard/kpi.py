import pandas as pd
import logging
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data_from_mysql(query):
    """
    Fetch data from MySQL database.
    """
    try:
        # Replace the following with your MySQL connection URL
        DATABASE_URL = "mysql+pymysql://username:password@host/database_name"
        engine = create_engine(DATABASE_URL)
        data = pd.read_sql(query, engine)
        logging.info(f"Data fetched successfully for query: {query}")
        return data
    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        raise

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

def top_selling_products(fact_df):
    """
    Get the top selling products from the fact data.
    """
    try:
        top_products = fact_df.groupby('Product_Name').agg(Sales=('Sales', 'sum')).reset_index()
        top_products = top_products.sort_values(by='Sales', ascending=False).head(5)
        logging.info("Top selling products calculated successfully.")
        return top_products
    except Exception as e:
        logging.error(f"Error calculating top selling products: {e}")
        raise

def average_delivery_time(dim_customer):
    """
    Calculate the average delivery time.
    """
    try:
        avg_delivery = dim_customer['Delivery_Time'].mean()
        logging.info(f"Average delivery time calculated successfully: {avg_delivery}")
        return avg_delivery
    except Exception as e:
        logging.error(f"Error calculating average delivery time: {e}")
        raise

def monthly_sales(fact_df):
    """
    Calculate monthly sales trend.
    """
    try:
        fact_df['Month'] = fact_df['Order_Date'].dt.to_period('M')
        monthly = fact_df.groupby('Month').agg(Sales=('Sales', 'sum')).reset_index()
        logging.info("Monthly sales calculated successfully.")
        return monthly
    except Exception as e:
        logging.error(f"Error calculating monthly sales: {e}")
        raise

def segment_sales(fact_df, dim_customer):
    """
    Calculate sales by customer segment.
    """
    try:
        segment = fact_df.merge(dim_customer[['Customer_ID', 'Segment']], on='Customer_ID', how='left')
        segment_sales = segment.groupby('Segment').agg(Sales=('Sales', 'sum')).reset_index()
        logging.info("Sales by customer segment calculated successfully.")
        return segment_sales
    except Exception as e:
        logging.error(f"Error calculating sales by segment: {e}")
        raise

def create_date_dimension(start_date, end_date):
    """
    Create a date dimension table with attributes such as day, month, quarter, and year.
    """
    try:
        # Generate a date range between start_date and end_date
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        # Create the date dimension dataframe
        date_dim = pd.DataFrame(date_range, columns=['Date'])
        date_dim['Day'] = date_dim['Date'].dt.day
        date_dim['Month'] = date_dim['Date'].dt.month
        date_dim['Quarter'] = date_dim['Date'].dt.quarter
        date_dim['Year'] = date_dim['Date'].dt.year
        date_dim['Month_Name'] = date_dim['Date'].dt.strftime('%B')
        date_dim['Day_Of_Week'] = date_dim['Date'].dt.day_name()
        date_dim['Day_Of_Year'] = date_dim['Date'].dt.dayofyear
        date_dim['Is_Weekend'] = date_dim['Date'].dt.weekday >= 5

        # Add a surrogate key for the date dimension
        date_dim['DateKey'] = date_dim['Date'].dt.strftime('%Y%m%d').astype(int)

        logging.info("Date dimension table created successfully.")
        return date_dim
    except Exception as e:
        logging.error(f"Failed to create date dimension: {e}")
        raise

# # Example Usage

# if __name__ == "__main__":
#     # Connect and load fact data and customer data
#     try:
#         fact_df = load_fact_data()
#         dim_customer = load_train_data()

#         # Calculate KPIs
#         total_sales, total_orders, avg_sales = calculate_kpis(fact_df)
#         avg_delivery = average_delivery_time(dim_customer)

#         # Output KPIs
#         print(f"Total Sales: ${total_sales:,.2f}")
#         print(f"Total Orders: {total_orders}")
#         print(f"Avg Sales per Order: ${avg_sales:,.2f}")
#         print(f"Avg Delivery Time: {avg_delivery} days")

#         # Create Date Dimension Table
#         date_dim = create_date_dimension("2021-01-01", "2021-12-31")
#         print(f"Date Dimension Created with {len(date_dim)} rows.")

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
