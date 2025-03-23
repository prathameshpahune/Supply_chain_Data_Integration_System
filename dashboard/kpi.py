import pandas as pd
import logging
from sqlalchemy import create_engine

DATABASE_URL = 'mysql+pymysql://root:root@localhost:3306/SALES'
engine = create_engine(DATABASE_URL)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data(query):
    try:
        df = pd.read_sql(query, con=engine)
        logging.info(f"Fetched data for query: {query[:50]}...")
        return df
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise

def load_tables():
    fact_df = fetch_data("SELECT * FROM fact_sales;")
    dim_customer = fetch_data("SELECT * FROM dim_customer;")
    dim_date = fetch_data("SELECT * FROM dim_date;")
    dim_product = fetch_data("SELECT * FROM dim_product;")
    dim_region = fetch_data("SELECT * FROM dim_region;")
    return fact_df, dim_customer, dim_date, dim_product, dim_region

def calculate_core_kpis(fact_df):
    total_sales = fact_df['Sales'].sum()
    total_orders = fact_df['Order_ID'].nunique()
    avg_sales = total_sales / total_orders if total_orders != 0 else 0
    return total_sales, total_orders, avg_sales


def avg_sales_per_month(fact_df, dim_date):
    try:
        # Merge using OrderDateKey instead of DateKey
        merged = pd.merge(fact_df, dim_date, left_on='OrderDateKey', right_on='DateKey', how='left')  

        # Group by Month_Year and sum sales
        monthly_sales = (
            merged.groupby(['Year', 'Month'])['Sales']
            .sum()
            .reset_index()
        )

        # Calculate average monthly sales
        avg_monthly_sales = monthly_sales['Sales'].mean()

        logging.info(f"Average monthly sales calculated: ${avg_monthly_sales:,.2f}")
        return avg_monthly_sales
    except Exception as e:
        logging.error(f"Error calculating average monthly sales: {e}")
        raise

def customer_count_segment(dim_customer):
    segment_group = dim_customer.groupby('Segment').size().reset_index(name='Customer_Count')
    return segment_group


def monthly_sales_trend(fact_df, dim_date):
    merged = fact_df.merge(dim_date[['DateKey', 'Month', 'Year']], left_on='OrderDateKey', right_on='DateKey', how='left')
    merged['Month_Year'] = merged['Month'].astype(str) + '-' + merged['Year'].astype(str)
    trend = merged.groupby('Month_Year').agg(Sales=('Sales', 'sum')).reset_index()
    return trend.sort_values('Month_Year')


def sales_by_year(fact_df, dim_date):
    merged = fact_df.merge(dim_date[['DateKey', 'Year']], left_on='OrderDateKey', right_on='DateKey', how='left')
    yearly_sales = merged.groupby('Year').agg(Sales=('Sales', 'sum')).reset_index()
    return yearly_sales.sort_values('Year')


def sales_by_quarter(fact_df, dim_date):
    merged = fact_df.merge(dim_date[['DateKey', 'Quarter', 'Year']], left_on='OrderDateKey', right_on='DateKey', how='left')
    merged['Quarter_Year'] = 'Q' + merged['Quarter'].astype(str) + '-' + merged['Year'].astype(str)
    q_sales = merged.groupby('Quarter_Year').agg(Sales=('Sales', 'sum')).reset_index()
    return q_sales.sort_values('Quarter_Year')


def weekend_sales(fact_df, dim_date):
    merged = fact_df.merge(dim_date[['DateKey', 'Is_Weekend']], left_on='OrderDateKey', right_on='DateKey', how='left')
    result = merged.groupby('Is_Weekend').agg(Sales=('Sales', 'sum')).reset_index()
    result['Type'] = result['Is_Weekend'].apply(lambda x: 'Weekend' if x else 'Weekday')
    return result[['Type', 'Sales']]


def top_states_sales(fact_df):
    state_sales = fact_df.groupby('State').agg(Sales=('Sales', 'sum')).reset_index()
    return state_sales.sort_values(by='Sales', ascending=False).head(5)

def top_states_sales(fact_df, dim_region):
    merged = fact_df.merge(dim_region[['RegionKey', 'State']], on='RegionKey', how='left')
    state_sales = merged.groupby('State').agg(Sales=('Sales', 'sum')).reset_index()
    return state_sales.sort_values(by='Sales', ascending=False).head(5)

def top_5_products(fact_df, dim_product):
    # Merge on Product_ID or your specific key
    merged_df = fact_df.merge(dim_product, on='ProductKey', how='left')
    
    top_products = (
        merged_df.groupby('Product_Name')
        .agg(Sales=('Sales', 'sum'))
        .reset_index()
        .sort_values(by='Sales', ascending=False)
        .head(5)
    )
    return top_products

