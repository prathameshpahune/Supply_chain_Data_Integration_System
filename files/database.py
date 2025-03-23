import pandas as pd
import logging
from sqlalchemy import create_engine, text
from config import DATABASE_URL

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_tables(engine):
    """
    Create tables with constraints if they do not exist.
    """
    try:
        with engine.connect() as conn:
            logging.info("Creating tables with constraints...")

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_customer (
                    CustomerKey INT PRIMARY KEY AUTO_INCREMENT,
                    Customer_ID VARCHAR(255),
                    Customer_Name VARCHAR(255),
                    Segment VARCHAR(255)
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_product (
                    ProductKey INT PRIMARY KEY AUTO_INCREMENT,
                    Product_ID VARCHAR(255),
                    Category VARCHAR(255),
                    Sub_Category VARCHAR(255),
                    Product_Name VARCHAR(255)
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_shipping (
                    ShippingKey INT PRIMARY KEY AUTO_INCREMENT,
                    Ship_Mode VARCHAR(255)
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_region (
                    RegionKey INT PRIMARY KEY AUTO_INCREMENT,
                    Country VARCHAR(255),
                    City VARCHAR(255),
                    State VARCHAR(255),
                    Postal_Code VARCHAR(255),
                    Region VARCHAR(255)
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_date (
                    DateKey INT PRIMARY KEY,
                    Date DATE,
                    Day INT,
                    Month INT,
                    Quarter INT,
                    Year INT,
                    Month_Name VARCHAR(255),
                    Day_Of_Week VARCHAR(255),
                    Day_Of_Year INT,
                    Is_Weekend BOOLEAN
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_sales (
                    Order_ID VARCHAR(255),
                    OrderDateKey INT,
                    ShipDateKey INT,
                    CustomerKey INT,
                    ProductKey INT,
                    ShippingKey INT,
                    RegionKey INT,
                    Sales DECIMAL(10, 2),
                    FOREIGN KEY (CustomerKey) REFERENCES dim_customer(CustomerKey),
                    FOREIGN KEY (ProductKey) REFERENCES dim_product(ProductKey),
                    FOREIGN KEY (ShippingKey) REFERENCES dim_shipping(ShippingKey),
                    FOREIGN KEY (RegionKey) REFERENCES dim_region(RegionKey),
                    FOREIGN KEY (OrderDateKey) REFERENCES dim_date(DateKey)
                );
            """))

            logging.info("Tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        raise


def save_and_insert_to_database(engine, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date):
    """
    Insert dataframes into database tables without overwriting constraints.
    """
    tables = {
        'dim_customer': dim_customer,
        'dim_product': dim_product,
        'dim_shipping': dim_shipping,
        'dim_region': dim_region,
        'dim_date': dim_date,
        'fact_sales': fact_sales
    }

    for table_name, df in tables.items():
        try:
            logging.info(f"Inserting data into {table_name}...")
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logging.info(f"Data inserted successfully into {table_name}.")
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}")
            raise



# def save_and_insert_to_database(engine, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date):
#     # SQLAlchemy engine
#     # engine = create_engine(f'{DATABASE_URL}')
#     # Dictionary mapping table names to DataFrames
#     tables = {
#         'dim_customer': dim_customer,
#         'dim_product': dim_product,
#         'dim_shipping': dim_shipping,
#         'dim_region': dim_region,
#         'fact_sales': fact_sales,
#         'dim_date': dim_date
#     }

#     # Insert data into MySQL
#     for table_name, df in tables.items():
#         try:
#             logging.info(f"Inserting data into {table_name}...")
#             df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
#             logging.info(f"Data inserted successfully into {table_name}.")
#         except Exception as e:
#             logging.error(f"Error inserting data into {table_name}: {e}")

    # engine.dispose()



def fetch_data_from_mysql(engine):
    """
    Fetch data from MySQL to verify the insertion.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM fact_sales;"))
            fact_sales_data = result.fetchall()
            logging.info(f"Fetched data: {fact_sales_data}")
            return fact_sales_data
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise
