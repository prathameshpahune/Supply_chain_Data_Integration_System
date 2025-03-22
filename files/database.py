import pandas as pd
import logging
from sqlalchemy import create_engine, text
from config import DATABASE_URL

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_tables(engine):
    """
    Create tables with constraints.
    """
    try:
        with engine.connect() as conn:
            logging.info("Creating tables with constraints...")

            # Create the dimension tables with constraints
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
                Order_ID VARCHAR(255),
                Order_Date DATE,
                Ship_Mode VARCHAR(255),
                Ship_Date DATE
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


# def insert_data_row_by_row(engine, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date):
#     """
#     Insert data row by row into MySQL database using direct CustomerKey, ProductKey, etc.
#     """
#     try:
#         with engine.begin() as conn:
#             logging.info("Inserting dimension tables row by row...")

#             # ----------------- Insert dim_customer -----------------
#             for _, row in dim_customer.iterrows():
#                 conn.execute(text("""
#                     INSERT INTO dim_customer (CustomerKey, Customer_ID, Customer_Name, Segment)
#                     VALUES (:CustomerKey, :Customer_ID, :Customer_Name, :Segment)
#                 """), {
#                     'CustomerKey': row['CustomerKey'],
#                     'Customer_ID': row['Customer_ID'],
#                     'Customer_Name': row['Customer_Name'],
#                     'Segment': row['Segment']
#                 })

#             # ----------------- Insert dim_product -----------------
#             for _, row in dim_product.iterrows():
#                 conn.execute(text("""
#                     INSERT INTO dim_product (ProductKey, Product_ID, Category, Sub_Category, Product_Name)
#                     VALUES (:ProductKey, :Product_ID, :Category, :Sub_Category, :Product_Name)
#                 """), {
#                     'ProductKey': row['ProductKey'],
#                     'Product_ID': row['Product_ID'],
#                     'Category': row['Category'],
#                     'Sub_Category': row['Sub_Category'],
#                     'Product_Name': row['Product_Name']
#                 })

#             # ----------------- Insert dim_shipping -----------------
#             for _, row in dim_shipping.iterrows():
#                 conn.execute(text("""
#                     INSERT INTO dim_shipping (ShippingKey, Order_ID, Order_Date, Ship_Mode, Ship_Date)
#                     VALUES (:ShippingKey, :Order_ID, :Order_Date, :Ship_Mode, :Ship_Date)
#                 """), {
#                     'ShippingKey': row['ShippingKey'],
#                     'Order_ID': row['Order_ID'],
#                     'Order_Date': row['Order_Date'],
#                     'Ship_Mode': row['Ship_Mode'],
#                     'Ship_Date': row['Ship_Date']
#                 })

#             # ----------------- Insert dim_region -----------------
#             for _, row in dim_region.iterrows():
#                 conn.execute(text("""
#                     INSERT INTO dim_region (RegionKey, Country, City, State, Postal_Code, Region)
#                     VALUES (:RegionKey, :Country, :City, :State, :Postal_Code, :Region)
#                 """), {
#                     'RegionKey': row['RegionKey'],
#                     'Country': row['Country'],
#                     'City': row['City'],
#                     'State': row['State'],
#                     'Postal_Code': row['Postal_Code'],
#                     'Region': row['Region']
#                 })

#             # ----------------- Insert dim_date -----------------
#             for _, row in dim_date.iterrows():
#                 conn.execute(text("""
#                     INSERT INTO dim_date (DateKey, Date, Day, Month, Quarter, Year, Month_Name, Day_Of_Week, Day_Of_Year, Is_Weekend)
#                     VALUES (:DateKey, :Date, :Day, :Month, :Quarter, :Year, :Month_Name, :Day_Of_Week, :Day_Of_Year, :Is_Weekend)
#                 """), {
#                     'DateKey': row['DateKey'],
#                     'Date': row['Date'],
#                     'Day': row['Day'],
#                     'Month': row['Month'],
#                     'Quarter': row['Quarter'],
#                     'Year': row['Year'],
#                     'Month_Name': row['Month_Name'],
#                     'Day_Of_Week': row['Day_Of_Week'],
#                     'Day_Of_Year': row['Day_Of_Year'],
#                     'Is_Weekend': row['Is_Weekend']
#                 })

#             logging.info("Dimension tables inserted successfully.")

#             # ----------------- Insert fact_sales directly -----------------
#             logging.info("Inserting fact_sales directly...")

#             for _, row in fact_sales.iterrows():
#                 conn.execute(text("""
#                     INSERT INTO fact_sales (Order_ID, OrderDateKey, ShipDateKey, CustomerKey, ProductKey, ShippingKey, RegionKey, Sales)
#                     VALUES (:Order_ID, :OrderDateKey, :ShipDateKey, :CustomerKey, :ProductKey, :ShippingKey, :RegionKey, :Sales)
#                 """), {
#                     'Order_ID': row['Order_ID'],
#                     'OrderDateKey': row['OrderDateKey'],
#                     'ShipDateKey': row['ShipDateKey'],
#                     'CustomerKey': row['CustomerKey'],
#                     'ProductKey': row['ProductKey'],
#                     'ShippingKey': row['ShippingKey'],
#                     'RegionKey': row['RegionKey'],
#                     'Sales': row['Sales']
#                 })

#             logging.info("Fact table inserted successfully.")

#     except Exception as e:
#         logging.error(f"Error inserting data: {e}", exc_info=True)
#         raise

# def insert_into_database(dim_customer, dim_product, dim_shipping, dim_region, fact_sales):
#     """
#     Insert data into MySQL database with constraints.
#     """
#     try:
#         engine = create_engine(DATABASE_URL)

#         # Write data to database (overwrite if exists)
#         dim_customer.to_sql('dim_customer', con=engine, if_exists='replace', index=False)
#         dim_product.to_sql('dim_product', con=engine, if_exists='replace', index=False)
#         dim_shipping.to_sql('dim_shipping', con=engine, if_exists='replace', index=False)
#         dim_region.to_sql('dim_region', con=engine, if_exists='replace', index=False)
#         fact_sales.to_sql('fact_sales', con=engine, if_exists='replace', index=False)

#         logging.info("Tables inserted...")

#         logging.info("Constraints added successfully.")
#     except Exception as e:
#         logging.error(f"Failed to insert data: {e}")
#         raise


def save_and_insert_to_database(engine, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date):
    # SQLAlchemy engine
    # engine = create_engine(f'{DATABASE_URL}')
    # Dictionary mapping table names to DataFrames
    tables = {
        'dim_customer': dim_customer,
        'dim_product': dim_product,
        'dim_shipping': dim_shipping,
        'dim_region': dim_region,
        'fact_sales': fact_sales,
        'dim_date': dim_date
    }

    # Insert data into MySQL
    for table_name, df in tables.items():
        try:
            logging.info(f"Inserting data into {table_name}...")
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logging.info(f"Data inserted successfully into {table_name}.")
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}")

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



# if __name__ == "__main__":
#     # Create the SQLAlchemy engine
#     engine = create_engine(DATABASE_URL)

#     # Create tables
#     create_tables(engine)

#     # Assuming dim_customer, dim_product, dim_shipping, dim_region, and fact_sales are pandas DataFrames
#     # Call the insert function to insert data row by row
#     insert_data_row_by_row(engine, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date)

#     # Fetch data from MySQL to verify
#     fetched_data = fetch_data_from_mysql(engine)
#     print(fetched_data)
