import pandas as pd
import logging

def replace_nan_with_mode(df):
    """
    Replace NaN values in each column with that column's mode (most frequent value).
    """
    try:
        for column in df.columns:
            if df[column].isnull().any():
                mode_series = df[column].mode()
                if not mode_series.empty:
                    mode_value = mode_series[0]
                    df[column].fillna(mode_value, inplace=True)
        logging.info("NaN values replaced with mode successfully.")
        return df
    except Exception as e:
        logging.error(f"Failed to replace NaN with mode: {e}")
        raise



def clean_columns(df):
    """
    Clean column names by replacing spaces and hyphens with underscores.
    """
    try:
        df.columns = df.columns.str.replace(' ', '_').str.replace('-', '_')
        logging.info("Column names cleaned.")
        return df
    except Exception as e:
        logging.error(f"Failed to clean column names: {e}")
        raise

def preprocess_dates(df):
    """
    Convert date strings to datetime objects.
    """
    try:
        df["Order_Date"] = pd.to_datetime(df["Order_Date"], format="%d-%m-%Y")
        df["Ship_Date"] = pd.to_datetime(df["Ship_Date"], format="%d-%m-%Y")
        logging.info("Date columns formatted successfully.")
        return df
    except Exception as e:
        logging.error(f"Date formatting failed: {e}")
        raise

def create_dimension_tables(df):
    """
    Create dimension tables with surrogate keys, including a date dimension table.
    """
    try:
        # Unique customers
        dim_customer = df[['Customer_ID', 'Customer_Name', 'Segment']].drop_duplicates().reset_index(drop=True)
        dim_customer['CustomerKey'] = dim_customer.index + 1

        # Unique products
        dim_product = df[['Product_ID', 'Category', 'Sub_Category', 'Product_Name']].drop_duplicates().reset_index(drop=True)
        dim_product['ProductKey'] = dim_product.index + 1

        # Unique shipping info (ONLY Ship_Mode now)
        dim_shipping = df[['Ship_Mode']].drop_duplicates().reset_index(drop=True)
        dim_shipping['ShippingKey'] = dim_shipping.index + 1

        # Unique regions
        dim_region = df[['Country', 'City', 'State', 'Postal_Code', 'Region']].drop_duplicates().reset_index(drop=True)
        dim_region['RegionKey'] = dim_region.index + 1

        # Date dimension
        min_date = df[['Order_Date', 'Ship_Date']].min().min()
        max_date = df[['Order_Date', 'Ship_Date']].max().max()

        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        date_dim = pd.DataFrame(date_range, columns=['Date'])
        date_dim['Day'] = date_dim['Date'].dt.day
        date_dim['Month'] = date_dim['Date'].dt.month
        date_dim['Quarter'] = date_dim['Date'].dt.quarter
        date_dim['Year'] = date_dim['Date'].dt.year
        date_dim['Month_Name'] = date_dim['Date'].dt.strftime('%B')
        date_dim['Day_Of_Week'] = date_dim['Date'].dt.day_name()
        date_dim['Day_Of_Year'] = date_dim['Date'].dt.dayofyear
        date_dim['Is_Weekend'] = date_dim['Date'].dt.weekday >= 5
        date_dim['DateKey'] = date_dim['Date'].dt.strftime('%Y%m%d').astype(int)

        # Reorder columns
        date_dim = date_dim[['DateKey', 'Date', 'Day', 'Month', 'Quarter', 'Year',
                             'Month_Name', 'Day_Of_Week', 'Day_Of_Year', 'Is_Weekend']]

        logging.info("Dimension tables created successfully.")
        return dim_customer, dim_product, dim_shipping, dim_region, date_dim

    except Exception as e:
        logging.error(f"Failed to create dimension tables: {e}")
        raise


def create_fact_table(df, dim_customer, dim_product, dim_shipping, dim_region, date_dim):
    """
    Create the fact_sales table by joining with dimension tables and extracting surrogate keys.
    """
    try:
        # Convert date columns
        df['Order_Date'] = pd.to_datetime(df['Order_Date'])
        df['Ship_Date'] = pd.to_datetime(df['Ship_Date'])

        # Merge dimension keys
        df_fact = df.merge(dim_customer[['Customer_ID', 'CustomerKey']], on='Customer_ID', how='left')
        df_fact = df_fact.merge(dim_product[['Product_ID', 'ProductKey']], on='Product_ID', how='left')
        df_fact = df_fact.merge(dim_shipping[['Ship_Mode', 'ShippingKey']], on='Ship_Mode', how='left')
        df_fact = df_fact.merge(dim_region[['Country', 'City', 'State', 'Postal_Code', 'Region', 'RegionKey']],
                                on=['Country', 'City', 'State', 'Postal_Code', 'Region'], how='left')

        # Merge date keys
        df_fact = df_fact.merge(date_dim[['Date', 'DateKey']], left_on='Order_Date', right_on='Date', how='left')
        df_fact = df_fact.rename(columns={'DateKey': 'OrderDateKey'}).drop('Date', axis=1)
        df_fact = df_fact.merge(date_dim[['Date', 'DateKey']], left_on='Ship_Date', right_on='Date', how='left')
        df_fact = df_fact.rename(columns={'DateKey': 'ShipDateKey'}).drop('Date', axis=1)

        # Final fact table
        fact_sales = df_fact[['Order_ID', 'OrderDateKey', 'ShipDateKey',
                              'CustomerKey', 'ProductKey', 'ShippingKey', 'RegionKey', 'Sales']].copy()

        logging.info(f"Fact table created successfully with {len(fact_sales)} rows.")
        return fact_sales

    except Exception as e:
        logging.error(f"Error while creating fact table: {e}")
        raise


def create_dimension_and_fact_tables(df):
    """
    Create dimension and fact tables from the cleaned data.
    """
    try:
        logging.info("Creating dimension tables...")
        dim_customer, dim_product, dim_shipping, dim_region, date_dim = create_dimension_tables(df)
        logging.info("Dimension tables created successfully.")

        logging.info("Creating fact table...")
        fact_sales = create_fact_table(df, dim_customer, dim_product, dim_shipping, dim_region, date_dim)
        logging.info("Fact table created successfully.")

        return dim_customer, dim_product, dim_shipping, dim_region, date_dim, fact_sales
    except Exception as e:
        logging.error(f"Error in creating dimension or fact tables: {e}")
        raise
