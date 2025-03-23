import logging
from sqlalchemy import create_engine
from files.load import load_data
from files.transform import clean_columns, preprocess_dates, create_dimension_and_fact_tables, replace_nan_with_mode
from files.database import save_and_insert_to_database, create_tables
from config import DATABASE_URL

# Logging setup
logging.basicConfig(filename='etl_process.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def load_and_clean_data(file_path):
    """
    Load and clean the data.
    """
    try:
        logging.info("Loading data...")
        df = load_data(file_path)
        df = clean_columns(df)
        df = preprocess_dates(df)
        df = replace_nan_with_mode(df)
        logging.info("Data loaded and cleaned.")
        return df
    except Exception as e:
        logging.error(f"Data load/clean failed: {e}")
        raise

if __name__ == "__main__":
    file_path = 'train.csv'
    try:  

        engine = create_engine(DATABASE_URL)

        # Load and clean data
        df = load_and_clean_data(file_path)

        # Create dimension and fact tables
        dims_and_fact = create_dimension_and_fact_tables(df)
        dim_customer, dim_product, dim_shipping, dim_region, dim_date, fact_sales = dims_and_fact

        # Create tables in DB
        create_tables(engine)

        # Insert data
        save_and_insert_to_database(engine, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date)

        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        print("ETL failed. Check logs.")
