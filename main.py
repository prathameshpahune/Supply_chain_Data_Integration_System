import logging
from sqlalchemy import create_engine
from files.load import load_data
from files.transform import clean_columns, preprocess_dates, create_dimension_and_fact_tables,replace_nan_with_mode
from files.database import save_and_insert_to_database
from config import DATABASE_URL

# Set up logging
logging.basicConfig(filename='etl_process.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def load_and_clean_data(file_path):
    """
    Load and clean the data.
    """
    try:
        logging.info("Starting data loading...")
        df = load_data(file_path)
        logging.info(f"Data loaded successfully from {file_path}.")

        logging.info("Starting data cleaning...")
        df = clean_columns(df)
        logging.info("Columns cleaned successfully.")

        df = preprocess_dates(df)
        logging.info("Dates preprocessed successfully.")

        return df
    except Exception as e:
        logging.error(f"Error in loading or cleaning data: {e}")
        raise

if __name__ == "__main__":
    file_path = 'train.csv'

    try:
        engine = create_engine(DATABASE_URL)

        # Step 1: Load and clean data
        df = load_and_clean_data(file_path)

        df = replace_nan_with_mode(df)  # Replace NaNs with mode here

        # Step 2: Create dimension and fact tables
        dim_customer, dim_product, dim_shipping, dim_region, dim_date, fact_sales = create_dimension_and_fact_tables(df)

        # Step 3: Save data to database
        save_and_insert_to_database(engine, dim_customer, dim_product, dim_shipping, dim_region, fact_sales, dim_date)

    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        print("An error occurred during the ETL process. Please check the log for details.")
