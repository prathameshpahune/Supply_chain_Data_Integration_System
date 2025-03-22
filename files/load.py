import pandas as pd
import logging

def load_data(file_path):
    """
    Load CSV data into a Pandas DataFrame.
    
    """
    try:
        data = pd.read_csv(file_path)
        logging.info("CSV file loaded successfully.")
        return data
    except Exception as e:
        logging.error(f"Failed to load CSV file: {e}")
        raise
