import pandas as pd
import glob
import logging
import time

# Configure logging
logging.basicConfig(filename='etl_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(file_path):
    """
    Extract data from a CSV file or multiple CSVs using glob.
    """
    start_time = time.time()
    try:
        if "*" in file_path:
            all_files = glob.glob(file_path)
            dataframes = [pd.read_csv(file) for file in all_files]
            df = pd.concat(dataframes, ignore_index=True)
        else:
            df = pd.read_csv(file_path)
        logging.info(f"Extracted data successfully from {file_path}")
    except Exception as e:
        logging.error(f"Failed to extract data: {e}")
    logging.info(f"Extraction Time: {time.time() - start_time} seconds")
    return df

def transform_data(df):
    """
    Perform data cleaning, transformations, and feature engineering.
    """
    try:
        logging.info("Starting data transformation...")

        # Initial Null Count
        null_counts = df.isnull().sum()
        logging.info(f"Initial Null Count:\n{null_counts}")

        # Remove rows with missing 'SUPPLIER' and 'ITEM TYPE'
        df.dropna(subset=['SUPPLIER', 'ITEM TYPE'], inplace=True)
        logging.info(f"Rows with missing 'SUPPLIER' or 'ITEM TYPE' dropped. Remaining rows: {len(df)}")

        # Display unique values of object columns
        for col in df.select_dtypes(include='object').columns:
            logging.info(f"Column '{col}' has unique values: {df[col].unique()[:10]}")

        # Calculate Total Sales
        df['TOTAL SALES'] = df['RETAIL SALES'] + df['RETAIL TRANSFERS'] + df['WAREHOUSE SALES']
        logging.info("Calculated 'TOTAL SALES' for all records.")

        # Remove duplicates
        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        logging.info(f"Removed {initial_rows - len(df)} duplicate rows. Remaining rows: {len(df)}")

        # Calculate Profit Margin
        df['PROFIT MARGIN'] = df.apply(
            lambda row: round(row['RETAIL SALES'] / row['TOTAL SALES'], 2) if row['TOTAL SALES'] != 0 else 0,
            axis=1
        )
        logging.info("Calculated 'PROFIT MARGIN' for all records.")
        logging.info("Data transformation completed successfully.")
        
        return df
    
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise

def load_data(df, output_path):
    """
    Load the transformed data into a CSV file.
    """
    try:
        df.to_csv(output_path, index=False)
        logging.info(f"Data successfully saved to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save data to {output_path}: {e}")
        raise

def run_etl(input_path, output_path):
    """
    Run the ETL pipeline by chaining the extract, transform, and load functions.
    """
    try:
        df = extract_data(input_path)
        transformed_df = transform_data(df)
        load_data(transformed_df, output_path)
        logging.info("ETL Process Completed Successfully!")
        print("ETL Process Completed Successfully!")
    except Exception as e:
        logging.error(f"ETL Process Failed: {e}")
        print(f"ETL Process Failed: {e}")

# Example Usage
input_path = r"C:\Users\Nalluri_Sri_varsha\OneDrive - Dell Technologies\Desktop\LLM\ETL\Warehouse_and_Retail_Sales.csv"  # or "data/*.csv" for multiple files
output_path = r"C:\Users\Nalluri_Sri_varsha\OneDrive - Dell Technologies\Desktop\LLM\ETL\transformed_data.csv"
run_etl(input_path, output_path)
