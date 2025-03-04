import pandas as pd
import numpy as np
import logging
from pathlib import Path
import sys

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent.parent))
from config import MASTER_DATA_RAW, MASTER_DATA_CLEAN

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def clean_and_format_data():
    """
    Combined function to clean and format the master data.
    """
    print("Starting data cleaning and formatting process...")
    
    try:
        # Load the master data file
        logging.info(f"Loading {MASTER_DATA_RAW}...")
        df = pd.read_csv(MASTER_DATA_RAW)
        initial_rows = len(df)
        logging.info(f"Initial number of rows: {initial_rows}")
        
        # 1. Remove duplicates
        df_no_duplicates = df.drop_duplicates()
        duplicates_removed = initial_rows - len(df_no_duplicates)
        logging.info(f"Duplicate rows removed: {duplicates_removed}")
        
        # 2. Clean and format required columns
        required_columns = [
            'Deal Announce Date',
            'T_minus_10_Date',
            'T_minus_10_Price',
            'T_plus_10_Date',
            'T_plus_10_Price',
            'Market_Cap_AD_mil',
            'ROA_percent',
            'Acquirer_GHG_Emissions'
        ]
        
        # Log missing values before cleaning
        logging.info("\nMissing values before cleaning:")
        for col in required_columns:
            missing = df_no_duplicates[col].isna().sum()
            logging.info(f"{col}: {missing:,} missing values")
        
        # 3. Format dates to YYYY-MM-DD
        date_columns = ['Deal Announce Date', 'T_minus_10_Date', 'T_plus_10_Date']
        for col in date_columns:
            df_no_duplicates[col] = pd.to_datetime(df_no_duplicates[col]).dt.strftime('%Y-%m-%d')
        
        # 4. Format numeric columns
        # Round prices and financial metrics to 2 decimal places
        numeric_columns = [
            'T_minus_10_Price', 
            'T_plus_10_Price',
            'Market_Cap_AD_mil',
            'ROA_percent',
            'Announced Total Value (mil.)',
            'Acquirer_GHG_Emissions'
        ]
        
        for col in numeric_columns:
            if col in df_no_duplicates.columns:
                df_no_duplicates[col] = pd.to_numeric(df_no_duplicates[col], errors='coerce').round(2)
        
        # 5. Clean string columns
        string_columns = ['Acquirer Ticker', 'Acquirer Name', 'Target Name', 'Deal Status']
        for col in string_columns:
            if col in df_no_duplicates.columns:
                df_no_duplicates[col] = df_no_duplicates[col].str.strip()
        
        # 6. Standardize Deal Status
        if 'Deal Status' in df_no_duplicates.columns:
            df_no_duplicates['Deal Status'] = df_no_duplicates['Deal Status'].str.capitalize()
        
        # 7. Remove rows with missing required values
        df_cleaned = df_no_duplicates.dropna(subset=required_columns)
        rows_removed = len(df_no_duplicates) - len(df_cleaned)
        logging.info(f"\nRows removed due to missing required values: {rows_removed:,}")
        
        # Save the cleaned and formatted dataset
        df_cleaned.to_csv(MASTER_DATA_CLEAN, index=False)
        
        # Print summary statistics
        logging.info(f"\n✅ Cleaning and formatting complete! Saved to {MASTER_DATA_CLEAN}")
        logging.info(f"Final number of rows: {len(df_cleaned):,}")
        
        # Print completeness summary
        logging.info("\nData completeness summary:")
        for column in df_cleaned.columns:
            missing = df_cleaned[column].isna().sum()
            complete = len(df_cleaned) - missing
            percent = (complete/len(df_cleaned))*100
            logging.info(f"{column}: {complete:,} records ({percent:.1f}% complete)")
            
    except FileNotFoundError:
        logging.error(f"Error: Could not find {MASTER_DATA_RAW}")
        raise
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        raise

if __name__ == "__main__":
    clean_and_format_data() 