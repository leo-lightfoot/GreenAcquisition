import pandas as pd
import numpy as np

def clean_master_data():
    print("Starting data cleaning process...")
    
    # Load the master data file
    input_file = "master_data.csv"
    try:
        print(f"Loading {input_file}...")
        df = pd.read_csv(input_file)
        print(f"Initial number of rows: {len(df)}")
    except FileNotFoundError:
        print(f"Error: Could not find {input_file}")
        return
    except Exception as e:
        print(f"Error loading file: {str(e)}")
        return

    # 1. Remove completely duplicate rows
    df_no_duplicates = df.drop_duplicates()
    duplicates_removed = len(df) - len(df_no_duplicates)
    print(f"\nDuplicate rows removed: {duplicates_removed}")
    
    # 2. Remove rows with missing values in specified columns
    required_columns = [
        'T_minus_10_Date',
        'T_minus_10_Price',
        'T_plus_10_Date',
        'T_plus_10_Price',
        'Market_Cap_AD_mil',
        'ROA_percent',
        'Acquirer_GHG_Emissions'
    ]
    
    # Count missing values per column before cleaning
    print("\nMissing values per column before cleaning:")
    for col in required_columns:
        missing = df_no_duplicates[col].isna().sum()
        print(f"{col}: {missing:,} missing values")
    
    # Remove rows where any of the required columns are missing
    df_cleaned = df_no_duplicates.dropna(subset=required_columns)
    
    # Calculate how many rows were removed
    rows_removed = len(df_no_duplicates) - len(df_cleaned)
    print(f"\nRows removed due to missing required values: {rows_removed:,}")
    print(f"Final number of rows: {len(df_cleaned):,}")
    
    # Save the cleaned dataset
    output_file = "master_data_cleaned.csv"
    df_cleaned.to_csv(output_file, index=False)
    
    # Print summary of the cleaned data
    print(f"\n✅ Cleaning complete! Cleaned data saved to {output_file}")
    print("\nFirst few rows of the cleaned data:")
    print(df_cleaned.head().to_string())
    
    # Print completeness summary for all columns
    print("\nData completeness summary for cleaned dataset:")
    for column in df_cleaned.columns:
        missing = df_cleaned[column].isna().sum()
        complete = len(df_cleaned) - missing
        percent = (complete/len(df_cleaned))*100
        print(f"{column}: {complete:,} records ({percent:.1f}% complete)")

if __name__ == "__main__":
    clean_master_data() 