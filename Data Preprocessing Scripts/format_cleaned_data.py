import pandas as pd
import numpy as np

def format_cleaned_data():
    print("Starting data formatting process...")
    
    # Load the dual classified data file
    input_file = "master_data_dual_classified.csv"
    try:
        print(f"Loading {input_file}...")
        df = pd.read_csv(input_file)
        print(f"Number of rows: {len(df)}")
    except FileNotFoundError:
        print(f"Error: Could not find {input_file}")
        return
    except Exception as e:
        print(f"Error loading file: {str(e)}")
        return

    # 1. Format dates to YYYY-MM-DD
    date_columns = ['Deal Announce Date', 'T_minus_10_Date', 'T_plus_10_Date']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # 2. Format numeric columns
    # Round prices to 2 decimal places
    price_columns = ['T_minus_10_Price', 'T_plus_10_Price']
    for col in price_columns:
        df[col] = df[col].round(2)
    
    # Round Market Cap to 2 decimal places
    df['Market_Cap_AD_mil'] = df['Market_Cap_AD_mil'].round(2)
    
    # Round ROA to 2 decimal places
    df['ROA_percent'] = df['ROA_percent'].round(2)
    
    # Round Announced Total Value to 2 decimal places
    df['Announced Total Value (mil.)'] = pd.to_numeric(df['Announced Total Value (mil.)'], errors='coerce').round(2)
    
    # Format GHG Emissions to 2 decimal places
    df['Acquirer_GHG_Emissions'] = df['Acquirer_GHG_Emissions'].round(2)
    
    # 3. String formatting
    # Remove leading/trailing spaces from string columns
    string_columns = ['Acquirer Ticker', 'Acquirer Name', 'Target Name', 'Seller Name', 'Deal Status']
    for col in string_columns:
        df[col] = df[col].str.strip()
    
    # Ensure Deal Status is consistently capitalized
    df['Deal Status'] = df['Deal Status'].str.capitalize()
    
    # 4. Ensure Previous Year is integer
    df['Previous Year'] = df['Previous Year'].astype(int)
    
    # 5. Select and reorder columns
    column_order = [
        'Deal Announce Date', 'Previous Year', 'Acquirer Ticker', 'Acquirer Name',
        'Acquirer_GHG_Emissions', 'Carbon_Intensity', 'Acquirer_Classification',
        'Target Name', 'Target_Classification', 'Seller Name',
        'Announced Total Value (mil.)', 'T_minus_10_Date', 'T_minus_10_Price',
        'T_plus_10_Date', 'T_plus_10_Price', 'Market_Cap_AD_mil', 'ROA_percent',
        'Deal Status'
    ]
    
    # Keep only the specified columns
    df = df[column_order]
    
    # Save the formatted dataset
    output_file = "master_data_formatted.csv"
    df.to_csv(output_file, index=False)
    
    # Print summary of the changes
    print(f"\n✅ Formatting complete! Formatted data saved to {output_file}")
    print("\nSample of formatted data:")
    print(df.head().to_string())
    
    # Print data types summary
    print("\nColumn data types:")
    for column in df.columns:
        print(f"{column}: {df[column].dtype}")
    
    # Print value ranges for numeric columns
    print("\nNumeric columns ranges:")
    numeric_columns = ['T_minus_10_Price', 'T_plus_10_Price', 'Market_Cap_AD_mil', 
                      'ROA_percent', 'Acquirer_GHG_Emissions', 'Announced Total Value (mil.)', 'Carbon_Intensity']
    for col in numeric_columns:
        print(f"{col}:")
        print(f"  Min: {df[col].min():.2f}")
        print(f"  Max: {df[col].max():.2f}")
        print(f"  Mean: {df[col].mean():.2f}")

if __name__ == "__main__":
    format_cleaned_data() 