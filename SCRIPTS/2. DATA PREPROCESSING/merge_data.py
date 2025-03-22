import pandas as pd
import os

def clean_dataframe(df):
    """
    Standardize dataframe by cleaning column names and ticker format
    """
    # Strip spaces from column names
    df.columns = df.columns.str.strip()
    
    # Convert 'Announce Date' to datetime if it exists
    if 'Announce Date' in df.columns:
        df['Announce Date'] = pd.to_datetime(df['Announce Date'], format='%d-%m-%Y', errors='coerce')
    
    # Convert 'periodenddate' to datetime if it exists
    if 'periodenddate' in df.columns:
        df['periodenddate'] = pd.to_datetime(df['periodenddate'], format='%d-%m-%Y', errors='coerce')
    
    # Standardize ticker format if it exists
    if 'Ticker' in df.columns:
        df['Ticker'] = df['Ticker'].str.strip().str.upper()
        # Remove rows where Ticker is empty
        df = df.dropna(subset=['Ticker'])
    
    return df

def process_ghg_data(ghg_data, deals_data):
    """
    Process GHG emissions data and merge with deals data
    """
    # Drop rows with empty tickers
    ghg_data = ghg_data.dropna(subset=['Acquirer Ticker'])
    
    # Extract year from periodenddate
    ghg_data['Reporting Year'] = ghg_data['periodenddate'].dt.year
    
    # Calculate previous year for deals data
    deals_data['Previous Year'] = deals_data['Deal Announce Date'].dt.year - 1
    
    # Group by 'Ticker' and 'Reporting Year' to ensure one value per company per year
    ghg_data = ghg_data.groupby(['Acquirer Ticker', 'Reporting Year'], as_index=False).agg({
        'GHG_Emissions': 'mean'  # Using mean instead of sum for multiple entries
    })
    
    # Merge GHG emissions into deals data
    merged_data = pd.merge(
        deals_data,
        ghg_data,
        left_on=['Acquirer Ticker', 'Previous Year'],
        right_on=['Acquirer Ticker', 'Reporting Year'],
        how='left'
    )
    
    # Rename column for clarity
    merged_data = merged_data.rename(columns={'GHG_Emissions': 'Acquirer_GHG_Emissions'})
    
    # Drop temporary columns
    merged_data.drop(columns=['Reporting Year'], inplace=True, errors='ignore')
    
    return merged_data

def process_bloomberg_sales_data(bbg_sales_data, deals_data):
    """
    Process Bloomberg sales data and merge with deals data
    """
    # Rename columns for consistency
    bbg_sales_data = bbg_sales_data.rename(columns={
        'List of Tickers': 'Acquirer Ticker',
        'Sales in Mn. Dollars': 'Annual_Sales'
    })
    
    # Convert year to integer if it's not
    if bbg_sales_data['Year'].dtype != 'int64':
        bbg_sales_data['Year'] = bbg_sales_data['Year'].astype(int)
    
    # Calculate previous year for deals data
    deals_data['Previous Year'] = deals_data['Deal Announce Date'].dt.year - 1
    
    # Merge Sales data into deals data
    merged_data = pd.merge(
        deals_data,
        bbg_sales_data,
        left_on=['Acquirer Ticker', 'Previous Year'],
        right_on=['Acquirer Ticker', 'Year'],
        how='left'
    )
    
    # Drop temporary columns
    merged_data.drop(columns=['Year'], inplace=True, errors='ignore')
    
    return merged_data

def main():
    # Define file paths
    ma_data_path = "../../DATA/1. RAW DATA/Bloomberg_MA_Data.csv"
    ghg_data_path = "../../DATA/1. RAW DATA/GHG.csv"
    tickers_path = "../../DATA/2. INTERIM/company_tickers.csv"
    financial_metrics_path = "../../DATA/2. INTERIM/financial_metrics.csv"
    bbg_sales_data_path = "../../DATA/1. RAW DATA/Sales_data_BBG.csv"
    output_path = "../../DATA/2. INTERIM/master_data_formatted.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    try:
        # Load datasets
        print("Loading datasets...")
        ma_data = pd.read_csv(ma_data_path)
        ghg_data = pd.read_csv(ghg_data_path)
        tickers = pd.read_csv(tickers_path)
        financial_metrics = pd.read_csv(financial_metrics_path)
        
        # Load Bloomberg sales data
        bbg_sales_data = None
        if os.path.exists(bbg_sales_data_path):
            bbg_sales_data = pd.read_csv(bbg_sales_data_path)
            print(f"Loaded Bloomberg sales data with {len(bbg_sales_data)} rows")
        else:
            print("Bloomberg sales data file not found. Proceeding without sales data.")
        
        # Clean datasets
        print("Cleaning datasets...")
        ma_data = clean_dataframe(ma_data)
        ghg_data = clean_dataframe(ghg_data)
        tickers = clean_dataframe(tickers)
        financial_metrics = clean_dataframe(financial_metrics)
        if bbg_sales_data is not None:
            bbg_sales_data = clean_dataframe(bbg_sales_data)
        
        # Merge M&A data with tickers
        print("Merging M&A data with tickers...")
        merged_data = pd.merge(
            ma_data,
            tickers,
            on=['Deal Number'],
            how='inner'
        )
        
        # Merge with financial metrics
        print("Merging with financial metrics...")
        merged_data = pd.merge(
            merged_data,
            financial_metrics,
            on=['Acquirer Ticker', 'Deal Announce Date'],
            how='left'
        )
        
        # Merge with Bloomberg sales data if available
        if bbg_sales_data is not None:
            print("Merging with Bloomberg sales data...")
            merged_data = process_bloomberg_sales_data(bbg_sales_data, merged_data)
        
        # Process GHG data and merge
        print("Processing GHG data and merging...")
        merged_data = process_ghg_data(ghg_data, merged_data)
        
        # Rename GHG emissions column for clarity
        merged_data = merged_data.rename(columns={'GHG_Emissions': 'Acquirer_GHG_Emissions'})
        
        # Save the merged dataset
        print(f"Saving merged dataset with {len(merged_data)} rows...")
        merged_data.to_csv(output_path, index=False)
        
        print(f"✅ Data merging complete! Output saved to {output_path}")
        
        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"Original M&A data: {len(ma_data)} rows")
        print(f"Tickers data: {len(tickers)} rows")
        print(f"Financial metrics: {len(financial_metrics)} rows")
        if bbg_sales_data is not None:
            print(f"Bloomberg sales data: {len(bbg_sales_data)} rows")
        print(f"GHG emissions data: {len(ghg_data)} rows")
        print(f"Final merged dataset: {len(merged_data)} rows")
        
        # Print column information
        print("\nColumns in merged dataset:")
        for col in merged_data.columns:
            non_null = merged_data[col].count()
            percent = (non_null / len(merged_data)) * 100
            print(f"{col}: {non_null} non-null values ({percent:.1f}%)")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 