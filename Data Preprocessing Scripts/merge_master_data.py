import pandas as pd

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

def main():
    print("Starting data merge process...")
    
    # Load all data files
    deals_file = "Bloomerg_M&A_data_ticker.csv"
    financials_file = "financial_metrics.csv"
    ghg_file = "GHG.csv"
    
    try:
        print(f"Loading {deals_file}...")
        deals_data = pd.read_csv(deals_file)
        print(f"Loading {financials_file}...")
        financials_data = pd.read_csv(financials_file)
        print(f"Loading {ghg_file}...")
        ghg_data = pd.read_csv(ghg_file)
    except FileNotFoundError as e:
        print(f"Error: Could not find one of the input files - {str(e)}")
        return
    except Exception as e:
        print(f"Error loading files: {str(e)}")
        return
    
    print("Cleaning and standardizing data...")
    # Clean all dataframes
    deals_data = clean_dataframe(deals_data)
    financials_data = clean_dataframe(financials_data)
    ghg_data = clean_dataframe(ghg_data)
    
    # Rename columns to match desired output
    deals_data = deals_data.rename(columns={
        'Announce Date': 'Deal Announce Date',
        'Ticker': 'Acquirer Ticker'
    })
    financials_data = financials_data.rename(columns={
        'Announce Date': 'Deal Announce Date',
        'Ticker': 'Acquirer Ticker'
    })
    ghg_data = ghg_data.rename(columns={
        'Ticker': 'Acquirer Ticker'
    })
    
    print("Merging deals data with financial metrics...")
    # First merge: Deals data with Financial metrics
    merged_data = pd.merge(
        deals_data,
        financials_data[['Acquirer Ticker', 'Deal Announce Date', 'Market_Cap_mil', 'Debt_to_Equity', 'ROA_percent']],
        on=['Acquirer Ticker', 'Deal Announce Date'],
        how='left'
    )
    
    print("Adding GHG emissions data...")
    # Second merge: Add GHG emissions data
    final_data = process_ghg_data(ghg_data, merged_data)
    
    # Rename Market Cap column for clarity
    final_data = final_data.rename(columns={'Market_Cap_mil': 'Market_Cap_AD_mil'})
    
    # Arrange columns in specified order
    column_order = [
        'Deal Announce Date',
        'Previous Year',
        'Acquirer Ticker',
        'Acquirer Name',
        'Target Name',
        'Seller Name',
        'Announced Total Value (mil.)',
        'TV/EBITDA',
        'T_minus_10_Date',
        'T_minus_10_Price',
        'T_plus_10_Date',
        'T_plus_10_Price',
        'Market_Cap_AD_mil',
        'ROA_percent',
        'Acquirer_GHG_Emissions',
        'Deal Status'
    ]
    
    # Create missing columns if they don't exist
    for col in column_order:
        if col not in final_data.columns:
            final_data[col] = None
    
    # Select and order columns
    final_data = final_data[column_order]
    
    # Save the final merged dataset
    output_file = "master_data.csv"
    final_data.to_csv(output_file, index=False)
    
    print(f"\n✅ Master data creation complete! Saved to {output_file}")
    print(f"Total records: {len(final_data)}")
    print("\nFirst few rows of the master data:")
    print(final_data.head().to_string())
    
    # Print data completeness summary
    print("\nData completeness summary:")
    for column in final_data.columns:
        missing = final_data[column].isna().sum()
        complete = len(final_data) - missing
        percent = (complete/len(final_data))*100
        print(f"{column}: {complete:,} records ({percent:.1f}% complete)")

if __name__ == "__main__":
    main() 