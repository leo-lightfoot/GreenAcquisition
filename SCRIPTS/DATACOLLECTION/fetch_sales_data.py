import pandas as pd
import yfinance as yf
import time
import random
import os

def clean_ticker(ticker):
    """
    Clean and format ticker symbols for Yahoo Finance.
    """
    if pd.isna(ticker):
        return ticker
        
    # Remove any whitespace
    ticker = str(ticker).strip()
    
    # Remove any $ prefix
    if ticker.startswith('$'):
        ticker = ticker[1:]
    
    # Handle specific exchanges
    if '.SW' in ticker:  # Swiss Exchange
        return ticker
    elif '.SG' in ticker:  # Stuttgart Exchange
        return ticker.replace('.SG', '.DE')  # Try German exchange
    elif '.HA' in ticker:  # Hanover Exchange
        return ticker.replace('.HA', '.DE')  # Try German exchange
        
    return ticker

def get_sales_data(ticker_obj, announce_date):
    """
    Get annual sales data for the year before the announcement date
    """
    try:
        # Convert announce_date to datetime if it's a string
        if isinstance(announce_date, str):
            announce_date = pd.to_datetime(announce_date, format='%d-%m-%Y', errors='coerce')
        
        # Get the previous year
        previous_year = announce_date.year - 1
        
        # Get annual financials
        annual_financials = ticker_obj.financials
        
        # Check if annual financials are available
        if annual_financials is None or annual_financials.empty:
            print(f"No annual financials available for {ticker_obj.ticker}")
            return None
        
        # Look for Total Revenue or similar in annual financials
        sales = None
        
        if annual_financials is not None and not annual_financials.empty:
            # Try different revenue labels that might be in the data
            revenue_labels = ['Total Revenue', 'Revenue', 'Sales']
            
            for label in revenue_labels:
                if label in annual_financials.index:
                    # Get the most recent annual revenue before the announcement date
                    for date in annual_financials.columns:
                        if date.year <= previous_year:
                            sales = annual_financials.loc[label, date]
                            break
                    
                    if sales is not None:
                        break
        
        if sales is None:
            print(f"Sales data not found for {ticker_obj.ticker} for the year {previous_year}")
        
        return sales
    except Exception as e:
        print(f"Error getting sales data: {str(e)}")
        return None

def main():
    # Load the master data file to get relevant tickers
    master_data_file = "../../DATA/3. PROCESSED/master_data_final.csv"
    output_file = "../../DATA/2. INTERIM/sales_data.csv"
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    try:
        # Load the master data to filter relevant companies
        master_df = pd.read_csv(master_data_file)
        print(f"Loaded {len(master_df)} rows from {master_data_file}")
        
        # Check if 'Acquirer Ticker' and 'Deal Announce Date' exist
        if 'Acquirer Ticker' not in master_df.columns or 'Deal Announce Date' not in master_df.columns:
            print("Error: Required columns not found in the master data file.")
            return
        
        # Initialize results list
        results = []
        
        # Process each row in the master data
        for idx, row in master_df.iterrows():
            ticker = clean_ticker(row['Acquirer Ticker'])  # Use the correct column name
            announce_date = row['Deal Announce Date']  # Use the correct column name
            
            if pd.notna(ticker):
                print(f"Processing {idx+1}/{len(master_df)}: {ticker}")
                
                try:
                    ticker_obj = yf.Ticker(ticker)
                    sales = get_sales_data(ticker_obj, announce_date)
                    
                    results.append({
                        'Acquirer Ticker': row['Acquirer Ticker'],  # Use the correct column name
                        'Deal Announce Date': announce_date,
                        'Annual_Sales': sales
                    })
                    
                    time.sleep(random.uniform(1.0, 2.0))
                    
                except Exception as e:
                    print(f"Error processing {ticker}: {str(e)}")
                    results.append({
                        'Acquirer Ticker': row['Acquirer Ticker'],  # Use the correct column name
                        'Deal Announce Date': announce_date,
                        'Annual_Sales': None
                    })
            
            if (idx + 1) % 10 == 0:
                print(f"Processed {idx+1}/{len(master_df)} companies")
        
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_file, index=False)
        print(f"Saved sales data for {len(results_df)} companies to {output_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 