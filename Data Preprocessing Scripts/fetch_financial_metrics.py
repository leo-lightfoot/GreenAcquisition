import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import random

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
    
    # Handle ISIN codes (they usually start with 2 letters followed by numbers)
    if len(ticker) > 12 and '.' in ticker:
        base, exchange = ticker.split('.')
        if any(exchange.endswith(suffix) for suffix in ['SG', 'HA']):
            # For German exchanges, try the main German exchange
            return f"{base}.DE"
    
    # Handle specific exchanges
    if '.SW' in ticker:  # Swiss Exchange
        return ticker
    elif '.SG' in ticker:  # Stuttgart Exchange
        return ticker.replace('.SG', '.DE')  # Try German exchange
    elif '.HA' in ticker:  # Hanover Exchange
        return ticker.replace('.HA', '.DE')  # Try German exchange
        
    return ticker

def get_stock_data_with_retry(ticker_obj, start_date, end_date, max_retries=3):
    """
    Get stock data with retries and exponential backoff
    """
    for attempt in range(max_retries):
        try:
            # Add a small random delay to avoid hitting rate limits
            time.sleep(random.uniform(1.0, 2.0))
            return ticker_obj.history(start=start_date, end=end_date, interval='1d')
        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                print(f"Error after {max_retries} attempts: {str(e)}")
                return None
            # Exponential backoff
            time.sleep((2 ** attempt) + random.uniform(0.1, 0.5))
    return None

def get_financial_metrics(ticker_obj, announce_date):
    """
    Get financial metrics for a company at the time closest to announce_date
    Returns: market_cap, debt_to_equity, roa
    """
    try:
        # Add delay to avoid rate limiting
        time.sleep(random.uniform(1.0, 2.0))
        
        # Get market cap from historical data
        hist = get_stock_data_with_retry(ticker_obj, 
                                       announce_date.strftime('%Y-%m-%d'),
                                       (announce_date + timedelta(days=1)).strftime('%Y-%m-%d'))
        
        market_cap = None
        if hist is not None and not hist.empty:
            # Calculate market cap using shares outstanding and price
            shares = ticker_obj.info.get('sharesOutstanding')
            if shares and 'Close' in hist:
                price = hist['Close'].iloc[0]
                market_cap = (shares * price) / 1_000_000  # Convert to millions
        
        # Get financial ratios
        time.sleep(random.uniform(1.0, 2.0))  # Add delay before fetching financials
        
        # Get quarterly financials
        financials = ticker_obj.quarterly_financials
        balance_sheet = ticker_obj.quarterly_balance_sheet
        
        # Calculate Debt-to-Equity
        debt_to_equity = None
        if not balance_sheet.empty:
            try:
                total_debt = balance_sheet.loc['Total Debt'].iloc[0]
                total_equity = balance_sheet.loc['Total Stockholder Equity'].iloc[0]
                if total_equity != 0:
                    debt_to_equity = round(total_debt / total_equity, 2)
            except (KeyError, IndexError):
                pass
        
        # Calculate ROA
        roa = None
        if not financials.empty and not balance_sheet.empty:
            try:
                net_income = financials.loc['Net Income'].iloc[0]
                total_assets = balance_sheet.loc['Total Assets'].iloc[0]
                if total_assets != 0:
                    roa = round((net_income / total_assets) * 100, 2)  # Convert to percentage
            except (KeyError, IndexError):
                pass
        
        return market_cap, debt_to_equity, roa
        
    except Exception as e:
        print(f"Error getting financial metrics: {str(e)}")
        return None, None, None

def main():
    # Read the existing data file
    input_file = "Bloomerg_M&A_data_ticker.csv"
    output_file = "financial_metrics.csv"
    
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: Could not find input file {input_file}")
        return
    
    # Create new dataframe with only the columns we need
    metrics_df = pd.DataFrame({
        'Ticker': df['Ticker'],
        'Acquirer Name': df['Acquirer Name'],
        'Announce Date': df['Announce Date']
    })
    
    # Add new columns for the financial metrics
    metrics_df['Market_Cap_mil'] = None
    metrics_df['Debt_to_Equity'] = None
    metrics_df['ROA_percent'] = None
    metrics_df['Status'] = None
    
    # Process each row
    for index, row in metrics_df.iterrows():
        try:
            original_ticker = row['Ticker']
            ticker_symbol = clean_ticker(original_ticker)
            announce_date = pd.to_datetime(row['Announce Date'], format='%d-%m-%Y')
            
            if pd.isna(ticker_symbol):
                print(f"Skipping missing ticker at row {index + 1}")
                metrics_df.at[index, 'Status'] = 'Invalid ticker'
                continue
            
            print(f"\nProcessing {ticker_symbol}...")
            
            # Add delay between processing different tickers
            time.sleep(random.uniform(1.0, 2.0))
            
            # Create ticker object and verify it exists
            ticker = yf.Ticker(ticker_symbol)
            
            # Get financial metrics
            market_cap, debt_to_equity, roa = get_financial_metrics(ticker, announce_date)
            
            # Update the dataframe with the results
            metrics_df.at[index, 'Market_Cap_mil'] = round(market_cap, 2) if market_cap is not None else None
            metrics_df.at[index, 'Debt_to_Equity'] = debt_to_equity
            metrics_df.at[index, 'ROA_percent'] = roa
            metrics_df.at[index, 'Status'] = 'Success'
            
            print(f"Successfully processed {ticker_symbol}")
            
            # Save progress after each successful processing
            metrics_df.to_csv(output_file, index=False)
            
        except Exception as e:
            print(f"Error processing {original_ticker}: {str(e)}")
            metrics_df.at[index, 'Status'] = f'Error: {str(e)}'
            continue
    
    print(f"\nProcessing complete!")
    print(f"Successfully processed {len(metrics_df[metrics_df['Status'] == 'Success'])} tickers")
    print(f"Failed to process {len(metrics_df[metrics_df['Status'] != 'Success'])} tickers")
    print(f"\nResults have been saved to {output_file}")
    print("\nCheck the 'Status' column for details on any failed tickers")

if __name__ == "__main__":
    main() 