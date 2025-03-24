import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
import time
import random
import os
import logging
import concurrent.futures
from tqdm import tqdm
import sys
import argparse  # Added for command line arguments

# Setup logging
def setup_logging():
    """Setup logging to both file and console."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"{log_dir}/fetch_stock_prices.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler()  # Also log to console
        ]
    )

def clean_ticker(ticker):
    """
    Clean and format ticker symbols for Yahoo Finance.
    
    Args:
        ticker (str): The ticker symbol to clean
        
    Returns:
        str: The cleaned ticker symbol
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

def get_valid_date(target_date):
    """
    If date is in future, return most recent trading day.
    
    Args:
        target_date (datetime): The target date to validate
        
    Returns:
        datetime: A valid date not in the future
    """
    now = datetime.now()
    if target_date > now:
        # Use the most recent weekday
        current_date = now
        while current_date.weekday() > 4:  # 5 = Saturday, 6 = Sunday
            current_date -= timedelta(days=1)
        return current_date
    return target_date

def get_stock_data_with_retry(ticker_obj, start_date, end_date, max_retries=3):
    """
    Get stock data with retries and exponential backoff.
    
    Args:
        ticker_obj (yfinance.Ticker): Ticker object
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        pandas.DataFrame: Historical stock data or None if failed
    """
    for attempt in range(max_retries):
        try:
            # More conservative delay to avoid rate limiting
            time.sleep(1.0 + random.random())  # Base 1 second + random delay
            return ticker_obj.history(start=start_date, end=end_date, interval='1d')
        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                logging.warning(f"Error after {max_retries} attempts: {str(e)}")
                return None
            # More aggressive backoff for retries
            backoff_time = 2.0 * (2 ** attempt) + random.random() * 2
            logging.info(f"Backing off for {backoff_time:.2f} seconds after attempt {attempt+1}")
            time.sleep(backoff_time)
    return None

def get_nearest_trading_day(ticker_obj, target_date, direction='backward'):
    """
    Find the nearest trading day for which we have data.
    
    Args:
        ticker_obj (yfinance.Ticker): The ticker object
        target_date (datetime): The target date
        direction (str): Search direction ('backward' or 'forward')
        
    Returns:
        datetime: The nearest trading day or None if not found
    """
    max_attempts = 10  # Increased from 5
    current_date = get_valid_date(target_date)
    
    for i in range(max_attempts):
        try:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Get data for a small window
            if direction == 'backward':
                start_date = (current_date - timedelta(days=5)).strftime('%Y-%m-%d')
                end_date = date_str
            else:
                start_date = date_str
                end_date = (current_date + timedelta(days=5)).strftime('%Y-%m-%d')
            
            # Try to get historical data with retry mechanism
            hist = get_stock_data_with_retry(ticker_obj, start_date, end_date)
            
            if hist is not None and not hist.empty:
                if direction == 'backward':
                    return hist.index[-1].to_pydatetime()
                else:
                    return hist.index[0].to_pydatetime()
            
        except Exception as e:
            logging.warning(f"Warning: Error getting data for date {date_str}: {str(e)}")
            # Add more delay between retry attempts
            time.sleep(1.5 + random.random())
        
        # Move to next day
        if direction == 'backward':
            current_date -= timedelta(days=1)
        else:
            current_date += timedelta(days=1)
    
    return None

def get_stock_price(ticker_obj, date):
    """
    Get stock price for a specific date, handling errors gracefully.
    
    Args:
        ticker_obj (yfinance.Ticker): The ticker object
        date (datetime): The date to get the price for
        
    Returns:
        float: The stock price or None if not available
    """
    try:
        # Try to get data for a 3-day window around the date
        start_date = (date - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Use retry mechanism for getting historical data
        hist = get_stock_data_with_retry(ticker_obj, start_date, end_date)
        
        if hist is not None and not hist.empty:
            # Get the closest date's closing price
            date_str = date.strftime('%Y-%m-%d')
            if date_str in hist.index:
                return hist.loc[date_str]['Close']
            else:
                # Get the closest available date
                return hist['Close'].iloc[-1]
                
    except Exception as e:
        logging.warning(f"Error getting price: {str(e)}")
        # Add more delay after error
        time.sleep(1.0 + random.random())
    return None

def process_company(row_data, days_before, days_after):
    """
    Process a single company to get stock prices.
    
    Args:
        row_data (tuple): Tuple containing (index, row)
        days_before (int): Number of days before announcement to fetch price
        days_after (int): Number of days after announcement to fetch price
        
    Returns:
        dict: Dictionary with results for this company
    """
    index, row = row_data
    
    try:
        ticker_symbol = clean_ticker(row['Ticker'])
        announce_date_dt = row.get('Announce_Date')
        
        # Check for valid ticker and announcement date
        if pd.isna(ticker_symbol) or ticker_symbol == "Ticker not found":
            return {
                'index': index,
                'status': 'Invalid ticker',
                'ticker': ticker_symbol,
                'company': row.get('Acquirer Name', 'Unknown')
            }
        
        if pd.isna(announce_date_dt):
            return {
                'index': index, 
                'status': 'Missing announcement date',
                'ticker': ticker_symbol,
                'company': row.get('Acquirer Name', 'Unknown')
            }
            
        # Convert string date to datetime if needed
        if isinstance(announce_date_dt, str):
            try:
                announce_date = pd.to_datetime(announce_date_dt)
            except:
                return {
                    'index': index, 
                    'status': 'Invalid date format',
                    'ticker': ticker_symbol,
                    'company': row.get('Acquirer Name', 'Unknown')
                }
        else:
            announce_date = announce_date_dt
        
        # Create ticker object
        ticker = yf.Ticker(ticker_symbol)
        
        # Calculate target dates using the configurable day parameters
        target_before = announce_date - timedelta(days=days_before)
        target_after = announce_date + timedelta(days=days_after)
        
        # Find nearest trading days
        actual_before = get_nearest_trading_day(ticker, target_before, 'backward')
        actual_after = get_nearest_trading_day(ticker, target_after, 'forward')
        
        if actual_before is None or actual_after is None:
            return {
                'index': index, 
                'status': 'No valid trading days found',
                'ticker': ticker_symbol,
                'company': row.get('Acquirer Name', 'Unknown')
            }
        
        # Get prices for both dates
        price_before = get_stock_price(ticker, actual_before)
        price_after = get_stock_price(ticker, actual_after)
        
        if price_before is None or price_after is None:
            return {
                'index': index, 
                'status': 'Could not get price data',
                'ticker': ticker_symbol,
                'company': row.get('Acquirer Name', 'Unknown')
            }
        
        # Calculate returns
        if price_before > 0:
            percent_return = ((price_after - price_before) / price_before) * 100
        else:
            percent_return = None
            
        # Return the results with T-n and T+n naming convention
        return {
            'index': index,
            'status': 'Success',
            'ticker': ticker_symbol,
            'company': row.get('Acquirer Name', 'Unknown'),
            f'T_minus_{days_before}_Date': actual_before.strftime('%Y-%m-%d'),
            f'T_minus_{days_before}_Price': round(price_before, 2) if price_before is not None else None,
            f'T_plus_{days_after}_Date': actual_after.strftime('%Y-%m-%d'),
            f'T_plus_{days_after}_Price': round(price_after, 2) if price_after is not None else None,
            'Percent_Return': round(percent_return, 2) if percent_return is not None else None
        }
    except Exception as e:
        return {
            'index': index,
            'status': f'Error: {str(e)}',
            'ticker': row.get('Ticker', 'Unknown'),
            'company': row.get('Acquirer Name', 'Unknown')
        }

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Fetch stock prices around announcement dates.')
    parser.add_argument('--days-before', type=int, default=10,
                       help='Days before announcement date (default: 10)')
    parser.add_argument('--days-after', type=int, default=10,
                       help='Days after announcement date (default: 10)')
    parser.add_argument('--workers', type=int, default=2,
                       help='Number of parallel workers (default: 2)')
    return parser.parse_args()

def get_missing_tickers(existing_df, new_df, days_before, days_after):
    """Get tickers that need price data fetched."""
    missing_tickers = []
    
    for _, row in new_df.iterrows():
        ticker = row['Ticker']
        # Check if ticker exists and has price data for specified days
        if ticker not in existing_df['Ticker'].values:
            missing_tickers.append(row)
        else:
            existing_row = existing_df[existing_df['Ticker'] == ticker].iloc[0]
            price_cols = [f'T_minus_{days_before}_Price', f'T_plus_{days_after}_Price']
            if any(pd.isna(existing_row.get(col)) for col in price_cols):
                missing_tickers.append(row)
                
    return pd.DataFrame(missing_tickers)

def main():
    """Main function to fetch stock prices for event study analysis."""
    args = parse_arguments()
    
    input_file = "data/2_interim/master_data_classified.csv"
    output_file = f"data/2_interim/master_data_with_stock_prices_{args.days_after}day.csv"
    
    days_before = args.days_before
    days_after = args.days_after
    
    try:
        # Load new data
        df = pd.read_csv(input_file)
        
        # Check if output file exists
        if os.path.exists(output_file):
            existing_df = pd.read_csv(output_file)
            df_to_process = get_missing_tickers(existing_df, df, days_before, days_after)
            if len(df_to_process) == 0:
                print("No new data to process.")
                return
            base_df = existing_df
        else:
            df_to_process = df
            base_df = df.copy()
        
        # Process companies
        companies_to_process = list(df_to_process.reset_index().iterrows())
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_company, row_data, days_before, days_after): 
                      row_data[0] for row_data in companies_to_process}
            
            for future in tqdm(concurrent.futures.as_completed(futures), 
                             total=len(futures),
                             desc=f"Fetching T-{days_before}/T+{days_after} stock prices"):
                result = future.result()
                results.append(result)
        
        # Update base DataFrame with new results
        for res in results:
            idx = res['index']
            for key, value in res.items():
                if key != 'index' and key != 'company':
                    base_df.loc[idx, key] = value
        
        # Save final results
        base_df.to_csv(output_file, index=False)
        print(f"Completed processing {len(results)} companies.")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()