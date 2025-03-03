import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
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

def get_valid_date(target_date):
    """
    If date is in future, return most recent trading day
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

def get_nearest_trading_day(ticker_obj, target_date, direction='backward'):
    """
    Find the nearest trading day for which we have data.
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
            print(f"Warning: Error getting data for date {date_str}: {str(e)}")
            time.sleep(random.uniform(1.0, 2.0))  # Add delay between attempts
        
        # Move to next day
        if direction == 'backward':
            current_date -= timedelta(days=1)
        else:
            current_date += timedelta(days=1)
    
    return None

def get_stock_price(ticker_obj, date):
    """
    Get stock price for a specific date, handling errors gracefully
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
        print(f"Error getting price: {str(e)}")
        time.sleep(random.uniform(1.0, 2.0))  # Add delay after error
    return None

# Read the input file
input_file = "Bloomerg_M&A_data_ticker.csv"
df = pd.read_csv(input_file)

# Remove unnamed columns
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Add new columns for the results
df['T_minus_10_Date'] = None
df['T_minus_10_Price'] = None
df['T_plus_10_Date'] = None
df['T_plus_10_Price'] = None
df['Price_Status'] = None

# Process each row
for index, row in df.iterrows():
    try:
        original_ticker = row['Ticker']
        ticker_symbol = clean_ticker(original_ticker)
        announce_date = pd.to_datetime(row['Announce Date'], format='%d-%m-%Y')
        
        if pd.isna(ticker_symbol):
            print(f"Skipping missing ticker at row {index + 1}")
            df.at[index, 'Price_Status'] = 'Invalid ticker'
            continue
        
        print(f"\nProcessing {ticker_symbol}...")
        
        # Add delay between processing different tickers
        time.sleep(random.uniform(1.0, 2.0))
        
        # Create ticker object and verify it exists
        ticker = yf.Ticker(ticker_symbol)
        
        # Calculate target dates
        target_before = announce_date - timedelta(days=10)
        target_after = announce_date + timedelta(days=10)
        
        # Find nearest trading days
        actual_before = get_nearest_trading_day(ticker, target_before, 'backward')
        actual_after = get_nearest_trading_day(ticker, target_after, 'forward')
        
        if actual_before is None or actual_after is None:
            print(f"Could not find valid trading days for {ticker_symbol}")
            df.at[index, 'Price_Status'] = 'No valid trading days found'
            continue
        
        # Get prices for both dates
        price_before = get_stock_price(ticker, actual_before)
        price_after = get_stock_price(ticker, actual_after)
        
        if price_before is None or price_after is None:
            print(f"Could not get prices for {ticker_symbol}")
            df.at[index, 'Price_Status'] = 'Could not get price data'
            continue
        
        # Update the dataframe with the results
        df.at[index, 'T_minus_10_Date'] = actual_before.strftime('%Y-%m-%d')
        df.at[index, 'T_minus_10_Price'] = round(price_before, 2) if price_before is not None else None
        df.at[index, 'T_plus_10_Date'] = actual_after.strftime('%Y-%m-%d')
        df.at[index, 'T_plus_10_Price'] = round(price_after, 2) if price_after is not None else None
        df.at[index, 'Price_Status'] = 'Success'
        
        print(f"Successfully processed {ticker_symbol}")
        
        # Save progress after each successful processing
        df.to_csv(input_file, index=False)
        
    except Exception as e:
        print(f"Error processing {original_ticker}: {str(e)}")
        df.at[index, 'Price_Status'] = f'Error: {str(e)}'
        continue

print(f"\nProcessing complete!")
print(f"Successfully processed {len(df[df['Price_Status'] == 'Success'])} tickers")
print(f"Failed to process {len(df[df['Price_Status'] != 'Success'])} tickers")
print(f"\nResults have been saved back to {input_file}")
print("\nCheck the 'Price_Status' column for details on any failed tickers")