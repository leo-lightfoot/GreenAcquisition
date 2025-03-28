import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
import time
import random
import os
import concurrent.futures
from tqdm import tqdm
import sys
import argparse

def clean_ticker(ticker):
    # Cleans and formats ticker symbols for Yahoo Finance
    if pd.isna(ticker):
        return ticker
        
    ticker = str(ticker).strip()
    
    if ticker.startswith('$'):
        ticker = ticker[1:]
    
    if len(ticker) > 12 and '.' in ticker:
        base, exchange = ticker.split('.')
        if any(exchange.endswith(suffix) for suffix in ['SG', 'HA']):
            return f"{base}.DE"
    
    if '.SW' in ticker:
        return ticker
    elif '.SG' in ticker:
        return ticker.replace('.SG', '.DE')
    elif '.HA' in ticker:
        return ticker.replace('.HA', '.DE')
        
    return ticker

def get_valid_date(target_date):
    # Returns most recent trading day if date is in future
    now = datetime.now()
    if target_date > now:
        current_date = now
        while current_date.weekday() > 4:
            current_date -= timedelta(days=1)
        return current_date
    return target_date

def get_stock_data_with_retry(ticker_obj, start_date, end_date, max_retries=3):
    # Gets stock data with retries and exponential backoff
    for attempt in range(max_retries):
        try:
            time.sleep(1.0 + random.random())
            return ticker_obj.history(start=start_date, end=end_date, interval='1d')
        except Exception as e:
            if attempt == max_retries - 1:
                return None
            backoff_time = 2.0 * (2 ** attempt) + random.random() * 2
            time.sleep(backoff_time)
    return None

def get_nearest_trading_day(ticker_obj, target_date, direction='backward'):
    # Finds nearest trading day with available data
    max_attempts = 10
    current_date = get_valid_date(target_date)
    
    for i in range(max_attempts):
        try:
            date_str = current_date.strftime('%Y-%m-%d')
            
            if direction == 'backward':
                start_date = (current_date - timedelta(days=5)).strftime('%Y-%m-%d')
                end_date = date_str
            else:
                start_date = date_str
                end_date = (current_date + timedelta(days=5)).strftime('%Y-%m-%d')
            
            hist = get_stock_data_with_retry(ticker_obj, start_date, end_date)
            
            if hist is not None and not hist.empty:
                if direction == 'backward':
                    return hist.index[-1].to_pydatetime()
                else:
                    return hist.index[0].to_pydatetime()
            
        except Exception as e:
            time.sleep(1.5 + random.random())
        
        if direction == 'backward':
            current_date -= timedelta(days=1)
        else:
            current_date += timedelta(days=1)
    
    return None

def get_stock_price(ticker_obj, date):
    # Gets stock price for a specific date
    try:
        start_date = (date - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        hist = get_stock_data_with_retry(ticker_obj, start_date, end_date)
        
        if hist is not None and not hist.empty:
            date_str = date.strftime('%Y-%m-%d')
            if date_str in hist.index:
                return hist.loc[date_str]['Close']
            else:
                return hist['Close'].iloc[-1]
                
    except Exception as e:
        time.sleep(1.0 + random.random())
    return None

def process_company(row_data, days_before, days_after):
    # Processes a single company to get stock prices
    index, row = row_data
    
    try:
        ticker_symbol = clean_ticker(row['Ticker'])
        announce_date_dt = row.get('Announce_Date')
        
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
        
        ticker = yf.Ticker(ticker_symbol)
        target_before = announce_date - timedelta(days=days_before)
        target_after = announce_date + timedelta(days=days_after)
        
        actual_before = get_nearest_trading_day(ticker, target_before, 'backward')
        actual_after = get_nearest_trading_day(ticker, target_after, 'forward')
        
        if actual_before is None or actual_after is None:
            return {
                'index': index, 
                'status': 'No valid trading days found',
                'ticker': ticker_symbol,
                'company': row.get('Acquirer Name', 'Unknown')
            }
        
        price_before = get_stock_price(ticker, actual_before)
        price_after = get_stock_price(ticker, actual_after)
        
        if price_before is None or price_after is None:
            return {
                'index': index, 
                'status': 'Could not get price data',
                'ticker': ticker_symbol,
                'company': row.get('Acquirer Name', 'Unknown')
            }
        
        if price_before > 0:
            percent_return = ((price_after - price_before) / price_before) * 100
        else:
            percent_return = None
            
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
    # Parses command line arguments
    parser = argparse.ArgumentParser(description='Fetch stock prices around announcement dates.')
    parser.add_argument('--days-before', type=int, default=10,
                       help='Days before announcement date (default: 10)')
    parser.add_argument('--days-after', type=int, default=10,
                       help='Days after announcement date (default: 10)')
    parser.add_argument('--workers', type=int, default=2,
                       help='Number of parallel workers (default: 2)')
    return parser.parse_args()

def get_missing_tickers(existing_df, new_df, days_before, days_after):
    # Gets tickers that need price data fetched
    missing_tickers = []
    
    for _, row in new_df.iterrows():
        ticker = row['Ticker']
        if ticker not in existing_df['Ticker'].values:
            missing_tickers.append(row)
        else:
            existing_row = existing_df[existing_df['Ticker'] == ticker].iloc[0]
            price_cols = [f'T_minus_{days_before}_Price', f'T_plus_{days_after}_Price']
            if any(pd.isna(existing_row.get(col)) for col in price_cols):
                missing_tickers.append(row)
                
    return pd.DataFrame(missing_tickers)

def main():
    # Main function to fetch stock prices for event study analysis
    args = parse_arguments()
    
    input_file = "data/2_interim/master_data_classified.csv"
    output_file = f"data/2_interim/master_data_with_stock_prices_{args.days_after}day.csv"
    
    days_before = args.days_before
    days_after = args.days_after
    
    try:
        df = pd.read_csv(input_file)
        
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
        
        for res in results:
            idx = res['index']
            for key, value in res.items():
                if key != 'index' and key != 'company':
                    base_df.loc[idx, key] = value
        
        base_df.to_csv(output_file, index=False)
        print(f"Completed processing {len(results)} companies.")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()