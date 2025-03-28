import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


def parse_date(date_str):
    # Parse date string into datetime, trying multiple formats
    if pd.isna(date_str):
        return None
        
    date_formats = ['%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    try:
        return pd.to_datetime(date_str)
    except:
        return None

def get_previous_year(date):
    # Get previous year date for merging sales and emissions data
    if date is None:
        return None, None
    
    try:
        one_year_prior = date - relativedelta(years=1)
        return one_year_prior, one_year_prior.year
    except Exception:
        try:
            new_year = date.year - 1
            new_month = date.month
            if date.month == 2 and date.day == 29:
                new_day = 28
            else:
                new_day = date.day
                
            previous_date = datetime(new_year, new_month, new_day)
            return previous_date, previous_date.year
        except Exception:
            return None, None

def load_ma_data(file_path):
    # Load M&A data and prepare dates for merging
    try:
        df = pd.read_csv(file_path)
        df['Announce_Date_DT'] = df['Announce Date'].apply(parse_date)
        
        df[['Reference_Date', 'Reference_Year']] = df['Announce_Date_DT'].apply(
            lambda date: pd.Series(get_previous_year(date))
        )
        
        df['GHG_Reference_Date'] = df['Reference_Year'].apply(
            lambda year: f"31-12-{year}" if pd.notna(year) else None
        )
        
        return df
    
    except Exception:
        raise

def load_ghg_data(file_path):
    # Load GHG emissions data and handle duplicates
    try:
        df = pd.read_csv(file_path)
        df = df[df['Ticker'].notna() & (df['Ticker'] != '')]
        
        df_grouped = df.groupby(['Ticker', 'periodenddate'])['GHG_Emissions'].mean().reset_index()
        df_grouped.rename(columns={'GHG_Emissions': 'Acquirer_GHG_Emissions'}, inplace=True)
        
        return df_grouped
    
    except Exception:
        raise

def load_sales_data(file_path):
    # Load sales data and handle duplicates
    try:
        df = pd.read_csv(file_path)
        df.rename(columns={
            'List of Tickers': 'Ticker',
            'Sales in Mn. Dollars': 'Annual_Sales'
        }, inplace=True)
        
        df_grouped = df.groupby(['Ticker', 'Year'])['Annual_Sales'].mean().reset_index()
        
        return df_grouped
    
    except Exception:
        raise

def merge_all_data(ma_data, ghg_data, sales_data):
    # Merge M&A, GHG, and sales data
    try:
        merged_df = pd.merge(
            ma_data,
            ghg_data,
            left_on=['Ticker', 'GHG_Reference_Date'],
            right_on=['Ticker', 'periodenddate'],
            how='left'
        )
        
        merged_df = pd.merge(
            merged_df,
            sales_data,
            left_on=['Ticker', 'Reference_Year'],
            right_on=['Ticker', 'Year'],
            how='left'
        )
        
        merged_df = merged_df.dropna(subset=['Acquirer_GHG_Emissions', 'Annual_Sales'])

        mask = (merged_df['Acquirer_GHG_Emissions'].notna() & 
                merged_df['Annual_Sales'].notna() & 
                (merged_df['Annual_Sales'] > 0))
                
        merged_df['Carbon_Intensity'] = np.nan
        merged_df.loc[mask, 'Carbon_Intensity'] = (
            merged_df.loc[mask, 'Acquirer_GHG_Emissions'] / 
            merged_df.loc[mask, 'Annual_Sales']
        )
        
        columns_to_drop = ['GHG_Reference_Date', 'periodenddate', 'Company Name', 'Year']
        merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
        
        column_order = [
            'Announce Date', 'Reference_Year', 'Ticker', 'Acquirer Name',
            'Annual_Sales', 'Acquirer_GHG_Emissions', 'Carbon_Intensity',
            'Target Name', 'Seller Name', 'Announced Total Value (mil.)',
            'TV/EBITDA', 'Deal Status'
        ]
        
        column_order = [col for col in column_order if col in merged_df.columns]
        merged_df = merged_df[column_order]
        
        return merged_df
    
    except Exception:
        raise

def main():
    # Merge M&A, GHG, and sales data into a single dataset
    ma_data_file = "data/1_raw/bloomberg_ma_with_tickers.csv"
    ghg_data_file = "data/1_raw/ghg.csv"
    sales_data_file = "data/1_raw/sales_data_bbg.csv"
    output_dir = "data/1_raw"
    output_file = f"{output_dir}/master_data_merged.csv"
    
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        ma_data = load_ma_data(ma_data_file)
        ghg_data = load_ghg_data(ghg_data_file)
        sales_data = load_sales_data(sales_data_file)
        
        merged_data = merge_all_data(ma_data, ghg_data, sales_data)
        merged_data.to_csv(output_file, index=False)
        
    except Exception:
        pass

if __name__ == "__main__":
    main() 