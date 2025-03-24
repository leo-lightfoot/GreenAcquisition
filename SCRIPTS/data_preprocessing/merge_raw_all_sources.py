import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta


def parse_date(date_str):
    """Parse date string into datetime object, handling multiple formats."""
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
    """Get the previous year for merging sales and emissions data."""
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
                new_day = 28  # Handle leap year
            else:
                new_day = date.day
                
            previous_date = datetime(new_year, new_month, new_day)
            return previous_date, previous_date.year
        except Exception:
            return None, None

def load_ma_data(file_path):
    """Load M&A data with tickers."""
    try:
        df = pd.read_csv(file_path)
        df['Announce_Date_DT'] = df['Announce Date'].apply(parse_date)
        
        # Get the previous year date and year
        df[['Reference_Date', 'Reference_Year']] = df['Announce_Date_DT'].apply(
            lambda date: pd.Series(get_previous_year(date))
        )
        
        # Format reference date for GHG data
        df['GHG_Reference_Date'] = df['Reference_Year'].apply(
            lambda year: f"31-12-{year}" if pd.notna(year) else None
        )
        
        return df
    
    except Exception:
        raise

def load_ghg_data(file_path):
    """Load GHG emissions data."""
    try:
        df = pd.read_csv(file_path)
        df = df[df['Ticker'].notna() & (df['Ticker'] != '')]  # Filter out empty tickers
        
        # Average duplicate records
        df_grouped = df.groupby(['Ticker', 'periodenddate'])['GHG_Emissions'].mean().reset_index()
        df_grouped.rename(columns={'GHG_Emissions': 'Acquirer_GHG_Emissions'}, inplace=True)
        
        return df_grouped
    
    except Exception:
        raise

def load_sales_data(file_path):
    """Load sales data."""
    try:
        df = pd.read_csv(file_path)
        df.rename(columns={
            'List of Tickers': 'Ticker',
            'Sales in Mn. Dollars': 'Annual_Sales'
        }, inplace=True)
        
        # Average duplicate records for same ticker and year
        df_grouped = df.groupby(['Ticker', 'Year'])['Annual_Sales'].mean().reset_index()
        
        return df_grouped
    
    except Exception:
        raise

def merge_all_data(ma_data, ghg_data, sales_data):
    """Merge all three data sources."""
    try:
        # Merge M&A data with GHG data
        merged_df = pd.merge(
            ma_data,
            ghg_data,
            left_on=['Ticker', 'GHG_Reference_Date'],
            right_on=['Ticker', 'periodenddate'],
            how='left'
        )
        
        # Merge with sales data
        merged_df = pd.merge(
            merged_df,
            sales_data,
            left_on=['Ticker', 'Reference_Year'],
            right_on=['Ticker', 'Year'],
            how='left'
        )
        
        # Remove rows with empty GHG emissions or sales
        merged_df = merged_df.dropna(subset=['Acquirer_GHG_Emissions', 'Annual_Sales'])

        # Calculate Carbon Intensity
        mask = (merged_df['Acquirer_GHG_Emissions'].notna() & 
                merged_df['Annual_Sales'].notna() & 
                (merged_df['Annual_Sales'] > 0))
                
        merged_df['Carbon_Intensity'] = np.nan
        merged_df.loc[mask, 'Carbon_Intensity'] = (
            merged_df.loc[mask, 'Acquirer_GHG_Emissions'] / 
            merged_df.loc[mask, 'Annual_Sales']
        )
        
        # Remove redundant columns
        columns_to_drop = ['GHG_Reference_Date', 'periodenddate', 'Company Name', 'Year']
        merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
        
        # Specify the desired columns in the required order
        column_order = [
            'Announce Date', 'Reference_Year', 'Ticker', 'Acquirer Name',
            'Annual_Sales', 'Acquirer_GHG_Emissions', 'Carbon_Intensity',
            'Target Name', 'Seller Name', 'Announced Total Value (mil.)',
            'TV/EBITDA', 'Deal Status'
        ]
        
        # Only include columns that exist
        column_order = [col for col in column_order if col in merged_df.columns]
        
        # Reorder the DataFrame
        merged_df = merged_df[column_order]
        
        return merged_df
    
    except Exception:
        raise

def main():
    """Main function to merge all three data sources."""
    # Define file paths
    ma_data_file = "data/1_raw/bloomberg_ma_with_tickers.csv"
    ghg_data_file = "data/1_raw/ghg.csv"
    sales_data_file = "data/1_raw/sales_data_bbg.csv"
    output_dir = "data/1_raw"
    output_file = f"{output_dir}/master_data_merged.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Load all data sources
        ma_data = load_ma_data(ma_data_file)
        ghg_data = load_ghg_data(ghg_data_file)
        sales_data = load_sales_data(sales_data_file)
        
        # Merge all data sources
        merged_data = merge_all_data(ma_data, ghg_data, sales_data)
        
        # Save the merged dataset to a CSV file
        merged_data.to_csv(output_file, index=False)
        
    except Exception:
        pass  # Silent failure in production

if __name__ == "__main__":
    main() 