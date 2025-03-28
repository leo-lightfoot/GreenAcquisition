import pandas as pd
import os
import re
import argparse
import numpy as np


def parse_arguments():
    # Parse command line arguments for stock data standardization
    parser = argparse.ArgumentParser(description='Standardize stock price data files.')
    parser.add_argument('filename', type=str, help='Name of file to standardize (e.g., master_data_with_stock_prices_10day.csv)')
    return parser.parse_args()


def standardize_data(df, day_value):
    # Standardize data format and column names
    df.columns = [str(col).strip() for col in df.columns]
    
    # Define standard column mappings
    column_mapping = {
        'announce date': 'Announce Date',
        'reference_year': 'Reference_Year', 
        'reference year': 'Reference_Year',
        'ticker': 'Ticker',
        'acquirer name': 'Acquirer Name',
        'annual_sales': 'Annual_Sales',
        'acquirer_ghg_emissions': 'Acquirer_GHG_Emissions',
        'carbon_intensity': 'Carbon_Intensity',
        'acquirer_classification': 'Acquirer_Classification',
        'target name': 'Target Name',
        'target_classification': 'Target_Classification',
        'seller name': 'Seller Name',
        'announced total value (mil.)': 'Announced Total Value (mil.)',
        'tv/ebitda': 'TV/EBITDA',
        'percent_return': 'Percent_Return',
        'deal status': 'Deal Status',
        f't_minus_{day_value}_date': f'T_minus_{day_value}_Date',
        f't_minus_{day_value}_price': f'T_minus_{day_value}_Price',
        f't_plus_{day_value}_date': f'T_plus_{day_value}_Date',
        f't_plus_{day_value}_price': f'T_plus_{day_value}_Price',
        f't-{day_value}_date': f'T_minus_{day_value}_Date',
        f't-{day_value}_price': f'T_minus_{day_value}_Price',
        f't+{day_value}_date': f'T_plus_{day_value}_Date',
        f't+{day_value}_price': f'T_plus_{day_value}_Price'
    }
    
    final_columns = [
        'Announce Date', 'Reference_Year', 'Ticker', 'Acquirer Name', 
        'Annual_Sales', 'Acquirer_GHG_Emissions', 'Carbon_Intensity',
        'Acquirer_Classification', 'Target Name', 'Target_Classification',
        'Seller Name', 'Announced Total Value (mil.)', 'TV/EBITDA',
        f'T_minus_{day_value}_Date', f'T_minus_{day_value}_Price',
        f'T_plus_{day_value}_Date', f'T_plus_{day_value}_Price',
        'Percent_Return', 'Deal Status'
    ]
    
    renamed_columns = {}
    
    # Try exact matches first
    for original_col in df.columns:
        if original_col.lower() in column_mapping:
            renamed_columns[original_col] = column_mapping[original_col.lower()]
    
    target_cols_to_find = set(final_columns) - set(renamed_columns.values())
    
    # Define flexible patterns for T-day fields
    flexible_patterns = {
        f'T_minus_{day_value}_Date': [f't[-_]{day_value}.*date', f'tminus{day_value}.*date'],
        f'T_minus_{day_value}_Price': [f't[-_]{day_value}.*price', f'tminus{day_value}.*price'],
        f'T_plus_{day_value}_Date': [f't[+_]{day_value}.*date', f'tplus{day_value}.*date'],
        f'T_plus_{day_value}_Price': [f't[+_]{day_value}.*price', f'tplus{day_value}.*price']
    }
    
    # Match remaining columns with patterns
    for original_col in df.columns:
        if original_col in renamed_columns:
            continue
            
        lowercase_col = original_col.lower()
        
        matched = False
        for target_col, patterns in flexible_patterns.items():
            if target_col in target_cols_to_find:
                if any(re.search(pattern, lowercase_col) for pattern in patterns):
                    renamed_columns[original_col] = target_col
                    target_cols_to_find.remove(target_col)
                    matched = True
                    break
        if matched:
            continue
            
        for pattern, target in column_mapping.items():
            if target in target_cols_to_find and (pattern in lowercase_col or 
                                                 any(word in lowercase_col for word in pattern.split('_'))):
                renamed_columns[original_col] = target
                target_cols_to_find.remove(target)
                break
    
    # Handle duplicate columns
    df_renamed = df.rename(columns=renamed_columns)
    if len(set(df_renamed.columns)) < len(df_renamed.columns):
        dup_cols = df_renamed.columns.duplicated(keep='first')
        df_renamed.columns = [f"{col}_dup{i}" if dup else col for i, (col, dup) in enumerate(zip(df_renamed.columns, dup_cols))]
    
    # Create final standardized dataframe
    df_standardized = pd.DataFrame(index=df.index)
    for col in final_columns:
        df_standardized[col] = df_renamed[col] if col in df_renamed.columns else np.nan
    
    # Format numeric columns
    numeric_cols = {
        'Annual_Sales': 2, 'Acquirer_GHG_Emissions': 2, 'Carbon_Intensity': 4,
        'Announced Total Value (mil.)': 2, 'TV/EBITDA': 2,
        f'T_minus_{day_value}_Price': 2, f'T_plus_{day_value}_Price': 2, 'Percent_Return': 2
    }
    
    for col, decimal_places in numeric_cols.items():
        if col in df_standardized.columns:
            try:
                df_standardized[col] = pd.to_numeric(df_standardized[col], errors='coerce')
                df_standardized[col] = df_standardized[col].replace([np.inf, -np.inf], np.nan).round(decimal_places)
            except Exception:
                pass
    
    # Process date columns
    date_cols = ['Announce Date', f'T_minus_{day_value}_Date', f'T_plus_{day_value}_Date']
    for col in date_cols:
        if col in df_standardized.columns:
            try:
                original_values = df_standardized[col].copy()
                df_standardized[col] = pd.to_datetime(df_standardized[col], errors='coerce')
                
                if df_standardized[col].isna().sum() > 0.5 * len(df_standardized):
                    df_standardized[col] = pd.to_datetime(original_values, dayfirst=True, errors='coerce')
                
                mask = df_standardized[col].notna()
                if mask.any():
                    df_standardized.loc[mask, col] = df_standardized.loc[mask, col].dt.strftime('%Y-%m-%d')
            except Exception:
                pass
    
    # Add units to column names
    denominations = {
        'Annual_Sales': 'Annual_Sales (Million $)',
        'Acquirer_GHG_Emissions': 'Acquirer_GHG_Emissions (Metric Tonnes)',
        f'T_minus_{day_value}_Price': f'T_minus_{day_value}_Price ($)',
        f'T_plus_{day_value}_Price': f'T_plus_{day_value}_Price ($)'
    }
    
    existing_cols = {k: v for k, v in denominations.items() if k in df_standardized.columns}
    return df_standardized.rename(columns=existing_cols)


def main():
    # Standardize stock price data files
    args = parse_arguments()
    
    day_match = re.search(r'(\d+)day', args.filename)
    if not day_match:
        day_value = 10
    else:
        day_value = int(day_match.group(1))
    
    input_dir = "data/2_interim"
    input_file = os.path.join(input_dir, args.filename)
    
    output_dir = "data/3_processed"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"standardized_stock_data_{day_value}day.csv")
    
    try:
        df = pd.read_csv(input_file, low_memory=False)
        df_standardized = standardize_data(df, day_value)
        df_standardized.to_csv(output_file, index=False)
        
    except Exception as e:
        pass


if __name__ == "__main__":
    main() 