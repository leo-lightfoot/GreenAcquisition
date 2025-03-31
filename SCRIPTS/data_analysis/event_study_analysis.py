import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
from scipy import stats
import argparse
from pathlib import Path
import seaborn as sns
from datetime import datetime
import json
import re
from scipy.stats import mstats


def winsorize_returns(returns, limits=(0.01, 0.01)):
    """Winsorize return data to handle outliers."""
    # Store the original index and NA positions
    original_index = returns.index
    is_na = returns.isna()
    
    # Winsorize only non-NA values
    non_na_values = returns[~is_na].values
    if len(non_na_values) > 0:
        winsorized_values = mstats.winsorize(non_na_values, limits=limits)
        
        # Create a new series with the same index as original
        result = pd.Series(index=original_index, dtype=float)
        result[~is_na] = winsorized_values
        result[is_na] = np.nan
        return result
    return returns


def load_data(stock_data_path, benchmark_path):
    """Load stock and benchmark data."""
    print(f"Loading stock data from: {stock_data_path}")
    stock_data = pd.read_csv(stock_data_path)
    
    # Convert string columns to numeric
    numeric_columns = stock_data.select_dtypes(include=['object']).columns
    for col in numeric_columns:
        if 'Date' in col or 'Classification' in col or 'Name' in col or 'Ticker' in col or 'Status' in col:
            continue
        stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')
    
    print(f"Loading benchmark data from: {benchmark_path}")
    benchmark_data = pd.read_csv(benchmark_path)
    benchmark_data['Date'] = pd.to_datetime(benchmark_data['Date'], format='%d-%m-%Y', errors='coerce')
    benchmark_data['Daily_return (%)'] = pd.to_numeric(benchmark_data['Daily_return (%)'], errors='coerce')
    
    # Winsorize benchmark returns while preserving NAs
    benchmark_data['Daily_return (%)'] = winsorize_returns(benchmark_data['Daily_return (%)'])
    benchmark_data = benchmark_data.dropna()
    
    stock_data['Announce Date'] = pd.to_datetime(stock_data['Announce Date'])
    
    # Identify date columns
    date_columns = [col for col in stock_data.columns if re.match(r'T_[a-z]+_\d+_Date', col)]
    print(f"Detected date columns: {date_columns}")
    
    # Extract day range
    day_pattern = re.search(r'(\d+)day', stock_data_path)
    if day_pattern:
        day_range = day_pattern.group(1)
        print(f"Detected {day_range}-day event window from filename")
    else:
        for col in date_columns:
            day_match = re.search(r'T_minus_(\d+)_Date', col)
            if day_match:
                day_range = day_match.group(1)
                print(f"Detected {day_range}-day event window from column names")
                break
        else:
            raise ValueError("Could not determine the day range.")
    
    # Set expected column names
    minus_date_col = f'T_minus_{day_range}_Date'
    plus_date_col = f'T_plus_{day_range}_Date'
    minus_price_col = f'T_minus_{day_range}_Price ($)'
    plus_price_col = f'T_plus_{day_range}_Price ($)'
    
    # Verify required columns
    required_cols = [minus_date_col, plus_date_col]
    missing_cols = [col for col in required_cols if col not in stock_data.columns]
    if missing_cols:
        raise ValueError(f"Required columns are missing: {missing_cols}")
    
    # Ensure price columns are numeric
    for price_col in [minus_price_col, plus_price_col]:
        if price_col in stock_data.columns:
            stock_data[price_col] = pd.to_numeric(stock_data[price_col], errors='coerce')
    
    # Ensure Percent_Return is numeric
    if 'Percent_Return' in stock_data.columns:
        stock_data['Percent_Return'] = pd.to_numeric(stock_data['Percent_Return'], errors='coerce')
    
    # Convert date columns to datetime
    stock_data[minus_date_col] = pd.to_datetime(stock_data[minus_date_col], errors='coerce')
    stock_data[plus_date_col] = pd.to_datetime(stock_data[plus_date_col], errors='coerce')
    
    # Ensure Carbon_Intensity and Annual_Sales are numeric
    if 'Carbon_Intensity' in stock_data.columns:
        stock_data['Carbon_Intensity'] = pd.to_numeric(stock_data['Carbon_Intensity'], errors='coerce')
    if 'Annual_Sales (Million $)' in stock_data.columns:
        stock_data['Annual_Sales (Million $)'] = pd.to_numeric(stock_data['Annual_Sales (Million $)'], errors='coerce')
    
    # Check for mixed types
    for col in stock_data.columns:
        if col not in ['Announce Date', minus_date_col, plus_date_col, 'Ticker', 'Acquirer Name', 
                      'Acquirer_Classification', 'Target Name', 'Target_Classification', 
                      'Seller Name', 'Deal Status']:
            if stock_data[col].dtype == 'object':
                print(f"Warning: Column '{col}' may have mixed data types.")
                stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')
    
    # Store column names for later use
    stock_data.attrs['minus_date_col'] = minus_date_col
    stock_data.attrs['plus_date_col'] = plus_date_col
    stock_data.attrs['minus_price_col'] = minus_price_col
    stock_data.attrs['plus_price_col'] = plus_price_col
    stock_data.attrs['day_range'] = day_range
    
    return stock_data, benchmark_data


def create_analysis_groups(stock_data):
    """Create analysis groups based on acquisition types."""
    all_deals = stock_data.copy()
    green_target = stock_data[stock_data['Target_Classification'] == 'Green'].copy()
    brown_acquirer_green_target = stock_data[
        (stock_data['Acquirer_Classification'] == 'Brown') & 
        (stock_data['Target_Classification'] == 'Green')
    ].copy()
    
    # Copy attributes to each group
    for group in [all_deals, green_target, brown_acquirer_green_target]:
        for attr_name, attr_value in stock_data.attrs.items():
            group.attrs[attr_name] = attr_value
    
    return {
        'All Deals': all_deals,
        'Green Target': green_target,
        'Brown Acquirer - Green Target': brown_acquirer_green_target
    }


def calculate_abnormal_returns(groups, benchmark_data):
    """Calculate abnormal returns."""
    results = {}
    
    for group_name, group_data in groups.items():
        print(f"Calculating abnormal returns for: {group_name}")
        group_data = group_data.copy()
        
        minus_date_col = group_data.attrs['minus_date_col']
        plus_date_col = group_data.attrs['plus_date_col']
        
        group_data['Benchmark_Return'] = np.nan
        
        for idx, row in group_data.iterrows():
            try:
                start_date = row[minus_date_col]
                end_date = row[plus_date_col]
                
                if pd.isna(start_date) or pd.isna(end_date):
                    print(f"Warning: Missing date for index {idx}.")
                    continue
                
                mask = (benchmark_data['Date'] >= start_date) & (benchmark_data['Date'] <= end_date)
                event_benchmark = benchmark_data.loc[mask]
                
                if not event_benchmark.empty:
                    benchmark_returns = event_benchmark['Daily_return (%)'].dropna() / 100
                    if not benchmark_returns.empty:
                        cumulative_return = (1 + benchmark_returns).prod() - 1
                        group_data.at[idx, 'Benchmark_Return'] = cumulative_return * 100
                    else:
                        print(f"Warning: No valid benchmark returns for index {idx}.")
                else:
                    print(f"Warning: No benchmark data found for dates between {start_date} and {end_date}.")
            except Exception as e:
                print(f"Error calculating benchmark return for index {idx}: {e}")
        
        # Winsorize Percent_Return before calculating abnormal returns
        group_data['Percent_Return'] = winsorize_returns(group_data['Percent_Return'])
        
        # Calculate abnormal returns
        group_data['Abnormal_Return'] = group_data['Percent_Return'] - group_data['Benchmark_Return']
        
        # Winsorize abnormal returns
        group_data['Abnormal_Return'] = winsorize_returns(group_data['Abnormal_Return'])
        
        # Calculate market cap weighted returns
        if 'Annual_Sales (Million $)' in group_data.columns:
            sales = group_data['Annual_Sales (Million $)'].fillna(0)
            if sales.sum() > 0:
                group_data['Weighted_Abnormal_Return'] = group_data['Abnormal_Return'] * (sales / sales.sum())
                group_data['Weight'] = sales / sales.sum()
            else:
                group_data['Weighted_Abnormal_Return'] = group_data['Abnormal_Return']
                group_data['Weight'] = 1 / len(group_data)
        
        # Add log transformation of Carbon_Intensity
        if 'Carbon_Intensity' in group_data.columns:
            # Add small constant to handle zeros before log transformation
            group_data['Log_Carbon_Intensity'] = np.log(group_data['Carbon_Intensity'].fillna(0) + 1)
        
        results[group_name] = group_data
    
    return results


def perform_comprehensive_analysis(results):
    """Perform comprehensive analysis combining statistical and scenario analysis."""
    analysis_results = {}
    
    first_group = next(iter(results.values()), None)
    day_range = first_group.attrs.get('day_range', 'unknown') if first_group is not None else 'unknown'
    
    for group_name, group_data in results.items():
        abnormal_returns = group_data['Abnormal_Return'].dropna()
        
        if len(abnormal_returns) == 0:
            print(f"No valid abnormal returns for {group_name}.")
            continue
        
        # Basic statistics
        mean_ar = abnormal_returns.mean()
        median_ar = abnormal_returns.median()
        std_ar = abnormal_returns.std()
        
        # T-test with robust standard errors
        # Using statsmodels for robust t-test
        X = sm.add_constant(np.ones(len(abnormal_returns)))
        model = sm.OLS(abnormal_returns, X).fit(cov_type='HC3')  # Using HC3 robust standard errors
        t_stat = model.tvalues[0]
        p_value = model.pvalues[0]
        
        # Market cap weighted return
        weighted_ar = group_data['Weighted_Abnormal_Return'].sum() if 'Weighted_Abnormal_Return' in group_data.columns else None
        
        # Carbon intensity analysis
        carbon_intensity_analysis = {}
        if 'Log_Carbon_Intensity' in group_data.columns:
            log_carbon_intensity = group_data['Log_Carbon_Intensity'].fillna(0)
            abnormal_returns_for_corr = group_data['Abnormal_Return'].fillna(0)
            valid_data = ~(np.isnan(log_carbon_intensity) | np.isnan(abnormal_returns_for_corr))
            valid_carbon = log_carbon_intensity[valid_data]
            valid_ar = abnormal_returns_for_corr[valid_data]
            
            if len(valid_carbon) > 1:
                correlation, p_val_corr = stats.pearsonr(valid_carbon, valid_ar)
                carbon_intensity_analysis['Correlation'] = float(correlation)
                carbon_intensity_analysis['Correlation P-value'] = float(p_val_corr)
            
            # Regression analysis with robust standard errors
            if len(valid_data) > 2:
                try:
                    X = sm.add_constant(valid_carbon)
                    Y = valid_ar
                    model = sm.OLS(Y, X).fit(cov_type='HC3')  # Using HC3 robust standard errors
                    carbon_intensity_analysis['Regression'] = {
                        'Coefficient': float(model.params.iloc[1]) if len(model.params) > 1 else None,
                        'P-value': float(model.pvalues.iloc[1]) if len(model.pvalues) > 1 else None,
                        'R-squared': float(model.rsquared),
                        'Robust Standard Error': float(model.bse.iloc[1]) if len(model.bse) > 1 else None
                    }
                except Exception as e:
                    print(f"Error in regression analysis for {group_name}: {e}")
        
        # Additional analyses
        positive_ar_count = int(sum(abnormal_returns > 0))
        negative_ar_count = int(sum(abnormal_returns < 0))
        win_ratio = positive_ar_count / len(abnormal_returns)
        
        # Size effect analysis
        size_effect = {}
        if 'Annual_Sales (Million $)' in group_data.columns:
            sales = group_data['Annual_Sales (Million $)'].fillna(0)
            size_quartiles = pd.qcut(sales, 4, labels=['Small', 'Medium', 'Large', 'Very Large'])
            size_returns = pd.DataFrame({
                'Size': size_quartiles,
                'Return': abnormal_returns
            }).groupby('Size', observed=True)['Return'].agg(['mean', 'count']).to_dict('index')
            size_effect['Returns by Size'] = size_returns
        
        # Store all results
        analysis_results[group_name] = {
            'Day Range': day_range,
            'Sample Characteristics': {
                'Sample Size': int(len(abnormal_returns)),
                'Positive Returns Count': positive_ar_count,
                'Negative Returns Count': negative_ar_count,
                'Win Ratio': float(win_ratio)
            },
            'Return Statistics': {
                'Mean Abnormal Return': float(mean_ar),
                'Median Abnormal Return': float(median_ar),
                'Standard Deviation': float(std_ar),
                'T-statistic': float(t_stat),
                'P-value': float(p_value),
                'Weighted Abnormal Return': float(weighted_ar) if weighted_ar is not None else None
            },
            'Carbon Intensity Analysis': carbon_intensity_analysis,
            'Size Effect Analysis': size_effect
        }
        
        # Add volatility analysis if 10-day window
        if day_range == '10':
            daily_volatility = group_data['Abnormal_Return'].std() / np.sqrt(10)
            analysis_results[group_name]['Volatility Analysis'] = {
                'Daily Volatility': float(daily_volatility),
                'Annualized Volatility': float(daily_volatility * np.sqrt(252))
            }
    
    return analysis_results


def save_results(results, output_dir):
    """Save analysis results to output directory."""
    os.makedirs(output_dir, exist_ok=True)
    
    first_group = next(iter(results.values()), None)
    day_range = first_group.attrs.get('day_range', 'unknown') if first_group is not None else 'unknown'
    
    # Save datasets with abnormal returns
    for group_name, group_data in results.items():
        file_name = f"{group_name.replace(' ', '_').lower()}_{day_range}day_results.csv"
        group_data.to_csv(os.path.join(output_dir, file_name), index=False, float_format='%.6f')
    
    # Perform comprehensive analysis
    analysis_results = perform_comprehensive_analysis(results)
    
    # Custom JSON encoder for numpy types
    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.integer, np.int_, np.intc, np.intp, np.int8,
                               np.int16, np.int32, np.int64, np.uint8, np.uint16,
                               np.uint32, np.uint64)):
                return int(obj)
            elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
                return float(obj)
            elif isinstance(obj, (np.ndarray,)):
                return obj.tolist()
            return json.JSONEncoder.default(self, obj)
    
    # Save comprehensive analysis results
    with open(os.path.join(output_dir, f'comprehensive_analysis_{day_range}day.json'), 'w') as f:
        json.dump(analysis_results, f, indent=4, cls=NumpyEncoder)
    
    # Generate visualizations
    create_visualizations(results, output_dir, day_range)
    
    print(f"All results saved to: {output_dir}")


def create_visualizations(results, output_dir, day_range):
    """Create visualizations for analysis results."""
    sns.set(style="whitegrid")
    
    vis_dir = os.path.join(output_dir, 'visualizations')
    os.makedirs(vis_dir, exist_ok=True)
    
    for group_name, group_data in results.items():
        abnormal_returns = pd.to_numeric(group_data['Abnormal_Return'], errors='coerce').dropna()
        
        # Distribution plot with winsorized returns
        plt.figure(figsize=(10, 6))
        sns.histplot(abnormal_returns, kde=True)
        plt.title(f'Distribution of Winsorized Abnormal Returns ({day_range}-day) - {group_name}')
        plt.xlabel('Abnormal Return (%)')
        plt.ylabel('Frequency')
        plt.savefig(os.path.join(vis_dir, f'{group_name.replace(" ", "_").lower()}_{day_range}day_ar_distribution.png'))
        plt.close()
        
        # Scatter plot with log carbon intensity
        if 'Log_Carbon_Intensity' in group_data.columns and len(group_data) > 0:
            log_carbon_intensity = pd.to_numeric(group_data['Log_Carbon_Intensity'], errors='coerce')
            abnormal_return = pd.to_numeric(group_data['Abnormal_Return'], errors='coerce')
            temp_df = pd.DataFrame({
                'Log Carbon Intensity': log_carbon_intensity,
                'Abnormal Return': abnormal_return
            }).dropna()
            
            if not temp_df.empty:
                plt.figure(figsize=(10, 6))
                sns.scatterplot(x='Log Carbon Intensity', y='Abnormal Return', data=temp_df)
                plt.title(f'Log Carbon Intensity vs Abnormal Return ({day_range}-day) - {group_name}')
                plt.xlabel('Log Carbon Intensity')
                plt.ylabel('Abnormal Return (%)')
                plt.savefig(os.path.join(vis_dir, f'{group_name.replace(" ", "_").lower()}_{day_range}day_carbon_vs_ar.png'))
                plt.close()
    
    # Comparison of mean abnormal returns
    if len(results) > 1:
        group_means = {}
        for group in results:
            abnormal_returns = pd.to_numeric(results[group]['Abnormal_Return'], errors='coerce').dropna()
            if not abnormal_returns.empty:
                group_means[group] = abnormal_returns.mean()
        
        if group_means:
            plt.figure(figsize=(10, 6))
            bars = plt.bar(group_means.keys(), group_means.values())
            plt.title(f'Mean Winsorized Abnormal Returns Across Groups ({day_range}-day)')
            plt.xlabel('Group')
            plt.ylabel('Mean Abnormal Return (%)')
            plt.xticks(rotation=45, ha='right')
            
            for bar in bars:
                height = bar.get_height()
                va = 'top' if height < 0 else 'bottom'
                offset = -0.5 if height < 0 else 0.5
                plt.text(bar.get_x() + bar.get_width()/2., height + offset,
                        f'{height:.2f}%', ha='center', va=va)
            
            plt.tight_layout()
            plt.savefig(os.path.join(vis_dir, f'mean_abnormal_returns_comparison_{day_range}day.png'))
            plt.close()


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Event Study Analysis for M&A Announcements')
    parser.add_argument('data_file', type=str, help='Path to standardized stock data file')
    parser.add_argument('--output', type=str, default='results', help='Output directory for results')
    args = parser.parse_args()
    
    # Resolve paths
    workspace_dir = Path(__file__).resolve().parents[2]
    
    data_file = args.data_file
    if not os.path.isabs(data_file) and not data_file.startswith('./') and not data_file.startswith('../'):
        data_file = os.path.join(workspace_dir, 'data', '3_processed', data_file)
    
    benchmark_file = os.path.join(workspace_dir, 'data', '1_raw', 'xle_benchmark_data_returns.csv')
    
    # Extract day range from filename
    day_pattern = re.search(r'(\d+)day', data_file)
    day_suffix = f"_{day_pattern.group(1)}day" if day_pattern else ""
    
    # Output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(workspace_dir, args.output, f'event_study{day_suffix}_{timestamp}')
    
    # Load data
    stock_data, benchmark_data = load_data(data_file, benchmark_file)
    
    # Create analysis groups
    groups = create_analysis_groups(stock_data)
    
    # Calculate abnormal returns
    results = calculate_abnormal_returns(groups, benchmark_data)
    
    # Save results
    save_results(results, output_dir)
    
    print(f"Event study analysis completed. Results saved to: {output_dir}")


if __name__ == "__main__":
    main() 