import pandas as pd
import numpy as np
import os

def classify_by_carbon_intensity(df):
    """Classify acquirers based on their carbon intensity (GHG emissions / Annual Sales)."""
    df['Carbon_Intensity'] = df['Acquirer_GHG_Emissions'] / df['Annual_Sales']
    df['Carbon_Intensity'] = df['Carbon_Intensity'].round(4)
    percentile_25th = df['Carbon_Intensity'].quantile(0.25)  # Use lowest 25% as green threshold
    df['Acquirer_Classification'] = df['Carbon_Intensity'].apply(
        lambda x: 'Green' if x <= percentile_25th else 'Brown'
    )
    return df, percentile_25th

def classify_target_by_keywords(target_name):
    """Classify targets based on keywords in their names."""
    if pd.isna(target_name):
        return 'Unknown'
        
    green_keywords = [
        'solar', 'wind', 'renewable', 'sustainable', 'geothermal', 
        'biomass', 'hydro', 'clean', 'energy storage', 'battery', 
        'recycling', 'water treatment', 'waste management', 'carbon capture',
        'electric vehicle', 'ev', 'green', 'circular economy'
    ]
    
    brown_keywords = [
        'coal', 'oil', 'gas', 'fossil', 'petroleum', 'nuclear', 
        'traditional', 'mining', 'drilling', 'fracking', 'combustion',
        'refinery', 'pipeline'
    ]

    target_name_lower = str(target_name).lower()
    
    if any(keyword in target_name_lower for keyword in green_keywords):
        return 'Green'
    elif any(keyword in target_name_lower for keyword in brown_keywords):
        return 'Brown'
    else:
        return 'Neutral'

def format_carbon_intensity(df):
    """Format carbon intensity values for better readability."""
    df['Carbon_Intensity'] = df['Carbon_Intensity'].replace([np.inf, -np.inf], np.nan)  # Handle infinity values
    mask = df['Carbon_Intensity'].notna()
    df.loc[mask, 'Carbon_Intensity'] = df.loc[mask, 'Carbon_Intensity'].round(4)
    return df

def main():
    """Main function to classify companies as Green or Brown based on carbon intensity."""
    input_file = "data/1_raw/master_data_merged.csv"
    output_dir = "data/2_interim"
    output_file = f"{output_dir}/master_data_classified.csv"
    
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        df = pd.read_csv(input_file)
        
        df_all = df.copy()
        
        required_columns = ['Target Name', 'Ticker', 'Acquirer Name', 'Acquirer_GHG_Emissions', 'Annual_Sales']
        missing_columns = [col for col in required_columns if col not in df_all.columns]
        
        if missing_columns:
            return  # Exit if required columns are missing
        
        if 'Target Name' in df_all.columns:
            df_all['Target_Classification'] = df_all['Target Name'].apply(classify_target_by_keywords)
        
        if 'Acquirer_GHG_Emissions' in df_all.columns and 'Annual_Sales' in df_all.columns:
            # Filter rows with valid data for classification
            df_carbon = df_all.dropna(subset=['Acquirer_GHG_Emissions', 'Annual_Sales'])
            df_carbon = df_carbon[df_carbon['Annual_Sales'] > 0]
            
            if len(df_carbon) > 0:
                df_carbon, percentile_25th = classify_by_carbon_intensity(df_carbon)
                
                if 'Ticker' in df_carbon.columns:
                    # Create mapping dictionaries for efficient application
                    classification_map = df_carbon.set_index('Ticker')['Acquirer_Classification'].to_dict()
                    carbon_intensity_map = df_carbon.set_index('Ticker')['Carbon_Intensity'].to_dict()
                    
                    if 'Ticker' in df_all.columns:
                        df_all['Acquirer_Classification'] = df_all['Ticker'].map(classification_map)
                        df_all['Carbon_Intensity'] = df_all['Ticker'].map(carbon_intensity_map)
                        df_all = format_carbon_intensity(df_all)
        
        # Specify the desired columns in the required order
        column_order = [
            'Announce Date', 'Reference_Year', 'Ticker', 'Acquirer Name',
            'Annual_Sales', 'Acquirer_GHG_Emissions', 'Carbon_Intensity',
            'Acquirer_Classification', 'Target Name', 'Target_Classification',
            'Seller Name', 'Announced Total Value (mil.)', 'TV/EBITDA', 'Deal Status'
        ]
        
        # Only include columns that exist in the DataFrame
        column_order = [col for col in column_order if col in df_all.columns]
        
        # Reorder the DataFrame
        df_all = df_all[column_order]
        
        df_all.to_csv(output_file, index=False)
        
    except FileNotFoundError:
        pass  # Silently fail if input file is missing
    except Exception as e:
        pass  # Silently fail on other errors

if __name__ == "__main__":
    main() 