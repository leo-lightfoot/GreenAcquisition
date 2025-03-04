import pandas as pd
import numpy as np

def classify_by_carbon_intensity(df):
    """
    Classify acquirers based on their carbon intensity (GHG emissions / Market Cap)
    Only the top 25th percentile (lowest carbon intensity) are classified as Green
    """
    # Calculate Carbon Intensity
    df['Carbon_Intensity'] = df['Acquirer_GHG_Emissions'] / df['Market_Cap_AD_mil']
    
    # Format Carbon Intensity to 4 decimal places
    df['Carbon_Intensity'] = df['Carbon_Intensity'].round(4)
    
    # Determine the 25th percentile threshold for classification
    percentile_25th = df['Carbon_Intensity'].quantile(0.25)
    
    # Classify acquirers (only top 25% as Green)
    df['Acquirer_Classification'] = df['Carbon_Intensity'].apply(
        lambda x: 'Green' if x <= percentile_25th else 'Brown'
    )
    
    return df, percentile_25th

def classify_target_by_keywords(target_name):
    """Classify targets based on keywords in their names"""
    green_keywords = ['solar', 'wind', 'renewable', 'sustainable', 'geothermal', 
                     'biomass', 'hydro', 'clean']
    brown_keywords = ['coal', 'oil', 'gas', 'fossil', 'petroleum', 'nuclear', 
                     'traditional']

    target_name_lower = str(target_name).lower()
    
    if any(keyword in target_name_lower for keyword in green_keywords):
        return 'Green'
    elif any(keyword in target_name_lower for keyword in brown_keywords):
        return 'Brown'
    else:
        return 'Neutral'

def format_carbon_intensity(df):
    """Format carbon intensity values for better readability"""
    # Replace extreme values and NaN
    df['Carbon_Intensity'] = df['Carbon_Intensity'].replace([np.inf, -np.inf], np.nan)
    
    # Format to 4 decimal places for reasonable values
    mask = df['Carbon_Intensity'].notna()
    df.loc[mask, 'Carbon_Intensity'] = df.loc[mask, 'Carbon_Intensity'].round(4)
    
    return df

def main():
    # Load the data
    input_file = "master_data_formatted.csv"
    try:
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} rows from {input_file}")
        
        # Create a copy of the full dataset for target classification
        df_all = df.copy()
        
        # Classify targets for all rows
        print("\nClassifying targets based on keywords...")
        df_all['Target_Classification'] = df_all['Target Name'].apply(classify_target_by_keywords)
        
        # For carbon intensity classification, we need to drop rows with missing values
        print("\nClassifying acquirers based on carbon intensity...")
        df_carbon = df_all.dropna(subset=['Acquirer_GHG_Emissions', 'Market_Cap_AD_mil'])
        df_carbon, percentile_25th = classify_by_carbon_intensity(df_carbon)
        
        # Merge the carbon intensity classification back to the main dataframe
        df_all = df_all.merge(
            df_carbon[['Acquirer_Classification', 'Carbon_Intensity']], 
            left_index=True, 
            right_index=True, 
            how='left'
        )
        
        # Format carbon intensity values
        df_all = format_carbon_intensity(df_all)
        
        # Save the results
        output_file = "master_data_dual_classified.csv"
        df_all.to_csv(output_file, index=False)
        
        # Print summary statistics
        print(f"\n✅ Classification complete! Results saved to {output_file}")
        print(f"\n25th Percentile Carbon Intensity Threshold: {percentile_25th:.4f}")
        print("\nTarget Classification Distribution:")
        print(df_all['Target_Classification'].value_counts())
        print("\nAcquirer Classification Distribution:")
        print(df_all['Acquirer_Classification'].value_counts())
        
        # Calculate percentage of companies in each classification
        total_classified = df_all['Acquirer_Classification'].notna().sum()
        green_percent = (df_all['Acquirer_Classification'] == 'Green').sum() / total_classified * 100
        brown_percent = (df_all['Acquirer_Classification'] == 'Brown').sum() / total_classified * 100
        
        print(f"\nAcquirer Classification Percentages:")
        print(f"Green: {green_percent:.1f}%")
        print(f"Brown: {brown_percent:.1f}%")
        
        print("\nSample of classified data:")
        print(df_all[['Target Name', 'Target_Classification', 
                     'Acquirer_Classification', 'Carbon_Intensity']].head().to_string())
        
    except Exception as e:
        print(f"Error processing the data: {str(e)}")

if __name__ == "__main__":
    main() 