import os
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.stats.diagnostic import het_breuschpagan, het_white
from scipy import stats
import argparse
from pathlib import Path
import json
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def load_results(results_file):
    """Load the comprehensive analysis results."""
    with open(results_file, 'r') as f:
        return json.load(f)

def perform_heteroskedasticity_tests(group_data):
    """
    Perform heteroskedasticity tests on the regression model.
    Returns results from Breusch-Pagan and White tests.
    """
    if 'Log_Carbon_Intensity' not in group_data.columns:
        return None
    
    # Prepare data
    X = sm.add_constant(group_data['Log_Carbon_Intensity'].fillna(0))
    y = group_data['Abnormal_Return'].fillna(0)
    
    # Fit the model
    model = sm.OLS(y, X).fit()
    
    # Get residuals
    residuals = model.resid
    
    # Perform Breusch-Pagan test
    bp_lm, bp_pvalue, bp_fvalue, bp_f_pvalue = het_breuschpagan(residuals, X)
    
    # Perform White test
    white_lm, white_pvalue, white_fvalue, white_f_pvalue = het_white(residuals, X)
    
    # Calculate robust standard errors
    robust_model = sm.OLS(y, X).fit(cov_type='HC3')
    
    return {
        'Breusch_Pagan': {
            'LM_statistic': float(bp_lm),
            'p_value': float(bp_pvalue),
            'F_statistic': float(bp_fvalue),
            'F_p_value': float(bp_f_pvalue)
        },
        'White': {
            'LM_statistic': float(white_lm),
            'p_value': float(white_pvalue),
            'F_statistic': float(white_fvalue),
            'F_p_value': float(white_f_pvalue)
        },
        'Robust_Standard_Errors': {
            'Coefficient': float(robust_model.params.iloc[1]),
            'Std_Error': float(robust_model.bse.iloc[1]),
            'P_value': float(robust_model.pvalues.iloc[1]),
            'R_squared': float(robust_model.rsquared)
        }
    }

def create_heteroskedasticity_plots(group_data, output_dir, group_name):
    """Create plots to visualize heteroskedasticity."""
    if 'Log_Carbon_Intensity' not in group_data.columns:
        return
    
    # Prepare data
    X = sm.add_constant(group_data['Log_Carbon_Intensity'].fillna(0))
    y = group_data['Abnormal_Return'].fillna(0)
    
    # Fit the model
    model = sm.OLS(y, X).fit()
    residuals = model.resid
    
    # Create plots directory
    plots_dir = os.path.join(output_dir, 'heteroskedasticity_plots')
    os.makedirs(plots_dir, exist_ok=True)
    
    # Residuals vs Fitted Values plot
    plt.figure(figsize=(10, 6))
    plt.scatter(model.fittedvalues, residuals, alpha=0.5)
    plt.axhline(y=0, color='r', linestyle='--')
    plt.xlabel('Fitted Values')
    plt.ylabel('Residuals')
    plt.title(f'Residuals vs Fitted Values - {group_name}')
    plt.savefig(os.path.join(plots_dir, f'{group_name}_residuals_vs_fitted.png'))
    plt.close()
    
    # Residuals vs Carbon Intensity plot
    plt.figure(figsize=(10, 6))
    plt.scatter(group_data['Log_Carbon_Intensity'], residuals, alpha=0.5)
    plt.axhline(y=0, color='r', linestyle='--')
    plt.xlabel('Log Carbon Intensity')
    plt.ylabel('Residuals')
    plt.title(f'Residuals vs Log Carbon Intensity - {group_name}')
    plt.savefig(os.path.join(plots_dir, f'{group_name}_residuals_vs_carbon.png'))
    plt.close()

def analyze_heteroskedasticity(results_file, output_dir):
    """Main function to analyze heteroskedasticity in the event study results."""
    # Load results
    results = load_results(results_file)
    
    # Create output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(output_dir, f'heteroskedasticity_analysis_{timestamp}')
    os.makedirs(output_dir, exist_ok=True)
    
    # Load the original data
    data_dir = Path(results_file).parent
    day_range = results['All Deals']['Day Range']
    
    heteroskedasticity_results = {}
    
    # Analyze each group
    for group_name in results.keys():
        # Load group data
        data_file = os.path.join(data_dir, f"{group_name.replace(' ', '_').lower()}_{day_range}day_results.csv")
        if not os.path.exists(data_file):
            print(f"Warning: Data file not found for {group_name}")
            continue
        
        group_data = pd.read_csv(data_file)
        
        # Perform heteroskedasticity tests
        test_results = perform_heteroskedasticity_tests(group_data)
        if test_results:
            heteroskedasticity_results[group_name] = test_results
            
            # Create visualization plots
            create_heteroskedasticity_plots(group_data, output_dir, group_name)
    
    # Save results
    with open(os.path.join(output_dir, 'heteroskedasticity_analysis.json'), 'w') as f:
        json.dump(heteroskedasticity_results, f, indent=4)
    
    # Create summary report
    create_summary_report(heteroskedasticity_results, output_dir)
    
    print(f"Heteroskedasticity analysis completed. Results saved to: {output_dir}")

def create_summary_report(results, output_dir):
    """Create a summary report of the heteroskedasticity analysis."""
    report = []
    report.append("Heteroskedasticity Analysis Summary")
    report.append("=" * 50)
    
    for group_name, group_results in results.items():
        report.append(f"\n{group_name}")
        report.append("-" * 30)
        
        # Breusch-Pagan test results
        bp = group_results['Breusch_Pagan']
        report.append("\nBreusch-Pagan Test:")
        report.append(f"LM Statistic: {bp['LM_statistic']:.4f}")
        report.append(f"P-value: {bp['p_value']:.4f}")
        report.append(f"F-statistic: {bp['F_statistic']:.4f}")
        report.append(f"F-test P-value: {bp['F_p_value']:.4f}")
        
        # White test results
        white = group_results['White']
        report.append("\nWhite Test:")
        report.append(f"LM Statistic: {white['LM_statistic']:.4f}")
        report.append(f"P-value: {white['p_value']:.4f}")
        report.append(f"F-statistic: {white['F_statistic']:.4f}")
        report.append(f"F-test P-value: {white['F_p_value']:.4f}")
        
        # Robust standard errors
        robust = group_results['Robust_Standard_Errors']
        report.append("\nRobust Standard Errors:")
        report.append(f"Coefficient: {robust['Coefficient']:.4f}")
        report.append(f"Standard Error: {robust['Std_Error']:.4f}")
        report.append(f"P-value: {robust['P_value']:.4f}")
        report.append(f"R-squared: {robust['R_squared']:.4f}")
        
        # Interpretation
        report.append("\nInterpretation:")
        if bp['p_value'] < 0.05 or white['p_value'] < 0.05:
            report.append("Evidence of heteroskedasticity detected. Using robust standard errors is recommended.")
        else:
            report.append("No significant evidence of heteroskedasticity. Standard errors are reliable.")
    
    # Save report
    with open(os.path.join(output_dir, 'heteroskedasticity_summary.txt'), 'w') as f:
        f.write('\n'.join(report))

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Heteroskedasticity Analysis for Event Study')
    parser.add_argument('results_file', type=str, help='Path to comprehensive analysis results JSON file')
    parser.add_argument('--output', type=str, default='results', help='Output directory for results')
    args = parser.parse_args()
    
    # Resolve paths
    workspace_dir = Path(__file__).resolve().parents[2]
    
    results_file = args.results_file
    if not os.path.isabs(results_file) and not results_file.startswith('./') and not results_file.startswith('../'):
        results_file = os.path.join(workspace_dir, 'results', results_file)
    
    output_dir = os.path.join(workspace_dir, args.output)
    
    # Run analysis
    analyze_heteroskedasticity(results_file, output_dir)

if __name__ == "__main__":
    main() 