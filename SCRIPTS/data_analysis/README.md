# Event Study Analysis Script

This script performs an event study analysis to investigate how M&A announcements affect stock prices, with a particular focus on green versus brown company acquisitions in the energy sector.

## Overview

The script analyzes three distinct groups:
1. All M&A deals
2. Any company acquiring a green company
3. Brown companies acquiring green companies

For each group, the script:
- Calculates abnormal returns by comparing actual returns to benchmark returns
- Performs statistical analysis (mean, standard deviation, t-tests)
- Calculates market cap weighted returns
- Analyzes the relationship between abnormal returns and carbon intensity
- Generates visualizations of the results

## Features

- Automatically detects the event window period (3-day, 10-day, etc.) from the input file name
- Dynamically adjusts to different column naming conventions in the input data
- Produces comprehensive statistical analyses for different acquisition types
- Generates visualization for easy interpretation of results
- Supports comparison between different event windows

## Usage

```bash
python event_study_analysis.py <data_file> [--output <output_directory>]
```

### Arguments:

- `data_file`: The name of the standardized stock data file (e.g., standardized_stock_data_3day.csv, standardized_stock_data_10day.csv)
- `--output`: (Optional) The directory where results will be stored (default: "results")

### Examples:

```bash
# For 3-day event window analysis
python event_study_analysis.py standardized_stock_data_3day.csv

# For 10-day event window analysis
python event_study_analysis.py standardized_stock_data_10day.csv
```

### Batch Processing

You can use the included batch file to run the analysis on both 3-day and 10-day windows consecutively:

```bash
.\run_event_study.bat
```

## Output

The script saves the following outputs to timestamped directories in the results folder:

1. CSV files with calculated abnormal returns for each analysis group
2. JSON files containing statistical analysis results
3. JSON files containing scenario analysis results
4. Visualizations (stored in a `visualizations` subdirectory):
   - Distribution of abnormal returns
   - Scatter plots of carbon intensity vs. abnormal returns
   - Comparative bar charts of mean abnormal returns across groups

The output directory names include the day range (e.g., `event_study_3day_20250324_034036`) for easy identification.

## Interpreting Results

When comparing results across different event windows (3-day vs 10-day):
- Shorter windows (3-day) typically capture the immediate market reaction to the announcement
- Longer windows (10-day) may capture more of the delayed market response and information leakage
- Differences between the two can provide insights into how quickly the market processes and reacts to M&A information

## Requirements

The script requires the following Python packages:
- pandas
- numpy
- matplotlib
- seaborn
- statsmodels
- scipy 