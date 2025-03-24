# GreenAcquisition

This repository contains scripts and data for analyzing the environmental impact of mergers and acquisitions in the energy sector, with a focus on green vs. brown company classifications based on carbon intensity.

## Project Overview

This project analyzes how the carbon intensity of acquiring companies affects their stock market performance around merger and acquisition (M&A) announcements. The analysis covers energy sector M&A transactions between January 2014 and December 2024.

## Data Sources

1. **M&A Transaction Data**: Sourced from Bloomberg, containing all energy sector M&A transactions between January 2014 and December 2024.
   
2. **GHG Emissions Data**: Collected from WRDS (Wharton Research Data Services), providing greenhouse gas emissions for companies.
   
3. **Sales Data**: Obtained from Bloomberg's Equity section, using the year prior to the announcement date as the reference year.
   
4. **Stock Price Data**: Retrieved from Yahoo Finance API for specific trading days before and after M&A announcements.

## Project Structure

```
GreenAcquisition/
├── SCRIPTS/
│   ├── data_collection/           # Scripts for collecting raw data
│   │   └── fetch_stock_prices.py  # Script to fetch stock prices around M&A dates
│   │
│   └── data_preprocessing/        # Scripts for cleaning and processing data
│       ├── merge_raw_all_sources.py    # Merge data from various sources
│       ├── greenbrown_classification.py # Classify companies as green or brown
│       └── standardize_stock_data.py    # Standardize stock price data format
│
├── data/
│   ├── 1_raw/      # Original unprocessed data
│   ├── 2_interim/  # Intermediate data that has been transformed
│   └── 3_processed/# Final processed data ready for analysis
│
└── logs/           # Log files from script executions
```

## Data Processing Pipeline

The data processing follows a comprehensive multi-stage pipeline:

1. **Data Collection**: 
   - Collect M&A transaction data from Bloomberg for the energy sector
   - Obtain GHG emissions data from WRDS
   - Gather annual sales data from Bloomberg's Equity section
   - Fetch stock prices from Yahoo Finance for specific time windows around announcement dates

2. **Data Merging**: 
   - Combine data from all three primary sources (M&A, emissions, sales)
   - Match records based on ticker symbols and reference years
   - Calculate carbon intensity metrics
   - Remove records with missing critical data (resulting in approximately 830 complete data points)

3. **Classification**: 
   - Classify acquirer companies as "Green" or "Brown" based on carbon intensity quantiles
   - Categorize acquisition targets as "Green," "Brown," or "Neutral" based on keyword analysis

4. **Stock Price Analysis**:
   - Retrieve stock prices for specific trading days before and after announcement dates
   - Calculate returns over the event windows

5. **Standardization**: 
   - Clean and standardize data formats for analysis
   - Ensure consistent naming conventions and data types
   - Add proper denominations to numeric columns

## Methodology

### Carbon Intensity Classification

1. **Acquirer Classification**:
   - Calculate carbon intensity as the ratio of GHG emissions to annual sales
   - Companies in the top quantile (lowest emissions) are classified as "Green"
   - Remaining companies are classified as "Brown"

2. **Target Classification**:
   - Analyze target company names and descriptions using keyword matching
   - **Brown Classification**: Contains fossil fuel-related keywords (coal, oil, gas, etc.)
   - **Green Classification**: Contains renewable energy-related keywords (solar, wind, sustainable, etc.)
   - **Neutral Classification**: No clear indicators of either category

### Stock Price Event Study

- **Event Window**: Trading days before and after M&A announcement dates
- **Price Data**: Closing prices for T-n and T+n days (where T is announcement date)
- **Performance Metric**: Percentage return over specified event windows

## Scripts Documentation

### Data Collection

#### `fetch_stock_prices.py`

This script fetches stock prices for companies around merger and acquisition announcement dates. It uses Yahoo Finance API to retrieve historical stock data, which is the most time-consuming process when fetching fresh data.

**Features:**
- Fetches stock prices for specified T-day windows (e.g., T-3, T+10)
- Handles missing or invalid ticker symbols
- Includes retry logic for API failures
- Supports parallel processing for faster data retrieval
- Logs progress and errors

**Usage:**
```bash
python SCRIPTS/data_collection/fetch_stock_prices.py --days_before 10 --days_after 10
```
or
```bash
python SCRIPTS/data_collection/fetch_stock_prices.py --days_before 3 --days_after 3
```

**Command-line Arguments:**
- `--days_before`: Number of trading days before announcement (default: 10)
- `--days_after`: Number of trading days after announcement (default: 10)

### Data Preprocessing

#### `merge_raw_all_sources.py`

This script merges data from multiple sources to create a comprehensive dataset for analysis.

**Data Sources:**
- M&A data with ticker symbols from Bloomberg
- GHG emissions data from WRDS
- Annual sales data from Bloomberg Equity section

**Processing Steps:**
1. Loads M&A data with tickers
2. Loads GHG emissions data
3. Loads sales data for the year prior to announcement date
4. Merges all data sources based on ticker and reference year
5. Calculates carbon intensity as the ratio of GHG emissions to annual sales
6. Removes entries with missing critical data
7. Saves the merged dataset to a CSV file

**Output:**
- `master_data_merged.csv` in the `data/1_raw` directory

#### `greenbrown_classification.py`

This script classifies companies as "Green" or "Brown" based on their carbon intensity and identifies acquisition targets by industry keywords.

**Classification Methods:**
- **Acquirer Classification**: Companies are classified as "Green" if their carbon intensity (GHG emissions / Annual Sales) is in the lowest quantile, otherwise "Brown"
- **Target Classification**: Companies are classified based on keywords in their names and descriptions:
  - **Brown**: Contains fossil fuel-related keywords (coal, oil, gas, etc.)
  - **Green**: Contains renewable energy-related keywords (solar, wind, sustainable, etc.)
  - **Neutral**: No clear indicators of either category

**Output:**
- `master_data_classified.csv` in the `data/2_interim` directory

#### `standardize_stock_data.py`

This script standardizes the stock price data format to ensure consistency across different time windows as a final data preprocessing step.

**Features:**
- Extracts day value from filenames (e.g., 3-day, 10-day)
- Maps and standardizes column names regardless of input format
- Handles duplicate columns and resolves naming conflicts
- Formats numeric values with appropriate precision
- Parses and standardizes date formats
- Adds denomination information to column headers (e.g., dollars, metric tonnes)

**Usage:**
```bash
python SCRIPTS/data_preprocessing/standardize_stock_data.py master_data_with_stock_prices_10day.csv
```
or
```bash
python SCRIPTS/data_preprocessing/standardize_stock_data.py master_data_with_stock_prices_3day.csv
```

**Output:**
- Standardized CSV files in the `data/3_processed` directory (e.g., `standardized_stock_data_10day.csv`)

## Data Files

### Raw Data
- `bloomberg_ma_with_tickers.csv` - M&A transaction data with company tickers from Bloomberg's M&A section
- `ghg.csv` - Greenhouse gas emissions data from WRDS
- `sales_data_bbg.csv` - Annual sales data from Bloomberg's Equity section

### Interim Data
- `master_data_merged.csv` - Combined data from all sources (~830 complete records)
- `master_data_classified.csv` - Data with green/brown classifications for both acquirers and targets
- `master_data_with_stock_prices_3day.csv` - M&A data with 3-day stock price windows
- `master_data_with_stock_prices_10day.csv` - M&A data with 10-day stock price windows

### Processed Data
- `standardized_stock_data_3day.csv` - Standardized 3-day stock price data
- `standardized_stock_data_10day.csv` - Standardized 10-day stock price data

## Column Definitions

### Key Columns
- `Announce Date` - Date when the M&A was announced
- `Reference_Year` - The year used for matching with emissions and financial data (year prior to announcement)
- `Ticker` - Company ticker symbol
- `Acquirer Name` - Name of the acquiring company
- `Target Name` - Name of the company being acquired
- `Acquirer_GHG_Emissions (Metric Tonnes)` - Greenhouse gas emissions of acquirer
- `Annual_Sales (Million $)` - Annual sales of acquirer
- `Carbon_Intensity` - Ratio of GHG emissions to sales
- `Acquirer_Classification` - Green or Brown classification based on carbon intensity
- `Target_Classification` - Green, Brown, or Neutral classification based on industry keywords
- `T_minus_X_Date` - Trading date X days before announcement
- `T_minus_X_Price ($)` - Stock price X days before announcement
- `T_plus_X_Date` - Trading date X days after announcement
- `T_plus_X_Price ($)` - Stock price X days after announcement
- `Percent_Return` - Percentage return over the event window

## License

This project is licensed under the terms of the LICENSE file included in this repository.
