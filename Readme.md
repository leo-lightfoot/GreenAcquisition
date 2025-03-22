GreenAcquisition/
├── data/
│   ├── raw/
│   │   ├── Bloomberg_MA_Data.csv
│   │   └── GHG.csv
│   │
│   ├── interim/
│   │   ├── company_tickers.csv
│   │   ├── financial_metrics.csv
│   │   ├── sales_data.csv
│   │   └── master_data_raw.csv
│   │
│   └── processed/
│       ├── master_data_clean.csv
│       ├── master_data_classified.csv
│       ├── results/
│       │   ├── all_energy_deals_results.csv
│       │   └── brown_acquires_green_results.csv
│       └── plots/
│           ├── CAR_vs_CI_All_Energy_Sector_MAs.png
│           └── CAR_vs_CI_Brown_Acquires_Green.png
│
├── scripts/
│   ├── 1_data_collection/
│   │   ├── fetch_tickers.py
│   │   ├── fetch_financial_metrics.py
│   │   ├── fetch_sales_data.py
│   │   └── fetch_stock_prices.py
│   │
│   └── 2_data_processing/
│       ├── merge_data.py
│       ├── clean_data.py   
│       └── GB_classification.py
│
├── run_pipeline.py
├── test.py
└── README.md


##############################################################################################################################################

# Green Acquisition Data Analysis Project

## Project Overview
This project processes and analyzes merger and acquisition (M&A) data with a focus on environmental metrics. It transforms raw Bloomberg M&A data into a structured dataset enriched with financial metrics, stock prices, sales data, and environmental classifications.

## Key Features
- Calculates carbon intensity as emissions/sales (instead of emissions/market cap)
- Performs event study analysis on M&A deals
- Conducts regression analysis of Cumulative Abnormal Returns (CAR) on carbon intensity
- Generates visualizations of the relationship between carbon intensity and abnormal returns

## Data Processing Pipeline

### 1. Data Collection

1. **Fetch Company Tickers** (`fetch_tickers.py`)
   - Input: `Bloomberg_MA_Data.csv`
   - Output: `company_tickers.csv`
   - Function: Extracts company tickers from Yahoo Finance

2. **Fetch Financial Metrics** (`fetch_financial_metrics.py`)
   - Input: `company_tickers.csv`
   - Output: `financial_metrics.csv`
   - Function: Retrieves market cap, debt/equity ratio, and ROA

3. **Fetch Sales Data** (`fetch_sales_data.py`) - NEW
   - Input: `company_tickers.csv`
   - Output: `sales_data.csv`
   - Function: Retrieves annual sales data for carbon intensity calculation

4. **Fetch Stock Prices** (`fetch_stock_prices.py`)
   - Input: `company_tickers.csv`
   - Output: Stock price data for event analysis
   - Function: Gets stock prices for T-10 and T+10 days around announcement

### 2. Data Preprocessing
1. **Merge Data** (`merge_data.py`)
   - Combines all collected data into a single master dataset
   - Performs initial data cleaning and standardization
   - Output: `master_data_raw.csv`

2. **Clean Data** (`clean_data.py`)
   - Handles missing values
   - Standardizes formats (dates, numbers)
   - Removes duplicates
   - Output: `master_data_clean.csv`

3. **Classify Companies** (`GB_classification.py`)
   - Calculates carbon intensity (emissions/sales)
   - Classifies companies as Green/Brown
   - Performs target company classification
   - Output: `master_data_classified.csv`

### 3. Analysis
1. **Event Study Analysis** (`test.py`)
   - Calculates abnormal returns around M&A announcements
   - Performs statistical tests on abnormal returns
   - Conducts regression analysis of CAR on carbon intensity
   - Generates visualizations
   - Output: Results CSV files and plots

## How to Run

### Using the Pipeline Script
The project includes a pipeline script that automates the entire workflow:

```bash
# Run the entire pipeline
python run_pipeline.py

# Skip data collection if you already have the data
python run_pipeline.py --skip-data-collection

# Skip sales data collection if you don't need to update it
python run_pipeline.py --skip-sales-data

# Skip preprocessing if you only want to run the analysis
python run_pipeline.py --skip-preprocessing --skip-data-collection

# Only run the analysis
python run_pipeline.py --skip-data-collection --skip-preprocessing
```

### Manual Execution
You can also run each script individually in the following order:

```bash
# 1. Data Collection
python SCRIPTS/1.\ DATA\ COLLECTION/fetch_tickers.py
python SCRIPTS/1.\ DATA\ COLLECTION/fetch_financial_metrics.py
python SCRIPTS/1.\ DATA\ COLLECTION/fetch_sales_data.py
python SCRIPTS/1.\ DATA\ COLLECTION/fetch_stock_prices.py

# 2. Data Preprocessing
python SCRIPTS/2.\ DATA\ PREPROCESSING/merge_data.py
python SCRIPTS/2.\ DATA\ PREPROCESSING/clean_data.py
python SCRIPTS/2.\ DATA\ PREPROCESSING/GB_classification.py

# 3. Analysis
python test.py
```

## Results
The analysis results are saved in the following locations:
- CSV files: `DATA/3. PROCESSED/results/`
- Plots: `DATA/3. PROCESSED/plots/`

## Dependencies
- pandas
- numpy
- yfinance
- statsmodels
- matplotlib
- seaborn
