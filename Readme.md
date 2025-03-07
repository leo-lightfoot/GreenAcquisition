GreenAcquisition/
├── data/
│   ├── raw/
│   │   ├── Bloomberg_MA_Data.csv
│   │   └── GHG.csv
│   │
│   ├── interim/
│   │   ├── company_tickers.csv
│   │   ├── financial_metrics.csv
│   │   └── master_data_raw.csv
│   │
│   └── processed/
│       ├── master_data_clean.csv
│       └── master_data_classified.csv
│
├── scripts/
│   ├── 1_data_collection/
│   │   ├── fetch_tickers.py
│   │   ├── fetch_financial_metrics.py
│   │   └── fetch_stock_prices.py
│   │
│   └── 2_data_processing/
│       ├── merge_data.py
│       ├── clean_data.py   
│       └── GB_classification.py
│
└── README.md


##############################################################################################################################################

Green Acquisition Data Analysis Project

## Project Overview
This project processes and analyzes merger and acquisition (M&A) data with a focus on environmental metrics. It transforms raw Bloomberg M&A data into a structured dataset enriched with financial metrics, stock prices, and environmental classifications.


## Data Processing Pipeline

### 1. Data Collection

Bloomberg_MA_Data.csv (filter:)

1. **Fetch Company Tickers** (`fetch_tickers.py`)
   - Input: `Bloomberg_MA_Data.csv`
   - Output: `company_tickers.csv`
   - Function: Extracts company tickers from Yahoo Finance

2. **Fetch Financial Metrics** (`fetch_financial_metrics.py`)
   - Input: `ma_data_with_tickers.csv`
   - Output: `financial_metrics.csv`
   - Function: Retrieves market cap, debt/equity ratio, and ROA

3. **Fetch Stock Prices** (`fetch_stock_prices.py`)
   - Input: `ma_data_with_tickers.csv`
   - Output: `stock_prices.csv`
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
   - Calculates carbon intensity
   - Classifies companies as Green/Brown
   - Performs target company classification
   - Output: `master_data_classified.csv`



1. Fetch raw data files
2. Run scripts in sequence:

# 1. Data Collection
python scripts/1_data_collection/fetch_tickers.py
python scripts/1_data_collection/fetch_financial_metrics.py
python scripts/1_data_collection/fetch_stock_prices.py

# 2. Data Preprocessing
python scripts/2_data_processing/merge_data.py
python scripts/3_data_analysis/GB_classification.py
python scripts/2_data_processing/clean_data.py
