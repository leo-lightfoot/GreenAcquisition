import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib.parse

# Function to get ticker directly from Yahoo Finance
def get_ticker(company_name):
    # URL encode the company name for the search
    encoded_name = urllib.parse.quote_plus(company_name)
    
    # Create the Yahoo Finance search URL for the company
    search_url = f"https://finance.yahoo.com/lookup?s={encoded_name}"
    
    # Set a user agent to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Send HTTP request with headers
        response = requests.get(search_url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the page content
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find the table containing search results
            table = soup.find('table', {'class': 'W(100%)'})
            
            if table:
                # Find the first row with data
                rows = table.find_all('tr')
                if len(rows) > 1:  # Skip header row
                    # First column should contain the ticker symbol
                    first_cell = rows[1].find_all('td')[0]
                    if first_cell:
                        ticker_link = first_cell.find('a')
                        if ticker_link:
                            return ticker_link.text.strip()
            
            # Try alternative method - look for any table with search results
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) > 1:
                    cells = rows[1].find_all('td')
                    if cells and len(cells) > 0:
                        ticker_element = cells[0].find('a')
                        if ticker_element:
                            return ticker_element.text.strip()
        
        # If we can't find the ticker, return "Not Found"
        return "Ticker not found"
    
    except Exception as e:
        # Handle any exceptions (network issues, etc.)
        print(f"Error retrieving data for {company_name}: {e}")
        return f"Error: {str(e)}"

# Load company names from a CSV file
file_path = "Bloomberg_M&A_Data.csv"  # Ensure this file contains a column "Company Name"
df = pd.read_csv(file_path)

# File to save the results
output_file = "Extracted_company_tickers.csv"

# Iterate over the companies and append tickers as we go
for index, row in df.iterrows():
    company_name = row["Company Name"]
    ticker = get_ticker(company_name)
    
    # Append the result to the CSV file
    result_df = pd.DataFrame([[company_name, ticker]], columns=["Company Name", "Ticker"])
    
    # Append to CSV, write header only once
    result_df.to_csv(output_file, mode='a', header=not pd.io.common.file_exists(output_file), index=False)

    print(f"Processed {company_name} - Ticker: {ticker}")

    # Add a delay to avoid rate limiting
    time.sleep(2)

print("Ticker extraction complete. Results are being appended to 'company_tickers.csv'.")