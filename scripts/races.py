# A dynamic script to access multiple Formula 1 race result pages.
# This script uses a function to make it reusable for different years.

import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
import re
import requests_cache
import psycopg2
from psycopg2.extras import execute_values
import unicodedata
from datetime import datetime
from psycopg2 import extras
import random

# My DB and Session configurations
DB_HOST = "database-1.cxm64uci8hxe.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "OnePieceReal.7143"
DB_SSLMODE = "require"
BASE = "https://www.formula1.com"

def conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        port=5432, sslmode=DB_SSLMODE
    )

def parse_year_data(response, year):
    soup = BeautifulSoup(response.text, 'lxml')
    
    all_race_data = []
    
    # Find the first main tag on the page.
    main_content_div = soup.find('main')
                    
    if main_content_div:
        # Find the first table within that main tag.
        table = main_content_div.find('table')
                        
        if table:
            # Find the table headers to use as DataFrame columns.
            headers = [th.text.strip() for th in table.find_all('th')]
                            
            # Find all table rows (tr) within the table body (tbody).
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
                            
            # Iterate over the rows and extract the data from each cell.
            for row in rows:
                cells = row.find_all('td')
                row_data = [cell.text.strip() for cell in cells]
                
                # Check for empty strings in data and replace with None for database compatibility
                row_data = [None if x == '' else x for x in row_data]
                                
                # Find the <a> tag inside the Grand Prix cell to get the link
                grand_prix_cell = row.find('td')
                if grand_prix_cell:
                    grand_prix_link = grand_prix_cell.find('a', href=True)
                    if grand_prix_link:
                        row_data.append(grand_prix_link.get('href'))
                    else:
                        row_data.append(None) # Add a None if no link is found
                else:
                    row_data.append(None) # Add a None if no Grand Prix cell is found
                                
                # Add the year to each row of data for context.
                row_data.append(year)
                all_race_data.append(row_data)

            # Add 'Year' to the headers list so it's a column in the DataFrame
            headers.append('Race Link')
            headers.append('Year')
            
            return pd.DataFrame(all_race_data, columns=headers)
        else:
            print(f"Could not find a table within the main content div for {year}.")

    else:
        print(f"Could not find the main content div for {year}.")
    
    return pd.DataFrame()


def scrape_f1_races(start_year: int, end_year: int):
    """
    Constructs the URL for a given year and attempts to access it.
    
    Args:
        year (int): The year for which to access the race results.
        
    Returns:
        A requests.Response object if successful, otherwise None.
    """
    
    # Dynamically construct the URL for each year.
    for year in range(start_year, end_year + 1):
        url = f"https://www.formula1.com/en/results/{year}/races"
        print(f"Attempting to access races for the year: {year} at URL: {url}\n")
    
        try:
            # Send a GET request with a timeout.
            response = requests.get(url, timeout=10)
            
            # Raise an exception for bad status codes (4xx or 5xx).
            response.raise_for_status()
            
            print(f"Request successful! Status Code: {response.status_code}")
            
            race_data = parse_year_data(response, year)
            if not race_data.empty:
                # Remove duplicate rows before cleaning and upserting
                race_data.drop_duplicates(subset=['Race Link', 'Year'], keep='first', inplace=True)
                
                race_data = clean_race_data(race_data)
                upsert_to_psql(race_data, "races")
                
        except Exception as e:
            print(f"[{year}] Error: {e}")
        
        # Add a small delay between requests to be polite to the server.
        time.sleep(random.randint(3,10))

def upsert_to_psql(df, table_name):
    """
    Upserts DataFrame records into a PostgreSQL table.
    """
    with conn() as connection:
        with connection.cursor() as cur:

            # Convert DataFrame to a list of tuples for bulk insertion
            df_tuples = [tuple(row) for row in df.itertuples(index=False)]

            # Dynamically create the column list for the SQL query
            columns = ', '.join(f'"{col}"' for col in df.columns)
            
            # Define the primary key for the ON CONFLICT clause
            # Assuming a composite key of (year, grand_prix)
            conflict_target = '(year, grand_prix)'
            
            # Define the update columns for the ON CONFLICT clause
            update_columns = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in ['year', 'grand_prix'])

            upsert_query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES %s
            ON CONFLICT {conflict_target} DO UPDATE SET
            {update_columns};
            """
            
            # Execute the upsert using execute_values for efficiency
            extras.execute_values(
                cur,
                upsert_query,
                df_tuples,
                page_size=100
            )
            
            connection.commit()
            print(f"Successfully upserted {len(df_tuples)} rows into {table_name}.")

    
def clean_race_data(df):
    
    # 1. Clean up and standardize the 'Grand Prix' column.
    # We will create a new 'Grand Prix' column based on the cleaned link.
    if 'Race Link' in df.columns:
        df['Grand Prix'] = df['Race Link'].str.split('/').str[-2]
        df['Grand Prix'] = df['Grand Prix'].str.replace('-', ' ').str.title()
        df.rename(columns={'Grand Prix': 'grand_prix'}, inplace=True)
    else:
        # Handle cases where the Grand Prix link column is missing
        df['grand_prix'] = None

    # 2. Split the 'Winner' column into separate columns.
    # We'll first extract the driver's three-letter code.
    if 'WINNER' in df.columns:
        df['Driver Code'] = df['WINNER'].str[-3:]

        # Then, we'll get the full name by removing the code and splitting by space.
        df['Full Name'] = df['WINNER'].str[:-3]
        name_parts = df['Full Name'].str.strip().str.split(expand=True)
        df['First Name'] = name_parts[0]
        df['Last Name'] = name_parts[1].fillna('')
    else:
        df['Driver Code'] = None
        df['First Name'] = None
        df['Last Name'] = None

    # 3. Clean up the 'Race Link' column to create a complete URL.
    if 'Race Link' in df.columns:
        base_url = 'https://www.formula1.com'
        df['Race Link'] = df['Race Link'].str.replace('/../..', '')
        df['Race Link'] = base_url + df['Race Link']

    # 4. Drop the temporary 'Winner' and 'Full Name' columns.
    if 'WINNER' in df.columns:
        df = df.drop(columns=['WINNER', 'Full Name'])

    # Reorder the columns for a cleaner output and convert to lowercase for psql
    new_column_order = ['Year', 'grand_prix', 'DATE', 'First Name', 'Last Name', 'Driver Code', 'TEAM', 'LAPS', 'TIME', 'Race Link']
    
    # Check for missing columns and handle them gracefully
    missing_cols = set(new_column_order) - set(df.columns)
    for c in missing_cols:
        df[c] = None
        
    df = df[new_column_order]
    
    # Convert all column names to lowercase to match the psql table schema
    df.columns = [col.lower() for col in df.columns]
    
    # After converting all columns to lowercase, correct the `grand_prix` column
    # because it will have a space
    df.columns = df.columns.str.replace(' ', '_')
    
    return df

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3:
        s, e = int(sys.argv[1]), int(sys.argv[2])
    elif len(sys.argv) == 2:
        s = e = int(sys.argv[1])
    else:
        print("Usage: python scrape_races.py <YEAR> [<END_YEAR>]")
        sys.exit(1)
    scrape_f1_races(s, e)
