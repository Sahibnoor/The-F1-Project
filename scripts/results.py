import requests
import time
import sys
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import extras
import random
import numpy as np

# My DB and Session configurations
DB_HOST = "database-1.cxm64uci8hxe.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "OnePieceReal.7143"
DB_SSLMODE = "require"

def conn():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        port=5432, sslmode=DB_SSLMODE
    )

def get_race_links_from_psql(start_year: int, end_year: int):
    """
    Retrieves race links from the 'races' table in PostgreSQL for a given year range.
    """
    query = """
    SELECT year, race_link, grand_prix
    FROM races
    WHERE year >= %s AND year <= %s
    ORDER BY year, date ASC;
    """
    
    with conn() as connection:
        with connection.cursor() as cur:
            cur.execute(query, (start_year, end_year))
            links = cur.fetchall()
            return links

def scrape_race_details(race_link: str, year: int, grand_prix: str):
    """
    Scrapes detailed results from a specific race page.
    
    Args:
        race_link (str): The full URL for the race.
        year (int): The year of the race.
        grand_prix (str): The name of the Grand Prix.

    Returns:
        A pandas DataFrame with the detailed results.
    """
    print("\n")
    print(f"Year: {year}")
    print(f"Grand Prix: {grand_prix}")

    try:
        response = requests.get(race_link, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'lxml')
        
        main = soup.find('main')
        table = main.find('table')
        th_tag = table.find_all('th')

        headers = ["YEAR", "GRAND PRIX"]        # Creating a list for dataframe headers

        for th in th_tag:
            p_tag = th.find('p')
            header_data = p_tag.get_text()
            headers.append(header_data)

        table_rows = table.find_all('tr')
        
        all_rows = []                       # list to store all the rows of the table
        
        for row in table_rows:
            cells = row.find_all('td')
            n_row = [year, grand_prix]      # list to store each row, which we'll then append into the all_rows list
            for cell in cells:
                row_p = cell.find('p')
                row_data = row_p.get_text()
                n_row.append(row_data)

            all_rows.append(n_row)
        table_df = pd.DataFrame(all_rows, columns=headers)
        table_df['DRIVER'] = table_df['DRIVER'].str[:-3]
        # print(table_df["DRIVER"])
        return table_df

    except requests.exceptions.RequestException as e:
        print(f"[{grand_prix}, {year}] Error accessing URL: {e}")
        return pd.DataFrame()

def clean_race_details(df):
    """
    Cleans and standardizes the detailed race results DataFrame.
    """
    if df.empty:
        return pd.DataFrame()
    
    df.dropna(subset=["DRIVER"], inplace=True)

    # Define the final column names to match the psql table schema
    final_columns = [
        'year', 'grand_prix', 'pos', 'car_number', 'driver_first_name', 
        'driver_last_name', 'team', 'laps', 'time', 'points'
    ]

    # Add the first name and last name columns at their appropriate positions
    og_headers = list(df.columns)
    driver_index = og_headers.index("DRIVER")

    new_name = ["driver_first_name", "driver_last_name"]
    og_headers[driver_index:driver_index] = new_name

    # Splitting the driver name inot their first and last names
    df['driver_first_name'] = df['DRIVER'].str.strip().str.split(expand=True)[0]
    df['driver_last_name'] = df['DRIVER'].str.strip().str.split(expand=True)[1]

    # Reordering the df, in accordance with the psql table
    df = df[og_headers]
    df = df.drop('DRIVER', axis=1)
    df.columns = final_columns

    for col in df.columns:
        df[col] = df[col].replace('', None, regex=True)
    
    indexTime = df[ df["time"] == "SHC"].index
    
    # df.drop(indexTime, inplace=True)
    
    df = df[df['time'] != "SHC"]

    return df

def upsert_to_psql(df, table_name: str, conflict_target: tuple):
    """
    Upserts DataFrame records into a PostgreSQL table.
    """
    if df.empty:
        print("DataFrame is empty, nothing to upsert.")
        return
        
    with conn() as connection:
        with connection.cursor() as cur:
            try:
                # Convert DataFrame to a list of tuples for bulk insertion
                df_tuples = [tuple(row) for row in df.itertuples(index=False)]

                # Dynamically create the column list for the SQL query
                columns = ', '.join(f'"{col}"' for col in df.columns)
                
                # Define the update columns for the ON CONFLICT clause
                update_cols_str = ', '. join(f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in conflict_target)
                
                # Build the upsert query
                upsert_query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES %s
                ON CONFLICT {str(conflict_target).replace("'", '"')} DO UPDATE SET
                {update_cols_str};
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

            except Exception as e:
                print(f"An error occurred during upsert: {e}")
                connection.rollback()


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Error: Please provide a year or a range of years to scrape.")
        print("Usage: python race_details_scraper.py <YEAR> [END_YEAR]")
        sys.exit(1)

    try:
        start_year = int(sys.argv[1])
        end_year = int(sys.argv[2]) if len(sys.argv) > 2 else start_year
    except ValueError:
        print("Invalid years provided. Please enter valid integers.")
        sys.exit(1)
    
    # Get all race links from the 'races' table for the specified year range
    race_links = get_race_links_from_psql(start_year, end_year)

    if not race_links:
        print(f"No race links found in the 'races' table for years {start_year}-{end_year}. Please run the races scraper first.")
        sys.exit(0)

    for year, link, grand_prix in race_links:
        detailed_data_df = scrape_race_details(link, year, grand_prix)
        if not detailed_data_df.empty:
            cleaned_df = clean_race_details(detailed_data_df)
            upsert_to_psql(cleaned_df, "race_results", ("year", "grand_prix", "car_number"))
        
        # Add a delay between requests to be polite to the server
        time.sleep(random.randint(2, 5))
