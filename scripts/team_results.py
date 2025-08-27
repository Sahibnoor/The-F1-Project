# Getting team results for each year

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

def scrape_team_results(link, year):
    """
    Scrapes team standings for each year.
    
    Args:
        race_link (str): The full URL for the race.
        year (int): The year of the race.
        grand_prix (str): The name of the Grand Prix.

    Returns:
        A pandas DataFrame with the detailed results.
    """
    print("\n")
    print(f"{year} Team Standings")

    try:
            response = requests.get(link, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'lxml')
            
            main = soup.find('main')
            table = main.find('table')
            th_tag = table.find_all('th')

            headers = ["YEAR"]        # Creating a list for dataframe headers

            for th in th_tag:
                p_tag = th.find('p')
                header_data = p_tag.get_text()
                headers.append(header_data)

            table_rows = table.find_all('tr')
            
            all_rows = []                       # list to store all the rows of the table
            
            for row in table_rows:
                cells = row.find_all('td')
                n_row = [year]      # list to store each row, which we'll then append into the all_rows list
                for cell in cells:
                    row_p = cell.find('p')
                    row_data = row_p.get_text()
                    n_row.append(row_data)

                all_rows.append(n_row)
            table_df = pd.DataFrame(all_rows, columns=headers)

            time.sleep(random.randint(3,10))

            return table_df

    except requests.exceptions.RequestException as e:
            print(f"[{year}] Error accessing URL: {e}")
            time.sleep(random.randint(3,10))
            return pd.DataFrame()

        

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
            # Assuming a composite key of (year, team)
            conflict_target = '(year, team)'
            
            # Define the update columns for the ON CONFLICT clause
            update_columns = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in ['year', 'team'])

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

def clean_team_data(df):
    
    # Reorder the columns for a cleaner output and convert to lowercase for psql
    new_column_order = ['year', 'pos', 'team', 'points']
    
    df.columns = new_column_order
    df['points'] = df['points'].astype('float')

    df['pos'] = pd.to_numeric(df['pos'], errors='coerce').fillna(0).astype(int)

    df.dropna(subset=["team"], inplace=True)

    # print("Final DataFrame:")
    # print(df)

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
    
    for year in range(s, e+1):
        link = f"https://www.formula1.com/en/results/{year}/team"
        df_to_upsert = scrape_team_results(link, year)
        upsert_to_psql(clean_team_data(df_to_upsert), "team_standings")
