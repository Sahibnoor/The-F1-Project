import time
import re
import pandas as pd
import requests
import requests_cache
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras as extras
import unicodedata
from urllib.parse import urljoin

# Assumes DB_HOST, DB_NAME, etc., and utility functions are defined elsewhere
DB_HOST = "database-1.cxm64uci8hxe.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "OnePieceReal.7143"
DB_SSLMODE = "require"

BASE = "https://www.formula1.com"

# --- polite client: caching + rate-limit ---
requests_cache.install_cache("f1_cache", expire_after=24*3600)  # 24h
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "F1ApexAnalyst/1.0 (academic/personal project; contact: you@example.com)"
})
REQUEST_DELAY = 1.0  # seconds between requests

def get(url):
    """GET with cache + delay when not cached."""
    resp = SESSION.get(url, timeout=30)
    if not getattr(resp, "from_cache", False):
        time.sleep(REQUEST_DELAY)
    resp.raise_for_status()
    return resp

def conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        port=5432, sslmode=DB_SSLMODE
    )

def normalize_space(s):
    return re.sub(r"\s+", " ", s or "").strip()

def normalize_unicode(text):
    if not text:
        return ""
    normalized = unicodedata.normalize('NFKD', text)
    return normalized.encode('ascii', 'ignore').decode('utf-8')

def parse_race_result_page(url, year, round_):
    """
    Parses a single race results page and returns a list of dictionaries
    containing race metadata and results.
    """
    try:
        resp = get(url)
        soup = BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        print(f"Error fetching or parsing {url}: {e}")
        return None, []

    # --- Section 1: Scrape Race Metadata ---
    h1 = soup.select_one("h1") or soup.select_one("header h1")
    grand_prix = normalize_space(h1.get_text()) if h1 else None
    
    header_txt = normalize_space((soup.select_one("header") or soup).get_text())
    mdate = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", header_txt)
    date_iso = None
    if mdate:
        from datetime import datetime
        try:
            date_iso = datetime.strptime(mdate.group(1), "%d %B %Y").date().isoformat()
        except Exception:
            pass

    circuit_tag = soup.select_one("p.typography-module_body-xs-semibold__Fyfwn")
    circuit = None
    country = None
    if circuit_tag:
        circuit_text = circuit_tag.get_text(" ", strip=True)
        if "," in circuit_text:
            parts = circuit_text.split(",")
            circuit = parts[0].strip()
            if len(parts) > 1:
                country = parts[1].strip()
    
    grand_prix = normalize_unicode(grand_prix)
    country = normalize_unicode(country)
    circuit = normalize_unicode(circuit)

    meta = {
        "grand_prix": grand_prix,
        "country": country,
        "date": date_iso,
        "circuit": circuit,
        "year": year,
        "round": round_
    }

    # --- Section 2: Find and Parse the Results Table ---
    tables = soup.select("table")
    table = None
    for t in tables:
        heads = [normalize_space(th.get_text()) for th in t.select("thead th")]
        if any(h.lower().startswith("pos") for h in heads):
            table = t
            break
    
    if table is None:
        return meta, []

    headers = [normalize_space(th.get_text()) for th in table.select("thead th")]
    header_map = {header: i for i, header in enumerate(headers)}
    
    results_rows = []
    for tr in table.select("tbody tr"):
        tds = tr.select("td")
        if not tds: continue

        driver_cell = tds[header_map.get("Driver")]
        driver_name_span = driver_cell.select_one('span:not(.d-lg-inline)')
        driver_code_span = driver_cell.select_one('span.d-lg-inline')

        driver_name = driver_name_span.get_text(strip=True) if driver_name_span else None
        driver_code = driver_code_span.get_text(strip=True) if driver_code_span else None

        pos_text = normalize_space(tds[header_map.get("Pos")].get_text())
        pos = int(re.sub(r"[^\d]", "", pos_text)) if pos_text else None

        constructor_cell_text = tds[header_map.get("Car") or header_map.get("Team")].get_text(strip=True)
        constructor = normalize_unicode(constructor_cell_text)

        laps_cell_text = tds[header_map.get("Laps")].get_text(strip=True)
        laps = int(laps_cell_text) if laps_cell_text.isdigit() else None

        time_text = normalize_space(tds[header_map.get("Time/Retired") or header_map.get("Time") or header_map.get("Result")].get_text())
        
        points_text = tds[header_map.get("PTS")].get_text(strip=True)
        points = float(points_text) if points_text.replace('.', '', 1).isdigit() else None
        
        results_rows.append({
            "position": pos,
            "driver_name": normalize_unicode(driver_name),
            "driver_code": normalize_unicode(driver_code),
            "constructor": constructor,
            "laps": laps,
            "time_text": normalize_unicode(time_text),
            "points": points,
            "year": year,
            "round": round_
        })

    return meta, results_rows

def upsert_df_to_postgres(conn, df, table_name, pk_cols):
    """
    Upserts a pandas DataFrame into a PostgreSQL table.
    """
    if df.empty:
        print(f"DataFrame for {table_name} is empty, no data to upsert.")
        return

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    pk_cols_str = ','.join(pk_cols)
    update_cols_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in pk_cols])

    sql = f"""
        INSERT INTO {table_name} ({cols})
        VALUES %s
        ON CONFLICT ({pk_cols_str}) DO UPDATE
        SET {update_cols_str};
    """
    
    with conn.cursor() as cur:
        try:
            extras.execute_values(cur, sql, tuples, page_size=1000)
            conn.commit()
            print(f"Successfully upserted {len(df)} rows into {table_name}.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            conn.rollback()
            raise

def scrape_all_results():
    with conn() as connection:
        with connection.cursor() as cur:
            # Query the legacy_races table to get all race URLs.
            cur.execute("SELECT year, round, result_url FROM legacy_races ORDER BY year, round;")
            races = cur.fetchall()
            
            for race in races:
                year, round_, url = race
                print(f"Scraping results for {year} Round {round_} at {url}")
                
                meta, results_rows = parse_race_result_page(url, year, round_)
                
                if results_rows:
                    # Map the results to a DataFrame for easier handling
                    df = pd.DataFrame(results_rows)
                    
                    # Clean and prepare the DataFrame for upsert.
                    df.rename(columns={
                        'position': 'position',
                        'driver_name': 'driver_name',
                        'driver_code': 'driver_code',
                        'constructor': 'constructor',
                        'points': 'points',
                        'time_text': 'time_text',
                        'laps': 'laps'
                    }, inplace=True)
                    
                    df['status'] = df['time_text'].apply(lambda x: 'Finished' if pd.notna(x) and not re.search(r'DNF|DSQ|DNS', str(x), re.IGNORECASE) else 'Retired')

                    df_to_upsert = df[[
                        'year', 'round', 'position', 'driver_name', 'driver_code',
                        'constructor', 'points', 'status', 'time_text', 'laps'
                    ]]
                    
                    upsert_df_to_postgres(connection, df_to_upsert, 'legacy_results', ['year', 'round', 'position'])

if __name__ == "__main__":
    scrape_all_results()