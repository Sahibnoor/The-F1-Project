#!/usr/bin/env python

import time
import re
import requests
import requests_cache
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import psycopg2
from psycopg2.extras import execute_values
import unicodedata

# Assumed DB and Session configurations
DB_HOST = "database-1.cxm64uci8hxe.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "OnePieceReal.7143"
DB_SSLMODE = "require"
BASE = "https://www.formula1.com"

requests_cache.install_cache("f1_cache", expire_after=24*3600)
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "F1ApexAnalyst/1.0 (academic/personal project; contact: you@example.com)"
})
REQUEST_DELAY = 1.0

def get(url):
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

def parse_date_to_iso(text:str, default_year:int|None=None):
    from datetime import datetime
    text = (text or "").strip()
    candidates = [text]
    if default_year and not any(ch.isdigit() for ch in text.split()[-1:]):
        candidates.append(f"{text} {default_year}")
    for s in candidates:
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except Exception:
                pass
    return None

def upsert_legacy_races(cur, rows):
    sql = """
        INSERT INTO legacy_races (year, round, grand_prix, country, date, result_url)
        VALUES %s
        ON CONFLICT (year, round) DO UPDATE
        SET grand_prix = EXCLUDED.grand_prix,
            country    = EXCLUDED.country,
            date       = EXCLUDED.date,
            result_url = EXCLUDED.result_url;
    """
    execute_values(cur, sql, rows)

def get_circuit_and_country(url):
    try:
        resp = get(url)
        soup = BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None, None
    
    circuit_tag = soup.select_one("p.typography-module_body-xs-semibold__Fyfwn")
    circuit = None
    country = None
    if circuit_tag:
        circuit_text = circuit_tag.get_text(" ", strip=True)
        if "," in circuit_text:
            parts = circuit_text.split(",")
            circuit = normalize_unicode(parts[0].strip())
            if len(parts) > 1:
                country = normalize_unicode(parts[1].strip())
    
    return circuit, country

def parse_year_index(year: int):
    candidates = [
        f"{BASE}/en/results/{year}/races",
        f"{BASE}/en/results.html/{year}/races.html",
    ]
    html = None
    for u in candidates:
        try:
            html = get(u).text
            break
        except Exception:
            continue
    if html is None:
        raise RuntimeError(f"Could not fetch year index for {year}")

    soup = BeautifulSoup(html, "lxml")
    table = None
    for t in soup.select("table"):
        heads = [h.get_text(" ", strip=True).lower() for h in t.select("thead th")]
        if any("grand prix" in h for h in heads) and any("date" in h for h in heads):
            table = t
            break
    if table is None:
        raise RuntimeError("No results table with Grand Prix + Date headers found")

    headers = [h.get_text(" ", strip=True) for h in table.select("thead th")]
    lower = [h.lower() for h in headers]
    gp_idx = next(i for i, h in enumerate(lower) if "grand prix" in h)
    date_idx = next(i for i, h in enumerate(lower) if "date" in h)

    rows = []
    rnd = 0
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) <= max(gp_idx, date_idx):
            continue

        gp_cell = tds[gp_idx]
        
        # Get the grand prix name from the yearly index for the database
        gp_text_raw = gp_cell.get_text(" ", strip=True)
        gp_text = normalize_unicode(gp_text_raw)
        
        date_text = tds[date_idx].get_text(" ", strip=True)

        ahref = tr.select_one('a[href*="race-result"]') or tr.select_one("a[href]")
        if not ahref:
            continue
        href = ahref.get("href")
        if not href.endswith("/race-result"):
            href = href.rstrip("/") + "/race-result"
        result_url = urljoin(BASE, href)

        rnd += 1
        rows.append({
            "year": year,
            "round": rnd,
            "grand_prix": gp_text,
            "date": parse_date_to_iso(date_text, default_year=year),
            "result_url": result_url
        })
    return rows

def scrape_all_races(start_year: int, end_year: int):
    with conn() as connection:
        with connection.cursor() as cur:
            for y in range(start_year, end_year + 1):
                try:
                    race_data = parse_year_index(y)
                    if not race_data:
                        print(f"[{y}] No races found in index.")
                        continue
                    
                    # New loop to get circuit and country from results page
                    final_data = []
                    for r in race_data:
                        circuit, country = get_circuit_and_country(r["result_url"])
                        r['country'] = country
                        # Temporarily use grand_prix column for circuit name
                        # We'll fix the schema later as you requested
                        r['grand_prix'] = circuit
                        final_data.append(r)
                    
                    upsert_rows = [
                        (d['year'], d['round'], d['grand_prix'], d['country'], d['date'], d['result_url'])
                        for d in final_data
                    ]
                    
                    upsert_legacy_races(cur, upsert_rows)
                    print(f"[{y}] Successfully scraped and upserted {len(final_data)} races with circuit names.")
                except Exception as e:
                    print(f"[{y}] Error: {e}")
            connection.commit()

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3:
        s, e = int(sys.argv[1]), int(sys.argv[2])
    elif len(sys.argv) == 2:
        s = e = int(sys.argv[1])
    else:
        print("Usage: python scrape_races.py <YEAR> [<END_YEAR>]")
        sys.exit(1)
    scrape_all_races(s, e)