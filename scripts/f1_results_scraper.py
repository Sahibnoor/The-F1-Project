# f1_results_scraper.py
import time
import random
import re
import unicodedata
import requests
import requests_cache
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from urllib.parse import urljoin


# --- WARNING: Storing credentials in code is insecure. ---
# It is highly recommended to use environment variables or a secrets management system.
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
# REQUEST_DELAY = 1.0  # seconds between requests


def get(url):
    """GET with cache + delay when not cached."""
    resp = SESSION.get(url, timeout=30)
    REQUEST_DELAY = random.randint(5,15)
    if not getattr(resp, "from_cache", False):
        time.sleep(REQUEST_DELAY)
    resp.raise_for_status()
    return resp

# --- DB helpers ---
def conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        port=5432, sslmode=DB_SSLMODE
    )

def upsert_legacy_race(cur, year:int, round_:int, grand_prix:str, country:str, date_iso:str):
    sql = """
        INSERT INTO legacy_races (year, round, grand_prix, country, date)
        VALUES %s
        ON CONFLICT (year, round) DO UPDATE
        SET grand_prix = EXCLUDED.grand_prix,
            country   = EXCLUDED.country,
            date      = EXCLUDED.date;
    """
    execute_values(cur, sql, [(year, round_, grand_prix, country, date_iso)])

def upsert_legacy_results(cur, rows):
    sql = """
        INSERT INTO legacy_results
        (year, round, position, driver_name, driver_code, constructor, points, status, time_text, laps)
        VALUES %s
        ON CONFLICT (year, round, position) DO UPDATE
        SET driver_name = EXCLUDED.driver_name,
            driver_code = EXCLUDED.driver_code,
            constructor = EXCLUDED.constructor,
            points      = EXCLUDED.points,
            status      = EXCLUDED.status,
            time_text   = EXCLUDED.time_text,
            laps        = EXCLUDED.laps;
    """
    execute_values(cur, sql, rows)

# --- parsing utils ---
def normalize_space(s):
    return re.sub(r"\s+", " ", s or "").strip()

def asciify_text(text: str) -> str:
    """Converts text with special characters to its closest ASCII equivalent."""
    if not text:
        return text
    # Decompose characters (e.g., é -> e + ´) then encode to ASCII, ignoring the accent marks.
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')

def extract_driver_code(name):
    m = re.search(r"\(([A-Z]{2,3})\)$", name or "")
    return m.group(1) if m else None

def parse_results_table(table):
    headers = [normalize_space(th.get_text()) for th in table.select("thead th")]
    body = []
    for tr in table.select("tbody tr"):
        cells = [normalize_space(td.get_text()) for td in tr.select("td")]
        if len(cells) < 5:
            continue
        row = dict(zip(headers, cells))
        body.append(row)
    return body

def parse_year_index(year: int):
    candidates = [
        f"{BASE}/en/results/{year}/races",
        f"{BASE}/en/results.html/{year}/races.html",
    ]
    html = None
    base_url = None
    for u in candidates:
        try:
            html = get(u).text
            base_url = u
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
    date_idx = next(i for i, h in enumerate(lower) if "date" in h)

    rows = []
    rnd = 0
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) <= date_idx:
            continue

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
            "round": rnd,
            "date": parse_date_to_iso(date_text, default_year=year),
            "result_url": result_url
        })
    return rows

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

def parse_race_result_page(url):
    """
    Return tuple: (meta, results_rows)
    meta: {grand_prix, country, date, circuit}
    results_rows: list of dicts with keys we’ll map later
    """
    resp = get(url)
    soup = BeautifulSoup(resp.text, "lxml")

    # Meta extraction (best-effort; site changes layout occasionally)
    h1 = soup.select_one("h1") or soup.select_one("header h1")
    grand_prix = normalize_space(h1.get_text()) if h1 else None

    # Try to find date/country in header blocks
    header_txt = normalize_space((soup.select_one("header") or soup).get_text())
    mdate = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", header_txt)
    date_iso = None
    if mdate:
        # Lightweight date parsing
        from datetime import datetime
        try:
            date_iso = datetime.strptime(mdate.group(1), "%d %B %Y").date().isoformat()
        except Exception:
            pass

    # Country often appears in breadcrumbs or metadata
    country = None
    for crumb in soup.select("nav a, .breadcrumb a, .f1-breadcrumb a"):
        txt = normalize_space(crumb.get_text())
        if txt and txt.lower() not in ("home", "results"):
            country = txt
            break
            
    # New code to get circuit name and country from the specified class
    circuit_tag = soup.select_one("p.typography-module_body-xs-semibold__Fyfwn")
    circuit = None
    if circuit_tag:
        circuit_text = circuit_tag.get_text(" ", strip=True)
        if "," in circuit_text:
            parts = circuit_text.split(",")
            circuit = parts[0].strip()
            # If a new country is found, override the previous one from breadcrumbs
            if len(parts) > 1:
                country = parts[1].strip()

    # Results table: F1.com uses a standard table; fall back to any table with 'Pos' header
    tables = soup.select("table")
    table = None
    for t in tables:
        heads = [normalize_space(th.get_text()) for th in t.select("thead th")]
        if any(h.lower().startswith("pos") for h in heads):
            table = t; break
    if table is None:
        raise RuntimeError(f"No results table found at {url}")

    rows = parse_results_table(table)

    # Apply normalization to the extracted names
    grand_prix = asciify_text(grand_prix)
    country = asciify_text(country)
    circuit = asciify_text(circuit)
    return {"grand_prix": grand_prix, "country": country, "date": date_iso, "circuit": circuit}, rows

def map_result_row(row):
    pos_text = row.get("Pos") or row.get("Position")
    try:
        position = int(re.sub(r"[^\d]", "", pos_text or ""))
    except Exception:
        position = None

    driver = row.get("Driver") or row.get("Driver Name")
    constructor = row.get("Car") or row.get("Team") or row.get("Constructor")
    points = row.get("PTS") or row.get("Points")
    time_text = row.get("Time/Retired") or row.get("Time") or row.get("Result")
    laps = row.get("Laps")

    try:
        points = float(points)
    except Exception:
        points = None
    try:
        laps = int(laps)
    except Exception:
        laps = None

    driver_name = normalize_space(driver)
    driver_code = extract_driver_code(driver_name)

    status = None
    if time_text:
        tt = time_text.upper()
        if any(s in tt for s in ("DNF", "DSQ", "DNS")):
            status = tt
        else:
            status = "Finished"

    return {
        "position": position, "driver_name": driver_name, "driver_code": driver_code,
        "constructor": normalize_space(constructor), "points": points,
        "time_text": normalize_space(time_text), "laps": laps, "status": status
    }

def scrape_year(year:int):
    print(f"[{year}] fetching index…")
    races = parse_year_index(year)
    print(f"[{year}] found {len(races)} race pages")

    with conn() as connection:
        with connection.cursor() as cur:
            for r in races:
                rnd = r["round"]
                url = r["result_url"]
                print(f"  → Round {rnd}: {url}")

                try:
                    meta, results_rows = parse_race_result_page(url)
                except Exception as e:
                    print(f"    ! skip (page parse failed): {e}")
                    continue
                
                circuit_name = meta.get("circuit")
                correct_country = meta.get("country")
                
                if circuit_name:
                    grand_prix_name = circuit_name
                elif correct_country:
                    grand_prix_name = f"{correct_country} Grand Prix"
                else:
                    grand_prix_name = f"Round {rnd}"

                # --- Final Override for Abu Dhabi ---
                if correct_country and "United Arab Emirates" in correct_country:
                    correct_country = "Abu Dhabi"
                    grand_prix_name = "Yas Marina Circuit"
                # --- End Override ---

                upsert_legacy_race(cur, year, rnd, grand_prix_name, correct_country, r["date"])
                
                mapped = [map_result_row(x) for x in results_rows if x]
                payload = [
                    (year, rnd,
                     m["position"], m["driver_name"], m["driver_code"],
                     m["constructor"], m["points"], m["status"], m["time_text"], m["laps"])
                    for m in mapped if m.get("position") is not None
                ]
                if payload:
                    upsert_legacy_results(cur, payload)

        connection.commit()

def scrape_range(start:int, end:int):
    for y in range(start, end+1):
        scrape_year(y)

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 3:
        s, e = int(sys.argv[1]), int(sys.argv[2])
    elif len(sys.argv) == 2:
        s = e = int(sys.argv[1])
    else:
        print("Usage: python f1_results_scraper.py <YEAR> [<END_YEAR>]")
        sys.exit(1)
    scrape_range(s, e)