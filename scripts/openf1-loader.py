# openf1_loader.py (normalized tables)
import sys, requests, psycopg2
from psycopg2.extras import execute_values

DB_HOST = "database-1.cxm64uci8hxe.us-east-2.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "OnePieceReal.7143"
DB_SSLMODE = "require"

API = "https://api.openf1.org/v1"

def conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        port=5432, sslmode=DB_SSLMODE
    )

def get(url, params=None):
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

# ---------- Get Session data ----------
def upsert_race(cur, session_key:int):
    sess = get(f"{API}/sessions", {"session_key": session_key})
    if not sess:
        return 0
    s = sess[0]
    row = (
        s.get("session_key"),
        s.get("meeting_key"),
        s.get("year"),
        s.get("country_name"),
        s.get("event_name"),
        s.get("circuit_short_name") or s.get("circuit_name"),
        s.get("date_start"),
        s.get("date_end"),
    )
    sql = """
        INSERT INTO races(session_key, meeting_key, year, country_name, event_name, circuit_short, date_start, date_end)
        VALUES %s
        ON CONFLICT (session_key) DO UPDATE
        SET meeting_key  = EXCLUDED.meeting_key,
            year         = EXCLUDED.year,
            country_name = EXCLUDED.country_name,
            event_name   = EXCLUDED.event_name,
            circuit_short= EXCLUDED.circuit_short,
            date_start   = EXCLUDED.date_start,
            date_end     = EXCLUDED.date_end;
    """
    execute_values(cur, sql, [row])
    return 1

# ---------- Get Driver data ----------
def upsert_drivers_for_session(cur, session_key:int):
    # Prefer per-session scope so we only insert active drivers we actually see
    drivers = get(f"{API}/drivers", {"session_key": session_key})
    if not drivers:
        return 0
    rows = []
    for d in drivers:
        rows.append((
            d.get("driver_number"),
            d.get("first_name"),
            d.get("last_name"),
            d.get("name_acronym"),
            d.get("team_name")
        ))
    sql = """
        INSERT INTO drivers(driver_number, first_name, last_name, driver_code, team)
        VALUES %s
        ON CONFLICT (driver_number) DO UPDATE
        SET driver_code = EXCLUDED.driver_code,
            team = EXCLUDED.team,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name;
    """
    execute_values(cur, sql, rows)
    return len(rows)

def upsert_results(cur, session_key:int):
    res = get(f"{API}/session_result", {"session_key": session_key})
    if not res:
        return 0
    rows = []
    for r in res:
        rows.append((
            r.get("session_key"),
            r.get("driver_number"),
            r.get("position"),
            r.get("points"),
            r.get("status"),
            r.get("team_name"),
        ))
    sql = """
        INSERT INTO results(session_key, driver_number, position, points, status, team_name)
        VALUES %s
        ON CONFLICT (session_key, driver_number) DO UPDATE
        SET position = EXCLUDED.position,
            points   = EXCLUDED.points,
            status   = EXCLUDED.status,
            team_name= EXCLUDED.team_name;
    """
    execute_values(cur, sql, rows)
    return len(rows)

def upsert_laps(cur, session_key:int):
    laps = get(f"{API}/laps", {"session_key": session_key})
    if not laps:
        return 0
    rows = []
    for r in laps:
        rows.append((
            r.get("session_key"),
            r.get("driver_number"),
            r.get("lap_number"),
            r.get("lap_duration"),
            r.get("s1_duration"),
            r.get("s2_duration"),
            r.get("s3_duration"),
            r.get("position"),
        ))
    sql = """
        INSERT INTO lap_times(session_key, driver_number, lap_number, lap_time_ms, sector1_ms, sector2_ms, sector3_ms, position)
        VALUES %s
        ON CONFLICT (session_key, driver_number, lap_number) DO UPDATE
        SET lap_time_ms = EXCLUDED.lap_time_ms,
            sector1_ms  = EXCLUDED.sector1_ms,
            sector2_ms  = EXCLUDED.sector2_ms,
            sector3_ms  = EXCLUDED.sector3_ms,
            position    = EXCLUDED.position;
    """
    execute_values(cur, sql, rows)
    return len(rows)

def main():
    if len(sys.argv) != 2:
        print("Usage: python openf1_loader.py <SESSION_KEY>")
        sys.exit(1)
    sk = int(sys.argv[1])
    with conn() as connection:
        with connection.cursor() as cur:
            upsert_race(cur, sk)
            upsert_drivers_for_session(cur, sk)
            n_res = upsert_results(cur, sk)
            n_lap = upsert_laps(cur, sk)
        connection.commit()
    print(f"Loaded session_key={sk}: results={n_res}, laps={n_lap}")

if __name__ == "__main__":
    main()
