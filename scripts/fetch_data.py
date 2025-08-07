import requests, sqlite3, time, os

db_path = 'data/f1.sqlite'
first_time = not os.path.exists(db_path)
conn = sqlite3.connect(db_path)
cur = conn.cursor()

if first_time:
    with open('sql/schema.sql', 'r') as f:
        cur.executescript(f.read())
    conn.commit()

# # Connect to SQLite database (creates file if not exists)
# conn = sqlite3.connect('data/f1.sqlite')
# cur = conn.cursor()
# # Create tables from schema
# with open('sql/schema.sql', 'r') as f:
#     cur.executescript(f.read())
# conn.commit()

years = list(range(2015, 2022))  # 2015-2021 inclusive
for year in years:
    # Fetch and insert all races for the season
    url = f"https://ergast.com/api/f1/{year}.json"
    data = requests.get(url).json()
    races = data['MRData']['RaceTable']['Races']
    for race in races:
        race_id = int(race['round']) + (year * 100)  # unique key combining year and round (e.g., 201501 for 2015 round1)
        cur.execute("INSERT OR REPLACE INTO races VALUES (?, ?, ?, ?, ?, ?);",
                    (race_id, year, int(race['round']), race['raceName'], race['Circuit']['circuitName'], race['date']))
    conn.commit()
    # Fetch results for each race in the season
    for race in races:
        round_num = int(race['round'])
        results_url = f"http://ergast.com/api/f1/{year}/{round_num}/results.json?limit=1000"
        time.sleep(0.2)  # small delay to respect rate limits
        results_data = requests.get(results_url).json()
        if results_data['MRData']['RaceTable']['Races']:
            results_list = results_data['MRData']['RaceTable']['Races'][0]['Results']
            race_id = int(race['round']) + (year * 100)
            for result in results_list:
                driver = result['Driver']; constructor = result['Constructor']
                cur.execute("INSERT OR IGNORE INTO drivers VALUES (?, ?, ?, ?, ?);", 
                            (driver['driverId'], driver.get('code'), driver['givenName'], driver['familyName'], driver['nationality']))
                cur.execute("INSERT OR IGNORE INTO constructors VALUES (?, ?, ?);", 
                            (constructor['constructorId'], constructor['name'], constructor['nationality']))
                cur.execute("""INSERT INTO results 
                               (raceId, driverId, constructorId, grid, position, points, status) 
                               VALUES (?, ?, ?, ?, ?, ?, ?);""",
                            (race_id, driver['driverId'], constructor['constructorId'], int(result['grid'] or 0),
                             int(result['position'] or 0), float(result['points'] or 0.0), result['status']))
    conn.commit()
    print(f"Season {year} data inserted: {len(races)} races, results for all races.")

conn.close()
