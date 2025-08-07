import requests, sqlite3, time
# Connect to SQLite database (creates file if not exists)
conn = sqlite3.connect('data/f1.sqlite')
cur = conn.cursor()
# Create tables from schema
with open('sql/schema.sql', 'r') as f:
    cur.executescript(f.read())
conn.commit()

# Fetch race schedule for 2021 to get race list
year = 2021
url = f"http://ergast.com/api/f1/{year}.json"
resp = requests.get(url)
data = resp.json()
races = data['MRData']['RaceTable']['Races']
for race in races:
    race_id = int(race['round'])  # use round number as raceId for uniqueness within a season
    race_name = race['raceName']
    circuit_name = race['Circuit']['circuitName']
    date = race['date']
    cur.execute("INSERT OR REPLACE INTO races VALUES (?, ?, ?, ?, ?, ?);", 
                (race_id, year, int(race['round']), race_name, circuit_name, date))
conn.commit()

# Fetch results for the first race of 2021
round_num = 1
results_url = f"http://ergast.com/api/f1/{year}/{round_num}/results.json"
resp = requests.get(results_url)
race_data = resp.json()
results_list = race_data['MRData']['RaceTable']['Races'][0]['Results']
for result in results_list:
    driver = result['Driver']
    constructor = result['Constructor']
    # Insert driver and constructor (if not already in table)
    cur.execute("INSERT OR IGNORE INTO drivers VALUES (?, ?, ?, ?, ?);", 
                (driver['driverId'], driver.get('code', None), driver['givenName'], driver['familyName'], driver['nationality']))
    cur.execute("INSERT OR IGNORE INTO constructors VALUES (?, ?, ?);", 
                (constructor['constructorId'], constructor['name'], constructor['nationality']))
    # Insert race result
    cur.execute("INSERT INTO results (raceId, driverId, constructorId, grid, position, points, status) VALUES (?, ?, ?, ?, ?, ?, ?);",
                (round_num, driver['driverId'], constructor['constructorId'], int(result['grid']),
                 int(result['position']), float(result['points']), result['status']))
conn.commit()
print(f"Inserted {len(results_list)} results for {race_name} {year}.")
conn.close()
