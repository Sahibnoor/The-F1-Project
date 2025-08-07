CREATE TABLE IF NOT EXISTS races (
    raceId INTEGER PRIMARY KEY,
    year INTEGER,
    round INTEGER,
    name TEXT,
    circuit TEXT,
    date TEXT
);
CREATE TABLE IF NOT EXISTS  drivers (
    driverId TEXT PRIMARY KEY,  -- using Ergast driverId (string) as primary key
    code TEXT,
    firstName TEXT,
    lastName TEXT,
    nationality TEXT
);
CREATE TABLE IF NOT EXISTS constructors (
    constructorId TEXT PRIMARY KEY,
    name TEXT,
    nationality TEXT
);
CREATE TABLE IF NOT EXISTS  results (
    resultId INTEGER PRIMARY KEY AUTOINCREMENT,
    raceId INTEGER,
    driverId TEXT,
    constructorId TEXT,
    grid INTEGER,
    position INTEGER,
    points REAL,
    status TEXT,
    FOREIGN KEY(raceId) REFERENCES races(raceId),
    FOREIGN KEY(driverId) REFERENCES drivers(driverId),
    FOREIGN KEY(constructorId) REFERENCES constructors(constructorId)
);
CREATE TABLE IF NOT EXISTS  pitstops (
    raceId INTEGER,
    driverId TEXT,
    stop INTEGER,
    lap INTEGER,
    duration REAL,
    PRIMARY KEY(raceId, driverId, stop),
    FOREIGN KEY(raceId) REFERENCES races(raceId),
    FOREIGN KEY(driverId) REFERENCES drivers(driverId)
);
CREATE TABLE IF NOT EXISTS  laptimes (
    raceId INTEGER,
    driverId TEXT,
    lap INTEGER,
    milliseconds INTEGER,
    PRIMARY KEY(raceId, driverId, lap),
    FOREIGN KEY(raceId) REFERENCES races(raceId),
    FOREIGN KEY(driverId) REFERENCES drivers(driverId)
);
