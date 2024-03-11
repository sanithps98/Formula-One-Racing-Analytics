-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw
LOCATION 'abfss://raw@formulaoneracing1dl.dfs.core.windows.net/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create tables for CSV Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####1) Create Circuits Table

-- COMMAND ----------

-- Create the table
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (
  path 'abfss://raw@formulaoneracing1dl.dfs.core.windows.net/circuits.csv',
  header 'true'
);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####2) Create Races Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (
  path "abfss://raw@formulaoneracing1dl.dfs.core.windows.net/races.csv", 
  header 'true'
);

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####3) Create Constructors Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING csv
OPTIONS (
  path "abfss://raw@formulaoneracing1dl.dfs.core.windows.net/constructors.csv", 
  header 'true'
);

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####4) Create Drivers Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
forename STRING, 
surname STRING,
dob DATE,
nationality STRING,
url STRING)
USING csv
OPTIONS (
  path "abfss://raw@formulaoneracing1dl.dfs.core.windows.net/drivers.csv", 
  header 'true'
);

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####5) Create Results Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING csv
OPTIONS (
  path "abfss://raw@formulaoneracing1dl.dfs.core.windows.net/results.csv", 
  header 'true'
);

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####6) Create Pit Stops Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING csv
OPTIONS (
  path "abfss://raw@formulaoneracing1dl.dfs.core.windows.net/pit_stops.csv", 
  header 'true',
  multiLine 'true'
);

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####7) Create Lap Times Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (
  path "abfss://raw@formulaoneracing1dl.dfs.core.windows.net/lap_times", 
  header 'true'
);

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####8) Create Qualifying Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING csv
OPTIONS (
  path "abfss://raw@formulaoneracing1dl.dfs.core.windows.net/qualifying", 
  header 'true',
  multiLine 'true'
);

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;