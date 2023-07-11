# Module Imports

import mariadb
import sys
import datetime
from datetime import timedelta
import pandas as pd

###Functions for better DB handling with Python

# This functions establishes a connection to the database
def connect_to_DB(user = "airq", password = "airq", host = "localhost", port = 3306, database = "airq_data"):
        #Connect with DB
    # Connect to MariaDB Platform
    try:
        connection = mariadb.connect(
            user="airq",
            password="airq",
            host="localhost",
            port=3306,
            database="airq_data"

        )
        return connection
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    
# This function creates a new table
def create_table(name):

    # Connect to DB
    conn = connect_to_DB()
    # Get Cursor
    cur = conn.cursor()

    #SQL-Statement zum kreieren einer Table
    sql = """CREATE TABLE IF NOT EXISTS  `""" + name +"""`
                (`TypPS` FLOAT DEFAULT NULL COMMENT ' Die durchschnittliche Partikelgröße in µm.'
                ,`oxygen` FLOAT DEFAULT NULL COMMENT 'Sauerstoff-Konzentration in Volumen-Prozent.'
                ,`pm10` FLOAT DEFAULT NULL COMMENT ' Feinstaubkonzentration für die Partikel 10 µm in µg/m3.'
                ,`cnt0_5` FLOAT DEFAULT NULL COMMENT 'Die Gesamtzahl der Feinstaub-Partikel größer als 0,5 µm.'
                ,`co` FLOAT DEFAULT NULL COMMENT 'CO-Konzentration in ppm.'
                ,`temperature` FLOAT DEFAULT NULL COMMENT 'Temperatur in °C.'
                ,`performance` FLOAT DEFAULT NULL COMMENT 'Berechneter Leistungsindex.'
                ,`co2` FLOAT DEFAULT NULL COMMENT 'CO2-Konzentration in ppm.'
                ,`measuretime` INT DEFAULT NULL COMMENT 'Zeit in ms, die für den gesamten letzten Messdurchlauf benötigt wurde.' 
                ,`so2` FLOAT DEFAULT NULL COMMENT 'SO2-Konzentration in µg/m3.'
                ,`no2` FLOAT DEFAULT NULL COMMENT 'NO2-Konzentration in µg/m3.'
                ,`cnt5` FLOAT DEFAULT NULL COMMENT 'Die Gesamtzahl der Feinstaub-Partikel größer als 5 µm.'
                ,`timestamp` TIMESTAMP NOT NULL COMMENT 'Zeitstempel zu den Messwerten als Unix-Epoche in Millisekunden.'
                ,`pm1` FLOAT DEFAULT NULL COMMENT 'Feinstaubkonzentration für die Partikel 1.0 µm in µg/m3.'
                ,`cnt1` FLOAT DEFAULT NULL COMMENT 'Die Gesamtzahl der Feinstaub-Partikel größer als 1 µm.'
                ,`dewpt` FLOAT DEFAULT NULL COMMENT 'Taupunkt in °C.'
                ,`tvoc` FLOAT DEFAULT NULL COMMENT 'VOC-Konzentration in ppb.'
                ,`pressure` FLOAT DEFAULT NULL COMMENT 'Luftdruck in hPa.'
                ,`cnt10` FLOAT DEFAULT NULL COMMENT 'Die Gesamtzahl der Feinstaub-Partikel größer als 10 µm.'
                ,`dCO2dt` FLOAT DEFAULT NULL COMMENT 'CO2-Änderungsrate in ppm/s.'
                ,`sound_max` FLOAT DEFAULT NULL COMMENT 'Maimaler Lärm in dB(A).'
                ,`health` FLOAT DEFAULT NULL COMMENT 'Berechneter Gesundheitsindex. Bereich 0 bis 1000: normale Bewertung. -200 bei Gasalarm. -800 bei Feueralarm.'
                ,`temperature_o2` FLOAT DEFAULT NULL COMMENT 'Temperatur in °C.'
                ,`cnt2_5` FLOAT DEFAULT NULL COMMENT 'Die Gesamtzahl der Feinstaub-Partikel größer als 2,5 µm.'
                ,`o3` FLOAT DEFAULT NULL COMMENT 'O3-Konzentration in µg/m3.'
                ,`humidity` FLOAT DEFAULT NULL COMMENT 'Relative Luftfeuchtigkeit in %.'
                ,`dHdt` FLOAT DEFAULT NULL COMMENT 'Änderungsrate der absoluten Luftfeuchtigkeit in g/m3/s.'
                ,`humidity_abs` FLOAT DEFAULT NULL COMMENT 'Absolute Luftfeuchtigkeit in g/m3.'
                ,`sound` FLOAT DEFAULT NULL COMMENT 'Lärm in dB(A).'
                ,`pm2_5` FLOAT DEFAULT NULL COMMENT 'Feinstaubkonzentration für die Partikel 2.5 µm in µg/m3.'
                ,`cnt0_3` FLOAT DEFAULT NULL COMMENT 'Die Gesamtzahl der Feinstaub-Partikel größer als 0,3 µm.',
                PRIMARY KEY (`timestamp`)
                )"""

    #führt SQL-Statement aus
    cur.execute(sql)

    #DB-Connection close
    conn.close()
    print("Table '" + name + "' created")

# This function deletes a existing Table in the Database
def drop_table(name):
    conn = connect_to_DB()

    # Get Cursor
    cur = conn.cursor()

    #SQL-Statement zum kreieren einer Table
    sql = """DROP TABLE `""" + name +"""`;"""

    #führt SQL-Statement aus
    cur.execute(sql)

    #DB-Connection close
    conn.close()
    print("Table '" + name + "' dropped")

def delete_all_records(name):
     # Connect to DB
    conn = connect_to_DB()
    # Get Cursor
    cur = conn.cursor()

    #SQL-Statement zum kreieren einer Table
    sql = """TRUNCATE TABLE`""" + name +"""`;"""

    #führt SQL-Statement aus
    cur.execute(sql)

    #DB-Connection close
    conn.close()
    print("All entries of table '"+ name + "'have been deleted")

def pad_date(date_string):
    year, month, day = map(int, date_string.split('/'))
    return datetime.date(datetime(year, month, day))

def delete_records_in_timespan(startdate, enddate, tablename):
    # Connect to DB
    conn = connect_to_DB(tablename)
    # Get Cursor
    cur = conn.cursor()

    startdate = pad_date(startdate)
    enddate = pad_date(enddate)
    enddate += timedelta(days=1)  # adding one day to end date to make sure it's inclusive

    #SQL Statement to delete records in a date range
    sql = f"""DELETE FROM `{tablename}` WHERE timestamp BETWEEN '{startdate}' AND '{enddate}';"""

    # Executes SQL Statement
    cur.execute(sql)

    # Commits changes to DB
    conn.commit()

    # Closes DB Connection
    conn.close()

    print(f"Entries from {startdate} to {enddate} in table '{tablename}' have been deleted.")

def get_latest_timestamp(name):
    # Connect to DB
    conn = connect_to_DB(name)
    # Get Cursor
    cur = conn.cursor()

    sql = """SELECT DATE(MAX(timestamp)) FROM """ + name
    # Runs SQL-Statement
    cur.execute(sql)
    # Fetches the result
    timestamp = cur.fetchone()

    # DB-Connection close
    conn.close()
    # Print latest timestamp in table
    #print("Latest timestamp in table '" + name + "' is " + str(timestamp[0]))
    return timestamp

def db_to_csv(tablename, filename):
    # Connect to DB
    conn = connect_to_DB(tablename)

    # Use pandas to execute SQL query and get a DataFrame
    df = pd.read_sql_query("SELECT * FROM " + tablename, conn)

    # Write DataFrame to CSV file
    df.to_csv(filename, index=False)

    # Close the DB connection
    conn.close()
