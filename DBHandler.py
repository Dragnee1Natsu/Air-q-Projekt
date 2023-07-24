# Module Imports

#DB stuff
import mariadb
import sys

#Encryption Stuff
import base64
from Crypto.Cipher import AES
import http.client
from Crypto import Random

#Other
import json
import time
import datetime
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine


class DBHandler():
    """
    This class contains all functions needed to operate the database and to request data from the AirQ-Sensor. 

        Args:
            airqIP (str, optional): IP-Address of the AirQ-Sensor. Defaults to '192.168.4.1'.
            airqpass (str, optional): Password of the AirQ-Sensor. Defaults to 'airqsetup'.

    """
    def __init__(self, airqIP = '192.168.4.1', airqpass = 'airqsetup'):
        """Constructor-method

        Args:
            airqIP (str, optional): IP-Address of the AirQ-Sensor. Defaults to '192.168.4.1'.
            airqpass (str, optional): Password of the AirQ-Sensor. Defaults to 'airqsetup'.
        """
        self.__airqIP = airqIP
        self.__airqpass = airqpass

    #Functions for de-and encoding the messages
    def __unpad(self, data):
        return data[:-ord(data[-1])]
    
    def __pad(self, data):
        length = 16 - (len(data) % 16)
        return data + chr(length).encode('utf-8')*length
    
    def __decodeMessage(self, msgb64):
        # First step: decode base64
        msg = base64.b64decode(msgb64)

        # Create AES key of length 32 from air-Q password
        key = self.__airqpass.encode('utf-8')
        if len(key) < 32:
            for i in range(32-len(key)):
                key += b'0'
        elif len(key) > 32:
            key = key[:32]

        # Second step: decoding AES256
        cipher = AES.new(key=key, mode=AES.MODE_CBC, IV=msg[:16])
        return self.__unpad(cipher.decrypt(msg[16:]).decode('utf-8'))
    
    def __encodeMessage(self, msg):
        # Create AES key of length 32 from air-Q password
        key = self.__airqpass.encode('utf-8')
        if len(key) < 32:
            for i in range(32-len(key)):
                key += b'0'
        elif len(key) > 32:
            key = key[:32]

        # First step: Encrypt AES256
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(key=key, mode=AES.MODE_CBC, IV=iv)
        msg = msg.encode('utf-8')
        crypt = iv + cipher.encrypt(self.__pad(msg))

        # Second step: encode base64
        msgb64 = base64.b64encode(crypt).decode('utf-8')
        return msgb64
    
    def __connect_to_DB(self, user = "airq", password = "airq", host = "localhost", port = 3306, database = "airq_data"):
        """Establishes a connection to the database

        Args:
            user (str, optional): DB-Username. Defaults to "airq".
            password (str, optional): DB-Password. Defaults to "airq".
            host (str, optional): DB-Host. Defaults to "localhost".
            port (int, optional): DB-Port. Defaults to 3306.
            database (str, optional): DB-Name. Defaults to "airq_data".

        Returns:
            Connection: Database connection
        """
        try:
            connection = mariadb.connect(
                user = user,
                password = password,
                host = host,
                port = port,
                database = database

            )
            return connection
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)
            
    def get_latest_timestamp(self, name):
        """Gets the latest timestamp in the database

        Args:
            name (str): name of the database table

        Returns:
            date: latest timestamp form database
        """
        


        conn = self.__connect_to_DB()
        # Get Cursor
        cur = conn.cursor()

        sql = """SELECT DATE(MAX(timestamp)) FROM """ + name
        # Runs SQL-Statement
        cur.execute(sql)
        # Fetches the result
        timestamp = cur.fetchone()

        # DB-Connection close
        conn.close()

        return timestamp[0]
   
    def __sql_data(self, contents, conn ,cur, table):
        """This function creates the sql-statement for inserting the requested data of the AirQ sensor

        Args:
            contents ( HTTPResponse-Objekt): _description_
            conn (Connection): DB-Connection
            cur (Cursor): DB-Cursor
            table (str): DB-Table
        """
        #content is processed line by line
        for line in contents.read().split(b'\n'):
            if line != b'':
                #Message is decoded
                line = self.__decodeMessage(line)

                #Line is converted to a dict
                line= json.loads(line)

                columns = ""
                values = ""

                if line["Status"] != "OK":
                        print("Skipped invalid measurements due to warm-up of the Sensor")
                        continue
                
                for type in line:
                    #These variables are filtered out and not included in the database
                    if type == "bat" or type == "DeviceID" or type == "uptime" or type == "window_event" or type == "door_event" or type == "person" or type == "window_open" or type == "Status":
                        continue

                    
                    #Creates the string for the columns
                    columns += type 

                    #creates the string for the values
                    #Edge-case: if value is a list
                    if isinstance(line[type], list):
                        values += str(line[type][0])
                    elif type == "timestamp":
                        values += "FROM_UNIXTIME(%s)" % (int(line[type]/1000))
                    else:  
                        values += str(line[type])

                    #Edge-case: No comma at the end
                    if type != "cnt0_3":  
                        columns += ", "
                        values += ", "

                sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s );" % (table, columns, values)
                try:
                    cur.execute(sql)
                    conn.commit()
                except Exception as e:
                    print(f"An error occurred: {e}")

    def __pad_date(self, date_string):
        """Converts a string to a datetime object.

        This function takes a date string in the format 'YYYY/MM/DD' and 
        converts it into a datetime object for further manipulation.

        Args:
            date_string (str): A string of the date in 'YYYY/MM/DD' format.

        Returns:
            datetime: A datetime object representing the same date as the input string.
        """
        year, month, day = map(int, date_string.split('/'))
        return datetime(year, month, day)

    def airq_data_to_db(self, startdate, enddate, tablename):
        """Connects to the AirQ-Sensor, requests the data from the defined timeframe and inserts the data into the database. Your PC has to be connected to the AirQ-Sensor to request the data.


        Args:
            startdate (str): Startdate of the requestet timeframe in 'YYYY/MM/DD' format. 
            enddate (str): Enddate of the requestet timeframe in 'YYYY/MM/DD' format.
            tablename (str): DB-Table where data should be stored
        """

        # Get latest entry in table
        latest = self.get_latest_timestamp(tablename)

        # Connect to DB
        conn = self.__connect_to_DB()
        # Get Cursor
        cur = conn.cursor()

        startdate = self.__pad_date(startdate)
        enddate = self.__pad_date(enddate)

        # check if latest entry is further than startdate
        if latest is not None:
            print("_____________________________________________________________________________")
            print("Latest enrty in table '" + tablename + "' is " + datetime.strftime(latest, "%Y-%m-%d"))
            # change startdate for performance
            if latest > startdate:
                startdate = latest
        
        # Establish connection to the air-Q
        connection = http.client.HTTPConnection(self.__airqIP)

        end = enddate
        end += timedelta(days=1)

        while startdate < end:

            year = str(startdate.year)
            month = str(startdate.month)
            day = str(startdate.day)

            print("_____________________________________________________________________________")
            print("Year: " + year + "\n" + "Month: " + month + "\n" + "Day: " + day + "\n")

            connection.request("GET","/dir?request="+self.__encodeMessage(year+"/"+month+"/"+day))
            files = connection.getresponse()
            files = json.loads(self.__decodeMessage(files.read()))
            for file in files:
                print("File: " + file)   
                connection.request("GET","/file?request="+self.__encodeMessage(year+"/"+month+"/"+day+"/"+file))
                contents = connection.getresponse()
                if contents.status == 200:
                    print("Status: " + str(contents.status) + "/OK")
                else:
                    print("Status: " + str(contents.status) + "/Canceled!")
                    return
                
                self.__sql_data(contents, conn, cur, tablename)
            
            startdate += timedelta(days=1)

        conn.commit()
        connection.close()

    def create_table(self, name):
        """Creates a new database table with the schema for the metrics of the airq sensor.

        Args:
            name (str): New table name 
        """
        conn = self.__connect_to_DB()
        # Get Cursor
        cur = conn.cursor()

        #SQL-Statement for table creation
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

        #Runns SQL-Statement
        cur.execute(sql)

        #DB-Connection close
        conn.close()
        print("Table '" + name + "' created")

    def drop_table(self, name):
        """Drops table

        Args:
            name (str): Table name 
        """
        conn = self.__connect_to_DB()

        # Get Cursor
        cur = conn.cursor()

        #SQL-Statement zum kreieren einer Table
        sql = """DROP TABLE `""" + name +"""`;"""

        #führt SQL-Statement aus
        cur.execute(sql)

        #DB-Connection close
        conn.close()
        print("Table '" + name + "' dropped")

    def delete_records(self, tablename, startdate=None, enddate=None):
        """Deletes records from a table in a database.

        This function deletes all records from the specified table. 
        If a date range is provided, it only deletes the records within that range.

        Args:
            tablename (str): The name of the table from which to delete records.
            startdate (str, optional): The start date of the range within which to delete records. 
                Date should be in 'YYYY/MM/DD' format. If not provided, all records will be deleted.
            enddate (str, optional): The end date of the range within which to delete records. 
                Date should be in 'YYYY/MM/DD' format. If not provided, all records will be deleted.

        Returns:
            None
        """
        
        # Connect to DB
        conn = self.__connect_to_DB()
        # Get Cursor
        cur = conn.cursor()

        if startdate is None and enddate is None:
            # SQL-Statement zum löschen einer Table
            sql = """TRUNCATE TABLE`""" + tablename + """`;"""
            print("All entries of table '"+ tablename + "' have been deleted")
        else:
            startdate = self.__pad_date(startdate)
            enddate = self.__pad_date(enddate)
            enddate += timedelta(days=1)  # adding one day to end date to make sure it's inclusive

            # SQL Statement to delete records in a date range
            sql = f"""DELETE FROM `{tablename}` WHERE timestamp BETWEEN '{startdate}' AND '{enddate}';"""
            print(f"Entries from {startdate} to {enddate} in table '{tablename}' have been deleted.")

        # Executes SQL Statement
        cur.execute(sql)

        # Commits changes to DB
        conn.commit()

        # Closes DB Connection
        conn.close()

    def entries_to_csv(self, tablename, filename, startdate=None, enddate=None, user = "airq", password = "airq", host = "localhost", port = 3306, database = "airq_data"):
        """
        Exports entries from a database table to a CSV file. 
        If startdate and enddate are specified, only entries within this date range will be exported. 
        If they are not specified, all entries will be exported.

        Args:
            tablename (str): The name of the table from which to export entries.
            filename (str): The name of the CSV file to which entries should be exported.
            startdate (str, optional): The start date of the range of entries to export, in the format 'YYYY/MM/DD'. 
                                        Defaults to None, in which case all entries are exported.
            enddate (str, optional): The end date of the range of entries to export, in the format 'YYYY/MM/DD'. 
                                    Defaults to None, in which case all entries are exported.
            user (str, optional): Database username. Defaults to "airq".
            password (str, optional): Database password. Defaults to "airq".
            host (str, optional): Database host. Defaults to "localhost".
            port (int, optional): Database port. Defaults to 3306.
            database (str, optional): Database name. Defaults to "airq_data".
        """
        try:
            # Create a database connection
            SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
            engine = create_engine(SQLALCHEMY_DATABASE_URI)

            # Formulate the SQL query
            if startdate and enddate:
                query = f"SELECT * FROM {tablename} WHERE timestamp BETWEEN '{startdate}' AND '{enddate}'"
            else:
                query = f"SELECT * FROM {tablename}"

            # Use pandas to execute SQL query and get a DataFrame
            df = pd.read_sql_query(query, engine)

            # Write DataFrame to CSV file
            df.to_csv(filename, index=False)
        except Exception as e:
            print(f"An error occurred: {e}")

    def csv_to_db(self, csv_file, tablename, startdate=None, enddate=None, user = "airq", password = "airq", host = "localhost", port = 3306, database = "airq_data"):
        """
        Loads data from a CSV file to a database table. 
        If startdate and enddate are provided, only the data within this period will be loaded.

        Args:
            csv_file (str): Name of the CSV file to load the data from.
            tablename (str): Name of the table to load the data into.
            startdate (str, optional): Start date in 'YYYY/MM/DD' format to filter the data. Defaults to None.
            enddate (str, optional): End date in 'YYYY/MM/DD' format to filter the data. Defaults to None.
            user (str, optional): Database username. Defaults to "airq".
            password (str, optional): Database password. Defaults to "airq".
            host (str, optional): Database host. Defaults to "localhost".
            port (int, optional): Database port. Defaults to 3306.
            database (str, optional): Database name. Defaults to "airq_data".
        """
        try:
            # Convert the startdate and enddate to datetime objects
            if startdate:
                startdate = self.__pad_date(startdate)
            if enddate:
                enddate = self.__pad_date(enddate)

            # Read the CSV file into a DataFrame
            df = pd.read_csv(csv_file)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')  # Adjusted to match your date format

            # If a date range is specified, filter the data
            if startdate and enddate:
                mask = (df['timestamp'].dt.date >= startdate) & (df['timestamp'].dt.date <= enddate)
                df = df.loc[mask]

            # Create a database connection
            SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
            engine = create_engine(SQLALCHEMY_DATABASE_URI)

            # Write DataFrame to database
            df.to_sql(tablename, engine, if_exists='append', index=False)

        except Exception as e:
            print(f"An error occurred: {e}")

    def db_data_to_df(self, tablename, startdate=None, enddate=None, user = "airq", password = "airq", host = "localhost", port = 3306, database = "airq_data"):
        """Loads data from a database table into a pandas DataFrame. 
        If a startdate and enddate are provided, only the data within this period will be loaded.

        Args:
            tablename (str): Name of the table to load the data from.
            startdate (str, optional): Start date in 'YYYY/MM/DD' format to filter the data. Defaults to None.
            enddate (str, optional): End date in 'YYYY/MM/DD' format to filter the data. Defaults to None.
            user (str, optional): Database username. Defaults to "airq".
            password (str, optional): Database password. Defaults to "airq".
            host (str, optional): Database host. Defaults to "localhost".
            port (int, optional): Database port. Defaults to 3306.
            database (str, optional): Database name. Defaults to "airq_data".

        Returns:
            pandas.DataFrame: DataFrame containing the data from the specified table (and within the specified date range, if provided).
        """
        try:
            # Create a database connection
            SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
            engine = create_engine(SQLALCHEMY_DATABASE_URI)

            # Create SQL query
            if startdate and enddate:
                query = f"SELECT * FROM {tablename} WHERE timestamp >= '{startdate}' AND timestamp <= '{enddate}'"
            else:
                query = f"SELECT * FROM {tablename}"
            
            # Execute SQL query and store result in DataFrame
            df = pd.read_sql_query(query, engine)

            df.drop('measuretime', axis = 1, inplace=True)
            df.drop('health', axis = 1, inplace=True)
            df.drop('performance', axis = 1, inplace=True)

            # Return DataFrame
            return df

        except Exception as e:
            print(f"Error connecting to the database or executing the query: {e}")
            sys.exit(1)

    def get_data_range(self, tablename):
        """
        Connect to the database and fetch the range of timestamps from the given table.

        Args:
        tablename (str): The name of the table from which to fetch the data range.

        Returns:
        tuple: A tuple containing the earliest and latest timestamps, or None if an error occurs.
        """
        try:
            # Connect to the database
            conn = self.__connect_to_DB()

            # Create a cursor
            cur = conn.cursor()

            # Execute the query to fetch the earliest and latest timestamps
            cur.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {tablename}")

            # Fetch the result
            result = cur.fetchone()

            # Print the range of timestamps
            print(f"The range of timestamps in table '{tablename}' is from {result[0]} to {result[1]}.")

            # Return the range of timestamps
            return result

        except Exception as e:
            print(f"Error while fetching data range: {e}")
            return None


