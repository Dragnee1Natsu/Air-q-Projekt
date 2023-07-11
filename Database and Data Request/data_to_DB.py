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


########################
airqIP = '192.168.4.1'
airqpass = 'airqsetup'
#########################

#Functions for de-and encoding the messages 
def unpad(data):
  return data[:-ord(data[-1])]

def pad(data):
  length = 16 - (len(data) % 16)
  return data + chr(length).encode('utf-8')*length

def decodeMessage(msgb64):
  # Erster Schritt: base64 dekodieren
  msg = base64.b64decode(msgb64)

  # AES-Schlüssel der Länge 32 aus dem air-Q-Passwort erstellen
  key = airqpass.encode('utf-8')
  if len(key) < 32:
    for i in range(32-len(key)):
      key += b'0'
  elif len(key) > 32:
    key = key[:32]

  # Zweiter Schritt: AES256 dekodieren
  cipher = AES.new(key=key, mode=AES.MODE_CBC, IV=msg[:16])
  return unpad(cipher.decrypt(msg[16:]).decode('utf-8'))

def encodeMessage(msg):
  # AES-Schlüssel der Länge 32 aus dem air-Q-Passwort erstellen
  key = airqpass.encode('utf-8')
  if len(key) < 32:
    for i in range(32-len(key)):
      key += b'0'
  elif len(key) > 32:
    key = key[:32]

  # Erster Schritt: AES256 verschlüsseln
  iv = Random.new().read(AES.block_size)
  cipher = AES.new(key=key, mode=AES.MODE_CBC, IV=iv)
  msg = msg.encode('utf-8')
  crypt = iv + cipher.encrypt(pad(msg))

  # Zweiter Schritt: base64 enkodieren
  msgb64 = base64.b64encode(crypt).decode('utf-8')
  return msgb64

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

# This function gets the latest timestamp from a table
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
    return timestamp[0]

#Funktion zur erstellung des SQL-Statements
def sql_data(contents, conn ,cur, table):
  #content wird Zeile für Zeile verarbeitet
  for line in contents.read().split(b'\n'):
      if line != b'':
          #Message wird dekodiert
          line = decodeMessage(line)
          #print(line)

          #Zeile wird in ein dict konvertiert
          line= json.loads(line)

          columns = ""
          values = ""

          if line["Status"] != "OK":
                  #print(line["Status"])
                  print("Skipped invalid measurements due to warm-up of the Sensor")
                  continue
          
          for type in line:
              #Diese Daten werden herausgefiltert und nicht mit in die Datenbank mit aufgenommen
              if type == "bat" or type == "DeviceID" or type == "uptime" or type == "window_event" or type == "door_event" or type == "person" or type == "window_open" or type == "Status":
                continue

              
              #Erstellt den String für die Columns
              columns += type 

              #Erstellt den String für die Values 
              #Edge-case: wenn value eine Liste ist
              if isinstance(line[type], list):
                  values += str(line[type][0])
              elif type == "timestamp":
                  values += "FROM_UNIXTIME(%s)" % (int(line[type]/1000))
              else:  
                  values += str(line[type])

              #Edge-case: Am Ende kein Komma
              if type != "cnt0_3":  
                  columns += ", "
                  values += ", "

          sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s );" % (table, columns, values)
          #print(sql)
          try:
            cur.execute(sql)
            conn.commit()
          except Exception as e:
            print(f"An error occurred: {e}")

          #cur.execute(sql)
          #conn.commit()

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

def pad_date(date_string):
    year, month, day = map(int, date_string.split('/'))
    return datetime.date(datetime(year, month, day))



def file_to_db(startdate, enddate, tablename):
    # Get latest entry in table
    latest = get_latest_timestamp(tablename)

    # Connect to DB
    conn = connect_to_DB(tablename)
    # Get Cursor
    cur = conn.cursor()

    startdate = pad_date(startdate)
    enddate = pad_date(enddate)

    # check if latest entry is further than startdate
    if latest is not None:
        print("_____________________________________________________________________________")
        print("Latest enrty in table '" + tablename + "' is " + datetime.strftime(latest, "%Y-%m-%d"))
        # change startdate for performance
        if latest > startdate:
            startdate = latest
    
    # Verbindung zum air-Q aufbauen
    connection = http.client.HTTPConnection(airqIP)

    end = enddate
    end += timedelta(days=1)

    while startdate < end:

        year = str(startdate.year)
        month = str(startdate.month)
        day = str(startdate.day)

        print("_____________________________________________________________________________")
        print("Year: " + year + "\n" + "Month: " + month + "\n" + "Day: " + day + "\n")

        connection.request("GET","/dir?request="+encodeMessage(year+"/"+month+"/"+day))
        files = connection.getresponse()
        files = json.loads(decodeMessage(files.read()))
        for file in files:
            print("File: " + file)   
            connection.request("GET","/file?request="+encodeMessage(year+"/"+month+"/"+day+"/"+file))
            contents = connection.getresponse()
            if contents.status == 200:
                print("Status: " + str(contents.status) + "/OK")
            else:
                print("Status: " + str(contents.status) + "/Canceled!")
                return
            
            sql_data(contents, conn, cur, tablename)
        
        startdate += timedelta(days=1)

    conn.commit()
    # Verbindung trennen
    connection.close()
    

#file_to_db("2023/06/08", "2023/06/22", "alex" )
file_to_db("2023/06/24", "2023/07/11", "home" )

