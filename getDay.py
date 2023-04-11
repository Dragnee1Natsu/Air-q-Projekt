#!/usr/bin/python3

# Downloads a specific day from an air-Q and converts it to a CSV file

import base64
from Crypto.Cipher import AES
import http.client
from Crypto import Random
import json
import time


##########################
# Put your request here:

airqIP   = '192.168.4.1'
airqpass = 'airqsetup'

year  = 2021
month = 10
day   = 27


##########################
# Other settings:

csvHeader = True
csvDelimiter = ","
exclude = ['Status','DeviceID','bat','dCO2dt','dHdt','measuretime','cnt0_3','cnt0_5','cnt1','cnt2_5','cnt5','cnt10','door_event','person','window_event','window_open','uptime','timestamp']


def encodeMessage(msg):
    try:
        # first step encode AES
        key = airqpass.encode('utf-8')
        if len(key) < 32:
            for i in range(32-len(key)):
                key += b'0'
        elif len(key) > 32:
            key = key[:32]
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(key=key, mode=AES.MODE_CBC, IV=iv)
        msg = msg.encode('utf-8')
        crypt = iv + cipher.encrypt(pad(msg))
        # second step encode base64
        msgb64 = base64.b64encode(crypt).decode('utf-8')
        return msgb64
    except Exception as e:
        print('Error - '+str(type(e))+' '+str(e))


def pad(data):
    length = 16 - (len(data) % 16)
    return data + chr(length).encode('utf-8')*length


def unpad(data):
    return data[:-ord(data[-1])]


def decodeMessage(msgb64):
    try:
        # first step decode base64
        msg = base64.b64decode(msgb64)
        # second step decode AES
        key = airqpass.encode('utf-8')
        if len(key) < 32:
            for i in range(32-len(key)):
                key += b'0'
        elif len(key) > 32:
            key = key[:32]
        cipher = AES.new(key=key, mode=AES.MODE_CBC, IV=msg[:16])
        return unpad(cipher.decrypt(msg[16:]).decode('utf-8'))
    except Exception as e:
        print('Error - '+str(type(e))+' '+str(e))


# get file names in selected day directory
connection = http.client.HTTPConnection(airqIP)
connection.request("GET","/dir?request="+encodeMessage("{}/{}/{}".format(year,month,day)))

contents = connection.getresponse()
filenames = json.loads(decodeMessage(contents.read()))
connection.close()


# download all files in directory
i = 0
for filename in filenames:
    i += 1
    print("Processing file {} of {}".format(i,len(filenames)), end="\r")
    connection = http.client.HTTPConnection(airqIP)
    connection.request("GET","/file?request="+encodeMessage("{}/{}/{}/{}".format(year,month,day,filename)))

    contents = connection.getresponse()

    with open("air-Q_{}-{}-{}.csv".format(year,month,day),"a") as f:
        for line in contents.read().split(b'\n'):
            if line != b'':
                datapacket = json.loads(decodeMessage(line))
                entries = list(datapacket.keys())
                entries.sort()
                csvString = ""
                if csvHeader is True:
                    csvHeader = False
                    csvString += "Date,"
                    for entry in entries:
                        if entry not in exclude:
                            csvString += str(entry)
                            csvString += csvDelimiter
                    csvString += "\n"
                csvString += "{},".format(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(datapacket['timestamp']/1000)))
                for entry in entries:
                    if entry not in exclude:
                        if isinstance(datapacket[entry],list):
                            csvString += str(datapacket[entry][0])
                        else:
                            csvString += str(datapacket[entry])
                        csvString += csvDelimiter
                csvString += "\n"
                f.write(csvString)
    connection.close()

print('File "{}" successfully created!'.format("air-Q_{}-{}-{}.csv".format(year,month,day)))
