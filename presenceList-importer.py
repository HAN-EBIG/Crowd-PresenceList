#!/usr/bin/python3

# Retrieves presenceList files from iSAS, parses them and adds data to the Ebig InfluxDB
# Fileformat:
# 0: AanwezigheidsregistratieID;
# 1: Datum v registreren;
# 2:  Lokaal;
# 3:  Rooster-timeslot van;
# 4: Rooster-timeslot tot;
# 5: startmoment aanwezigheidsregistratrie;
# 6: aantal aanwezig;
# 7: aantal te laat

from influxdb import InfluxDBClient
import datetime
import arrow
import pytz
import sys

from pathlib import Path

if sys.version_info[0] >= 3:
    from urllib.request import urlretrieve
else:
    from urllib import urlretrieve

existingTuples = set()

def readExistingPresenceItems(registrationDate):
    existingPresenceFile = Path(registrationDate)
    if existingPresenceFile.is_file():
        with open(registrationDate, "r") as inFile:
            for line in inFile:
                tuples = line.split(";")
                if(tuples.__len__() <= 7):
                    continue
                else:
                    existingTuples.add(tuples[0])

def influx_handler(room, registrationTime, numPeople):
    series = []
    datapackage = {
        'measurement': 'room',
        'tags': {
            'roomLocation': room,
        },
        'fields': {
            'numPeople' : numPeople
        },
        'time': registrationTime,
    }
    series.append(datapackage)
    client = InfluxDBClient('145.74.104.50', 8086, 'sensorcontroller', '@password@', 'ebig')
    client.write_points(series)

room_dict = {'D104': 'R26-D-1.04',
             'D112': 'R26-D-1.12',
             'E012': 'R26-E-0.12'
             }

currentDate = arrow.now().format('YYYY-MM-DD')
local_tz = pytz.timezone('Europe/Amsterdam')

print("Importing presence list for %s" % currentDate)
url = "http://sascalendar.han.nl/getrss.aspx?id=audfp&type=UWE_AWR_HTML:" + currentDate
readExistingPresenceItems(currentDate)
urlretrieve (url, currentDate)

with open(currentDate, "r") as inFile:
    for line in inFile:
        if(line.split(";").__len__() <= 7):
            continue
        presenceTuple = line.split(";")
        if(existingTuples.__contains__(presenceTuple[0])):
            print("Skipping existing tuple.")
            continue
        room = presenceTuple[2]
        registrationTime = presenceTuple[5]
        numPresent = int(presenceTuple[6])
        numTooLate = int(presenceTuple[7])
        dateFormatted = currentDate + "T" + registrationTime + "Z"

        datetime_local = local_tz.localize(datetime.datetime.strptime(dateFormatted, '%Y-%m-%dT%H:%M:%SZ'),is_dst=None)
        datetime_utc = datetime_local.astimezone(pytz.utc)
        influx_handler(room_dict[room], datetime_utc, numPresent + numTooLate)
