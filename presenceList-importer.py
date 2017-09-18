from influxdb import InfluxDBClient
import datetime
import arrow
import pytz
import sys

if sys.version_info[0] >= 3:
    from urllib.request import urlretrieve
else:
    # Not Python 3 - today, it is most likely to be Python 2
    # But note that this might need an update when Python 4
    # might be around one day
    from urllib import urlretrieve

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
urlretrieve (url, currentDate)


# 0: AanwezigheidsregistratieID;
# 1: Datum v registreren;
# 2:  Lokaal;
# 3:  Rooster-timeslot van;
# 4: Rooster-timeslot tot;
# 5: startmoment aanwezigheidsregistratrie;
# 6: aantal aanwezig;
# 7: aantal te laat


with open(currentDate, "r") as inFile:
    for line in inFile:
        if(line.split(";").__len__() <= 7):
            continue
        room = line.split(";")[2]
        registrationTime = line.split(";")[5]
        numPresent = int(line.split(";")[6])
        numTooLate = int(line.split(";")[7])
        dateFormatted = currentDate + "T" + registrationTime + "Z"

        datetime_local = local_tz.localize(datetime.datetime.strptime(dateFormatted, '%Y-%m-%dT%H:%M:%SZ'),is_dst=None)
        datetime_utc = datetime_local.astimezone(pytz.utc)
        print(datetime_utc)
        influx_handler(room_dict[room], datetime_utc, numPresent + numTooLate)
