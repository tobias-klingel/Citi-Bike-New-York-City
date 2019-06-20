import json
import urllib2
from datetime import datetime
import time
import json

##########################################################################
#Initial configuration

import sys
#Add additional path to make it possible to import files to the project
sys.path.insert(0, '/Users/OS_USERNAME/database_File_access/')
from mongoDBLib import MongoDB

dbName = "city_bike"
dbCollectionName = "citiBikeStations"

mongo = MongoDB("mongodb://USER:PASS@IP:PORT/DB", dbName, dbCollectionName)


#URL of the cuurent status of all citibike stations in NYC
url = ("http://feeds.citibikenyc.com/stations/stations.json")

##########################################################################

#Write initally each station in a seperated document in MongoDB
def initialStationToDataBaseWrite():
    print "Start writing station data to database"
    content = None
    #Scrape citiBike data from online source
    content = urllib2.urlopen(url).read()
    #Put data in JSON format
    respJSON = json.loads(content)
    print "Started " + str(datetime.now())
    print "Found " + str(len(respJSON["stationBeanList"])) + " stations"
    #Creates a document for each citiBike station in MongoDB
    for i in range(len(respJSON["stationBeanList"])):
        station = respJSON["stationBeanList"][i]
        citi_data = {
            '_id': '{}'.format(station["id"]),
            'city': '{}'.format(station["city"]),
            'altitude': '{}'.format(station["altitude"]),
            'stAddress2': '{}'.format(station["stAddress2"]),
            'longitude': '{}'.format(station["longitude"]),
            'postalCode': '{}'.format(station["postalCode"]),
            'stAddress1': '{}'.format(station["stAddress1"]),
            'stationName': '{}'.format(station["stationName"]),
            'landMark': '{}'.format(station["landMark"]),
            'latitude': '{}'.format(station["latitude"]),
            'statusKey': '{}'.format(station["statusKey"]),
            'location': '{}'.format(station["location"]),
            'statistics': [{'lastCommunicationTime':'{}'.format(station["lastCommunicationTime"]),
                            'totalDocks': '{}'.format(station["totalDocks"]),
                            'availableBikes': '{}'.format(station["availableBikes"]),
                            'availableDocks': '{}'.format(station["availableDocks"]),
                            'statusValue': '{}'.format(station["statusValue"]),
                            'statusKey': '{}'.format(station["statusKey"])}]

        }
        #Write new object to MongoDB
        mongo.writeInDB(citi_data)
        #Debug
        #print "Added station " + str(station["id"]) + " to the database"
    print "Finished " + str(datetime.now())
    print "#######################################"

##########################################################################

#Update one document (one station) based on the ID (station number)
#One document hosts all the information over time
def updateStationData(id, lastCommunicationTime, totalDocks, availableBikes, availableDocks, statusValue, statusKey):
    #This object will be added to the statistics array and contains the updates information
    obj = {
        'statistics': {'lastCommunicationTime': '{}'.format(lastCommunicationTime),
                        'totalDocks': '{}'.format(totalDocks),
                        'availableBikes': '{}'.format(availableBikes),
                        'availableDocks': '{}'.format(availableDocks),
                        'statusValue': '{}'.format(statusValue),
                        'statusKey': '{}'.format(statusKey)}

    }
    #Debug
    #print "Change ID: " +str(id) + " - " + str(lastCommunicationTime) + " - " + str(totalDocks) + " - " + str(availableBikes) + " - " + str(availableDocks) + " - " + str(statusValue) + " - " + str(statusKey)
    return mongo.updateCollection(id, obj)


##########################################################################

#Update just some data in each doucument(station) based on the station ID
def main():
    print "Start writing station data to database"
    print "Started " + str(datetime.now())
    content = None
    content = urllib2.urlopen(url).read()
    #Put data in JSON format
    respJSON = json.loads(content)

    #Updates new scraped citiBike data in MongoDB station by station
    for i in range(len(respJSON["stationBeanList"])):
        stationData = respJSON["stationBeanList"][i]
        print updateStationData(stationData["id"],stationData["lastCommunicationTime"],stationData["totalDocks"],stationData["availableBikes"],
                          stationData["availableDocks"],stationData["statusValue"],stationData["statusKey"])

    print "#######################################"
    print "Finished " + str(datetime.now())
    #Waiting time until retrive new dataset of citiBike stations
    time.sleep(180)
    print "#######################################"

##########################################################################
##########################################################################


#This needs to be done only once
##initialStationToDataBaseWrite()

#Calls the scraping process in a endless loop
while True:
    main()
