import os
import json

import matplotlib
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime


def getPathToJson():
    current_dir = os.path.abspath(os.path.dirname(__file__))
    parent_dir = os.path.abspath(current_dir + "/../")
    pathToJson = parent_dir + '\\data\\'

    printDev=False
    if printDev :
        print('current_dir : ' + current_dir)
        print('parent_dir : ' + parent_dir)
        print('pathToJson : ' + pathToJson)

    return pathToJson


def getAllJsonFilesList():
    pathToJson = getPathToJson()

    json_files = [pos_json for pos_json in os.listdir(pathToJson) if pos_json.endswith('.json')]
    nb_json_files = len(json_files)
    if False : print("\nNb de json identifiés : ", nb_json_files)

    return json_files


def checkIfFileAlreadyExists(fileName=''):
    json_files = getAllJsonFilesList()
    if False: print('json_files : ', json_files)

    if fileName!="" and fileName in json_files:
        return True
    else:
        return False


def loadjson(fileName):
    data = {}

    if fileName != "":
        pathToJson = getPathToJson()
        fullPath = pathToJson + fileName

        with open(fullPath, 'r', encoding="utf8") as json_file:
            data = json.load(json_file)

    return data


def getLastTweetDateInJson(fileName=''):
    data = loadjson(fileName)
    newestDate = -1

    print("\t\t\t - Nombre de Tweets dans le fichier existant : ", len(data))

    if len(data) > 0:
        fullDateList = []
        dayDateList = []
        for eachTweet in data:
            fullDateList.append(datetime.strptime(eachTweet['created_at'][0:19], '%Y-%m-%d %H:%M:%S'))
            dayDateList.append(datetime.strptime(eachTweet['created_at'][0:10], '%Y-%m-%d'))
        if False:
            print("\t -> oldest Tweet : ", min(fullDateList))
            print("\t -> newest Tweet : ", max(fullDateList))
        newestDate=max(fullDateList).isoformat()

    return newestDate

#getLastTweetDateInJson("73968763__soniaoue__Ouertani__Sonia__2012.json")

def getTweets(): # Pas utilisé

    json_files = getAllJsonFilesList()
    nb_json_files = len(json_files)

    firstJsonToAnalyse = 0
    lastJsonToAnalyse = 300

    pathToJson = getPathToJson()

    ind = 0
    for eachJson in json_files:

        if ind >= firstJsonToAnalyse and ind <= lastJsonToAnalyse:
            print("\n -> (", str(ind), "/", str(nb_json_files), ") : ", eachJson)

            fullPath = pathToJson + eachJson

            with open(fullPath, 'r', encoding="utf8") as json_file:
                data = json.load(json_file)

            print("\t -> Nombre de Tweets : ", len(data))

            if len(data)>0:
                fullDateList = []
                dayDateList = []
                for eachTweet in data:
                    fullDateList.append(datetime.strptime(eachTweet['created_at'][0:19], '%Y-%m-%d %H:%M:%S'))
                    dayDateList.append(datetime.strptime(eachTweet['created_at'][0:10], '%Y-%m-%d'))
                print("\t -> oldest Tweet : ", min(fullDateList))
                print("\t -> newest Tweet : ", max(fullDateList))

                plotFiguresBool = False

                if plotFiguresBool:
                    plt.hist(dayDateList, len(set(dayDateList)), histtype='bar')
                    plt.xticks(rotation=45)
                    plt.title('matplotlib.pyplot.hist() function Example\n\n', fontweight="bold")
                    plt.show()

        ind += 1

#getTweets()