from typing import Counter
import tweepy
import csv
import json
import datetime
import math
import os

from datetime import datetime, timedelta

import tokens
from checkJson import checkIfFileAlreadyExists, getLastTweetDateInJson, loadjson

'''
LISTE DES APIs tweepy get (non exhaustive) :

    search_recent_tweets (from query key words)
    get_users_tweets (from User ID)
    
    get_liking_users (from Tweet ID) -> Allows you to get information about a Tweet’s liking users.
    get_liked_tweets (from User ID) -> Allows you to get information about a user’s liked Tweets.
    get_retweeters (from Tweet ID) -> Allows you to get information about who has Retweeted a Tweet.
    get_users_mentions (from User ID) -> Returns Tweets mentioning a single user specified by the requested user ID. 
    get_users_followers (from User ID) -> Returns a list of users who are followers of the specified user ID.
    get_users_following (from User ID) -> Returns a list of users the specified user ID is following.
    
    get_recent_tweets_count (from query key words) -> The recent Tweet counts endpoint returns count of Tweets from the last seven days that match a search query.

'''

#Initialisation des parametres de l'auth de l'api
auth = tweepy.OAuthHandler(tokens.consumer_key, tokens.consumer_secret)
auth.set_access_token(tokens.access_token, tokens.access_token_secret)
api = tweepy.API(auth)
API = tweepy.Client(bearer_token=tokens.bearer_token, 
                    consumer_key=tokens.consumer_key,
                    consumer_secret=tokens.consumer_secret, 
                    access_token=tokens.access_token, 
                    access_token_secret=tokens.access_token_secret,
                    return_type=[dict])


#Déclarations des variables 
sourceFile = "data/twitterNames.csv" # Fichier d'entrée avec la liste des persones dont on veut récupérer les Tweets. Les personnes sont identifiées par leur usernames (@XXX) et de leur ID Twitter 



current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(current_dir + "/../")
candidatesFile = "candidats.csv"
data_dir = parent_dir + "\data\\"

data_path = data_dir + candidatesFile
destinationFolderTweetDepute=data_dir #"data/01_tweetList/"  #prefix dossier de sortie


limit = 100 # Nombre de tweets max par requete. Limité à XXX (à retrouver) par l'API Twitter !
totalNumberRequested=3100 # 3100 # nombre de Tweets désirés pour chaque personne. Si totalNumberRequested>limit, alors plusieurs requêtes sont lancées à la suite pour récupérer les totalNumberRequested Tweets.
personsToExportCounterMin=0 # première valeur du counter pour laquelle on récupère les personnes. for option 1 only
personsToExportCounterMax=2000 # for option 1 only


tweet_fields=['attachments','author_id','context_annotations','conversation_id','created_at','entities','geo','id','in_reply_to_user_id','lang','possibly_sensitive','public_metrics','referenced_tweets','reply_settings','source','text','withheld'] # Liste des paramètres des Tweets à récupérer

#start_time="2017-05-14T00:00:00Z"
#end_time="2022-05-14T00:00:00Z"

twitterIdList = {
    '2022': [
        '1130169982917180000',
        '1207401907129659397',
        '943672818',
        '376865423',
        '3255832605',
        '329686497',
        '1489995966132412422',
        '49627758',
        '1203299532655770000',
        '539156968',
        '425231735',
        '1156135942660460000',
        '2933680894',
        '371381075',
        '371381075',
        '1536288124195810000',
        '15719595',
        '17747931',
        '153108420',
        '28308363',
        '1537768362658480000'
    ],
    '2017': [
        '1009368668579516416',
        '29447272',
        '3417457690',
        '1326458542266736642',
        '68457158',
        '377865606',
        '1519977276728881155',
        '58584212',
        '2188765218',
        '493483010',
        '1326458542266736642',
        '1728425251',
        '869912215691960320',
        '3362289555',
        '1054417619338620929'
    ],
    '2012': [
        '1326458542266736642',
        '475208227',
        '73968763',
        '392528127',
        '85285173',
        '295577929',
        '1011204318',
        '780405831930089000',
        '14233770',
        '740207052581175000'
    ]
}


extractFullTweet=True # extraction de toutes les infos du Tweet (pour le format json)
header = [["Time","Text"]]  #entete des fichiers de destination


def getLimitDates(legislature):
    return {
        '2012': ["2012-05-09T00:00:00Z","2012-06-09T23:59:59Z"],
        '2017': ["2017-05-10T00:00:00Z","2017-06-10T23:59:59Z"],
        '2022': ["2022-05-11T00:00:00Z","2022-06-11T23:59:59Z"]
    }.get(legislature, ["2022-05-10T00:00:00Z","2022-05-10T00:00:00Z"])


def storeTweets(data,destinationFile):
    devPrint = False
    if devPrint: 
        print("\n storeTweets -> data :",data)
        print("\n storeTweets -> destinationFile :",destinationFile)
    
    with open(destinationFile+".csv", 'w+', encoding="utf-8") as f:
        write = csv.writer(f)
        write.writerows(header)
        write.writerows(data)
    
    if devPrint: print("\t -> Tweets stored in file "+destinationFile)

def storeTweetsJson(data,destinationFile):
    devPrint = False
    if devPrint:
        print("\n storeTweets -> data :",data)
        print("\n storeTweets -> destinationFile :",destinationFile)

    if devPrint: print("data : ", data)

    if True:
        with open(destinationFile, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    if devPrint: print("\t -> Tweets stored in json "+destinationFile)


# Fonction permettant de récupérer et d'organiser le contenu d'un Tweet.
def tweetExtraction(userTweets):
    devPrint = False
    
    returnResult={}
    filterdUserTweetsRow = [] # Extraction au format txt ?
    filterdUserTweetsObject = [] # Extraction au format json
    counter = 0
    
    if False: print("\n🐤 len(userTweets) :", len(userTweets))
    
    if userTweets is None:
        tempObject={}
        tempObject['no matching Tweet']=""
        filterdUserTweetsObject.append(tempObject)
    else:
        for tweet in userTweets:
            counter+=1
            if counter<2 and False: 
                print("\n🐤 tweet ",str(counter)," :", tweet.keys())
            # tempRow = [tweet.created_at, tweet.full_text if 'full_text' in tweet._json else tweet.text] #pour chaque tweet on recupère la date et le texte
            tempObject={} # Réinitialisation de tempObject
            if False: #extractFullTweet: # Extraction de toutes les infos du Tweet au format json
                tempObject=tweet._json
            else:  # Extraction de quelques infos du Tweet au format json
                tempObject['id']=str(tweet.id)
                tempObject['text']=tweet.text 
                tempObject['created_at']=str(tweet.created_at)
                
                tempObject['attachments']=tweet.attachments
                tempObject['author_id']=tweet.author_id
                tempObject['context_annotations']=tweet.context_annotations
                tempObject['conversation_id']=tweet.conversation_id
                tempObject['entities']=tweet.entities
                tempObject['geo']=tweet.geo
                tempObject['in_reply_to_user_id']=tweet.in_reply_to_user_id
                tempObject['lang']=tweet.lang
                tempObject['possibly_sensitive']=tweet.possibly_sensitive
                tempObject['public_metrics']=tweet.public_metrics
                # tempObject['referenced_tweets']=tweet.referenced_tweets
                tempObject['reply_settings']=tweet.reply_settings
                tempObject['source']=tweet.source
                tempObject['withheld']=tweet.withheld
                
                
                if counter<5 and False:
                    print(" // tweet.text :",tweet.public_metrics)
                    
            filterdUserTweetsObject.append(tempObject)
    
    
    returnResult['filterdUserTweetsRow']=filterdUserTweetsRow
    returnResult['filterdUserTweetsObject']=filterdUserTweetsObject
    
    return returnResult



def fileNameBuilder(row, legislature):
    return row['id_twitter']+"__"+row['username']+"__"+row['nom']+"__"+row['prenom']+"__"+str(legislature)+'.json'


def extractTweetsFromListWithIterations(row, fileName, legislature, newestTweet=-1):
    printDev=False
    next_token=0
    
    allTweets=[]



    callApiBool = True

    start_time = getLimitDates(legislature)[0]
    end_time = getLimitDates(legislature)[1]

    if newestTweet != -1:
        print("\n\t\tVérification des dates de récupération des Tweets... ")

        if False:
            print("startRequestedDate : ", start_time)
            print("endRequestedDate : ", end_time)
            print("lastTweetDate : ", newestTweet)
        startRequestedDate_dateFormat = datetime.strptime(str(start_time).replace('T', ' ').replace('Z', ''), '%Y-%m-%d %H:%M:%S')
        endRequestedDate_dateFormat = datetime.strptime(str(end_time).replace('T', ' ').replace('Z', ''), '%Y-%m-%d %H:%M:%S')
        lastTweetDate_dateFormat = datetime.strptime(str(newestTweet).replace('T', ' ').replace('Z', ' '), '%Y-%m-%d %H:%M:%S')

        # Recalage de la date de début de recherche à la date du dernier Tweet + 1 seconde
        if lastTweetDate_dateFormat>startRequestedDate_dateFormat and lastTweetDate_dateFormat<endRequestedDate_dateFormat:
            start_time = (lastTweetDate_dateFormat+timedelta(0,1)).isoformat()+"Z"
            print("\t\t\t - Start time recalé à la date du dernier Tweet + 1sec : ", start_time)

        print("\t\t\t - New dates for request :\n\t\t\t\t -> Start time :  ", start_time, " - End time : ", end_time)

        print("\t\t\t... fin de vérification des dates de récupération des Tweets ")


    if not callApiBool:
        print("\n!!! Pas d'appel de l'API Twitter !!!")
    else:
        print("\n\t\tLancement de la fonction API en cours... ")

        tweetCounter=0
        # Premier appel de l'API Tweeter
        userTweets = API.get_users_tweets(id=row['id_twitter'],
                                          max_results=limit,
                                          tweet_fields=tweet_fields,
                                          start_time=start_time,
                                          end_time=end_time
                                          )  # API sans next_token

        # Récupération du 'next_token' (s'il existe) et sauvegarde des Tweets
        if userTweets[0] is not None:
            if 'next_token' in userTweets[3]:
                next_token = userTweets[3]['next_token']  # récupération du next_token pour la prochaine requête
            else:
                next_token = 0
            tweetCounter += len(userTweets[0])
            allTweets = allTweets + userTweets[0]


        # Relance de l'API si la clé next_token existe
        iteration=0
        while next_token!=0:
            iteration+=1

            userTweets = API.get_users_tweets(id=row['id_twitter'],
                                              max_results=limit,
                                              tweet_fields=tweet_fields,
                                              pagination_token=next_token,
                                              start_time=start_time,
                                              end_time=end_time
                                              ) # API avec next_token

            if userTweets[0] is not None:
                if 'next_token' in userTweets[3]:
                    next_token=userTweets[3]['next_token'] # récupération du next_token pour la prochaine requête
                else:
                    next_token=0
                tweetCounter+= len(userTweets[0])
                allTweets=allTweets+userTweets[0]

        print("\n\t\tNombre de nouveaux tweets récupérés : ", len(allTweets))

        filterdUserTweets = []
        filterdUserTweets = tweetExtraction(allTweets) # Fonction d'extraction des données des Tweets
        newTweets = filterdUserTweets['filterdUserTweetsObject']

        if printDev:
            print("\n filterdUserTweets : ", filterdUserTweets['filterdUserTweetsObject'])
            print("\n len filterdUserTweets : ", len(filterdUserTweets['filterdUserTweetsObject']))


        if tweetCounter>=totalNumberRequested:
            print(" loops over -> OK ")
        else:
            print("") # !!!!!!!!!!! loop over NOK !!!!!!!!!!!!

        destination = destinationFolderTweetDepute + fileName #on stock dans le repertoire qui correspond aux tweet des deputes

        originalJson = loadjson(fileName)
        # print("originalJson : ", originalJson)

        if len(originalJson)>0:
            jsonListToSave  = newTweets + originalJson
        else:
            jsonListToSave = newTweets

        if printDev:
            print("\nnewTweets : ", newTweets)
            print("\njsonListToSave : ", jsonListToSave)

        if len(newTweets)>0 or len(originalJson)==0:
            print("\t    🐤 Sauvegarde du fichier :", fileName)
            storeTweetsJson(jsonListToSave,destination) # méthode de sauvegarde au format json
        else:
            print("\t    🐤 Pas de Tweets -> Pas de sauvegarde")


def getDeputesList():
    # Récupération des id/username des personnes dont on veut récupérer les Tweets

    deputesListe = []

    # Chargement du fichier .csv contenant la liste des députés
    with open(data_path, encoding='utf8') as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=';')
        next(csv_reader)  # pour éviter la colonne d'en-tête

        totalDeputesCounter = 0
        withIdCounter = 0


        # Pour chaque personne du csv
        for row in csv_reader:
            totalDeputesCounter += 1

            if row:
                rowItems = {}
                rowItems['prenom'] = row[1]
                rowItems['nom'] = row[2]
                rowItems['sexe'] = row[3]
                rowItems['date_naissance'] = row[4]
                rowItems['id_twitter'] = row[5]
                rowItems['username'] = row[6]
                rowItems['compte_verifie'] = row[7]
                rowItems['date_creation_compte'] = row[8]


                if rowItems['id_twitter'] != "":
                    withIdCounter+=1
                    deputesListe.append(rowItems)

    print("\nSynthèse fichier '" + candidatesFile + "' : ")
    print("\t -> Nombre de députés :", totalDeputesCounter)
    print("\t\t ... dont " + str(withIdCounter) + " avec un ID Twitter")

    if False : print("\ndeputesListe '", deputesListe[0:2])

    return deputesListe


def checkLaunchTwitterAPI(deputesListe):
    deputeCounter = 0
    treatedDeputeCounter = 0
    totalDeputesCounter = len(deputesListe)

    # Pour chaque personne du csv
    for row in deputesListe:

        if True: print('\n\n====== Traitement de',row['prenom'],row['nom'],' ======')

        deputeCounter+=1

        if personsToExportCounterMin<deputeCounter and deputeCounter<=min(totalDeputesCounter,personsToExportCounterMax): # interruption du code pour le dev

            if False:
                print("\t "+str(deputeCounter)+"/"+str(min(totalDeputesCounter,personsToExportCounterMax))+" >> Extracting tweets from "+row['prenom']+" "+row['nom']+" // id : "+row['id_twitter'])
                print("\t total tweets Requested : ", str(totalNumberRequested), " - limit by request :", str(limit))

            for eachLegislature in ['2012', '2017', '2022']:

                fileName = fileNameBuilder(row, eachLegislature)  # on stock dans un fichier qui aura comme format de nom id__@username__nom__prenom

                if row['id_twitter'] not in twitterIdList[eachLegislature]:
                    print("\n\t -> Pas candidat en ", eachLegislature)
                else:
                    print("\n\t -> Candidat en ", eachLegislature, " !")
                    start_time, end_time = getLimitDates(eachLegislature)
                    print("\t\tStart time : ", start_time, " - End time : ", end_time)

                    if checkIfFileAlreadyExists(fileName):
                        print("\n\t\tCe député est déjà connu dans la base de données ! (répertoire 'data') ")
                        newestTweet = getLastTweetDateInJson(fileName)
                        print("\t\t\t - newestTweet : ", newestTweet)

                    else:
                        print("\t\tNouveau député")

                    extractTweetsFromListWithIterations(row, fileName, eachLegislature, newestTweet)

            treatedDeputeCounter+=1

    print("\n"+ str(treatedDeputeCounter) +" Comptes parcourus sur "+ str(deputeCounter))



if __name__ == "__main__":    
    print("\n=== CUSTOM TWEET EXTRACTOR - SPOT DEBAT V1.1 ===\n")

    nbLoops = int(math.ceil(totalNumberRequested/limit))
    print("Total number of necessary loops : ", str(nbLoops))
          
    deputesListe = getDeputesList()
    checkLaunchTwitterAPI(deputesListe)

    print("\n===! CUSTOM TWEET EXTRACTOR - SPOT DEBAT V1.1  !===\n")
    





