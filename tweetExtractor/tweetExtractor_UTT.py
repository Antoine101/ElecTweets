from typing import Counter
import tweepy
import tokens
import csv
import json
import datetime
import math
import os

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
        '1489995966132412422'
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

def getLimitDates(legislature):
    return {
        '2012': ["2012-05-09T00:00:00Z","2012-06-09T23:59:59Z"],
        '2017': ["2017-05-10T00:00:00Z","2017-06-10T23:59:59Z"],
        '2022': ["2022-05-11T00:00:00Z","2022-06-11T23:59:59Z"]
    }.get(legislature, ["2022-05-10T00:00:00Z","2022-05-10T00:00:00Z"])


extractFullTweet=True # extraction de toutes les infos du Tweet (pour le format json)
header = [["Time","Text"]]  #entete des fichiers de destination



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

    if False: print("data : ", data)

    if True:
        with open(destinationFile+'.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    if devPrint: print("\t -> Tweets stored in json "+destinationFile+'.json')


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



def extractTweetsFromListWithIterations(row, legislature):
    printDev=False
    next_token=0
    
    allTweets=[]

    #print("Legislature : ", legislature)
    #print("twitterIdList[legislature] : ", twitterIdList[legislature])


    if row[4] not in twitterIdList[legislature]:
        print("\t -> Pas candidat en ", legislature)
    else:
        print("\n\t -> Candidat en ", legislature, " !")
        start_time, end_time = getLimitDates(legislature)
        print("\n\tStart time : ", start_time[0:10], " - End time : ", end_time[0:10])

        print("         ... en cours : ")
        tweetCounter=0

        userTweets = API.get_users_tweets(id=row[4],
                                          max_results=limit,
                                          tweet_fields=tweet_fields,
                                          start_time=start_time,
                                          end_time=end_time
                                          )  # API sans next_token

        if userTweets[0] is not None:
            if 'next_token' in userTweets[3]:
                next_token = userTweets[3]['next_token']  # récupération du next_token pour la prochaine requête
            else:
                next_token = 0
            tweetCounter += len(userTweets[0])
            allTweets = allTweets + userTweets[0]



        iteration=0
        while next_token!=0:
            iteration+=1

            if printDev:
                print("tweetCounter ", str(tweetCounter), ' tweets recorded / ', str(totalNumberRequested),' => new request')
                print("tweetCounter : ", tweetCounter, " - next_token : ", next_token)

            if printDev: print("  -> next_token ", next_token)
            userTweets = API.get_users_tweets(id=row[4],
                                              max_results=limit,
                                              tweet_fields=tweet_fields,
                                              pagination_token=next_token,
                                              start_time=start_time,
                                              end_time=end_time
                                              ) # API avec next_token

            if printDev: print("userTweets[3] : ", userTweets)

            if userTweets[0] is not None:
                if 'next_token' in userTweets[3]:
                    next_token=userTweets[3]['next_token'] # récupération du next_token pour la prochaine requête
                else:
                    next_token=0
                tweetCounter+= len(userTweets[0])

                if printDev:
                    print("\n\n userTweets : ", userTweets[0][0:2])
                    print(" len : ", len(userTweets[0]))
                    print(" userTweets : ", type(userTweets[0]))

                allTweets=allTweets+userTweets[0]
                if printDev:
                    print(" allTweets : ", allTweets)
                    print(" len allTweets : ", len(allTweets))

        print("      Nombre de tweets récupérés : ", len(allTweets))

        filterdUserTweets = []
        filterdUserTweets = tweetExtraction(allTweets) # Fonction d'extraction des données des Tweets

        if printDev:
            print("\n filterdUserTweets : ", filterdUserTweets['filterdUserTweetsObject'])
            print("\n len filterdUserTweets : ", len(filterdUserTweets['filterdUserTweetsObject']))


        if tweetCounter>=totalNumberRequested:
            print(" loops over -> OK ")
        else:
            print("") # !!!!!!!!!!! loop over NOK !!!!!!!!!!!!

        fileName = row[4]+"__"+row[5]+"__"+row[1]+"__"+row[0]+"__"+str(legislature) #on stock dans un fichier qui aura comme format de nom id__@username__nom__prenom
        destination = destinationFolderTweetDepute + fileName #on stock dans le repertoire qui correspond aux tweet des deputes

        storeTweetsJson(filterdUserTweets['filterdUserTweetsObject'],destination) # méthode de sauvegarde au format json

    
def extractTweetsFromList(row):
    # try :
    devPrint = False # Permet d'activer/désactiver les print de développement
    
    userTweets = API.get_users_tweets(
        id=row[4], 
        max_results=limit, 
        tweet_fields=tweet_fields,
        start_time=start_time,
        end_time=end_time
        )
    
    if devPrint: 
        print("\n next_token :", userTweets[3]['next_token'])
        print("\n len :", len(userTweets))
        print("\n userTweets[0] :", userTweets[0])
    
    #Formating the tweets to store them
    filterdUserTweets = []
    filterdUserTweets = tweetExtraction(userTweets[0]) # Fonction d'extraction des données des Tweets
        
    if devPrint: 
        print("\n filterdUserTweetsRow :", filterdUserTweets['filterdUserTweetsRow']) # affichage du fichier au format txt ?
        print("\n filterdUserTweetsObject :", filterdUserTweets['filterdUserTweetsObject']) # affichage du fichier au format json
    
    fileName = row[4]+"__"+row[5]+"__"+row[1]+"__"+row[0]+"__"+str(legislature) #on stock dans un fichier qui aura comme format de nom id__@username__nom__prenom
    destination = destinationFolderTweetDepute + fileName #on stock dans le repertoire qui correspond aux tweet des deputes
    
    if devPrint: print("destination :", destination)
    print("\t -> Tweets from -- "+row[3]+" -- extracted successfully")    
    
    #storeTweets(filterdUserTweets['filterdUserTweetsRow'],destination) #on envoie le resultat a la methode qui aura pour role de stocker le resultat
    storeTweetsJson(filterdUserTweets['filterdUserTweetsObject'],destination) # méthode de sauvegarde au format json
    
    # except :
    #     print("\nAn error occured in the extraction process")
    
     

def getDeputeFromList():
    deputeCounter = 0
    treatedDeputeCounter =0

    if False : print("data_path :", data_path)

    # Récupération des id/username des personnes dont on veut récupérer les Tweets
    with open(data_path) as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=';')
        next(csv_reader) #intruction pour eviter la conlonne d'entete

        totalDeputesCounter = 0
        totalIdCounter = 0
        for row in csv_reader:
            totalDeputesCounter += 1

            if row[4]!="":
                totalIdCounter += 1
        #totalDeputesCounter = sum(1 for row in csv.reader(csv_file, delimiter=';') )
        if True:
            print("\nSynthèse fichier '" + candidatesFile + "' : " )
            print("\t -> Nombre de députés :", totalDeputesCounter)
            print("\t\t ... dont " + str(totalIdCounter) + " avec un ID Twitter" )


    with open(data_path, encoding='utf8') as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=';')
        next(csv_reader)  # intruction pour eviter la conlonne d'entete

        # Pour chaque personne du csv
        for row in csv_reader:

            if True: print("\nrow : ", row)

            if row and row[4]!="":

                deputeCounter+=1

                if personsToExportCounterMin<deputeCounter and deputeCounter<=min(totalDeputesCounter,personsToExportCounterMax): # interruption du code pour le dev
                
                    print("\t "+str(deputeCounter)+"/"+str(min(totalDeputesCounter,personsToExportCounterMax))+" >> Extracting tweets from "+row[0]+" "+row[1]+" // id : "+row[4])
                    print("\t total tweets Requested : ", str(totalNumberRequested), " - limit by request :", str(limit))
                    
                    if True:
                        if totalNumberRequested>limit: # besoin de faire plusieurs requêtes (en utilisant next_token)

                            for eachLegislature in ['2012', '2017', '2022']:
                                extractTweetsFromListWithIterations(row, eachLegislature)
                        else: # pas besoin de faire plusieurs requêtes à la suite  (avec next_token)
                            extractTweetsFromList(row)
                    treatedDeputeCounter+=1

    print("\n"+ str(treatedDeputeCounter) +" Comptes parcourus sur "+ str(deputeCounter))



if __name__ == "__main__":    
    print("\n=== CUSTOM TWEET EXTRACTOR - SPOT DEBAT V1.1 ===\n")

    nbLoops = int(math.ceil(totalNumberRequested/limit))
    print("Total number of necessary loops : ", str(nbLoops))
          
    getDeputeFromList()
    
    print("\n===! CUSTOM TWEET EXTRACTOR - SPOT DEBAT V1.1  !===\n")
    





