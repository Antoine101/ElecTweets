
import pandas as pd
import psycopg2 #depreciated use instead from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")


dbname="twitter_elections"
user="admin"
host="database"
password="password"

# Create a connection to the database
conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password)
#conn = psycopg2.connect(dbname="ElecTweets", user="postgres", host="localhost")

candidates_df = pd.read_sql_query(
"""SELECT 
candidats.id AS id_candidat,
prenom,
nom,
sexe,
date_naissance,
id_twitter,
compte_verifie,
date_creation_compte,
annee_election,
date_premier_tour,
affiliation_elections.nom_annee_election,
nom_derniere_election,
sortant,
dissident,
parti_vainqueur_presidentielles,
resultat_election

FROM public.candidats
INNER JOIN public.affiliation_elections ON candidats.id = affiliation_elections.id_candidat 
INNER JOIN public.denomination_partis ON affiliation_elections.nom_annee_election = denomination_partis.nom_annee_election
INNER JOIN public.contexte_elections ON affiliation_elections.annee_election = contexte_elections.annee
""", conn)

tweets_aggr_df = pd.read_sql_query(
"""SELECT
candidats.id AS id_candidat,
prenom,
nom,
id_twitter,
annee_election,

--aggr on tweets : count
      (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS nb_tweets_elec_period,
        
      (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 21) AND (date_premier_tour) AND author_id = id_twitter)
        AS nb_tweets_reserve_period,

      (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour  - 7) AND date_premier_tour AND author_id = id_twitter)
        AS nb_tweets_last_week,

      (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation1 || '%') AND author_id = id_twitter)       
        AS nb_tweets_1st_concern,

      (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation2 || '%') AND author_id = id_twitter)       
        AS nb_tweets_2nd_concern,
      
      (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation3 || '%') AND author_id = id_twitter)       
        AS nb_tweets_3rd_concern,
        
--aggr on likes : sum, max, avg
    -- on elec period:
      (SELECT SUM(tweets.like_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS sum_likes_elec_period,
        
      (SELECT ROUND(MAX(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS max_likes_elec_period,
      
      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS avg_likes_elec_period,

    -- on reserve period:
      (SELECT SUM(tweets.like_counts)
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 21) AND date_premier_tour AND author_id = id_twitter)
        AS sum_likes_reserve_period,  

      (SELECT ROUND(MAX(tweets.like_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 21) AND date_premier_tour AND author_id = id_twitter)
        AS max_likes_reserve_period,   

      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 21) AND date_premier_tour AND author_id = id_twitter)
        AS avg_likes_reserve_period,

    -- on the last week:
      (SELECT SUM(tweets.like_counts)
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 7) AND date_premier_tour AND author_id = id_twitter)
        AS sum_likes_last_week,  

      (SELECT ROUND(MAX(tweets.like_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 7) AND date_premier_tour AND author_id = id_twitter)
        AS max_likes_last_week,

      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 7) AND date_premier_tour AND author_id = id_twitter)
        AS avg_likes_last_week,

    -- on 1st concern:
      (SELECT SUM(tweets.like_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation1 || '%') AND author_id = id_twitter)       
        AS sum_likes_1st_concern,

      (SELECT ROUND(MAX(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation1 || '%') AND author_id = id_twitter)       
        AS max_likes_1st_concern,

      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation1 || '%') AND author_id = id_twitter)       
        AS avg_likes_1st_concern,

    -- on 2nd concern:
      (SELECT SUM(tweets.like_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation2 || '%') AND author_id = id_twitter)       
        AS sum_likes_2nd_concern,

      (SELECT ROUND(MAX(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation2 || '%') AND author_id = id_twitter)       
        AS max_likes_2nd_concern,

      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation2 || '%') AND author_id = id_twitter)       
        AS avg_likes_2nd_concern,

    -- on 3rd concern:
      (SELECT SUM(tweets.like_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation3 || '%') AND author_id = id_twitter)       
        AS sum_likes_3rd_concern,

      (SELECT ROUND(MAX(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation3 || '%') AND author_id = id_twitter)       
        AS max_likes_3rd_concern,

      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation3 || '%') AND author_id = id_twitter)       
        AS avg_likes_3rd_concern,

--aggr on retweets : sum, max, avg
  -- on elec period:
      (SELECT SUM(tweets.retweet_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS sum_retweets_elec_period,

      (SELECT ROUND(MAX(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS max_retweets_elec_period,
        
      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS avg_retweets_elec_period,

  -- on reserve period:  
      (SELECT SUM(tweets.retweet_counts)
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 21) AND date_premier_tour AND author_id = id_twitter)
        AS sum_retweets_reserve_period,

      (SELECT ROUND(MAX(tweets.retweet_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 21) AND date_premier_tour AND author_id = id_twitter)
        AS max_retweets_reserve_period,

      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 21) AND date_premier_tour AND author_id = id_twitter)
        AS avg_retweets_reserve_period,

  -- on the last week: 
      (SELECT SUM(tweets.retweet_counts)
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 7) AND date_premier_tour AND author_id = id_twitter)
        AS sum_retweets_last_week,

      (SELECT ROUND(MAX(tweets.retweet_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 7) AND date_premier_tour AND author_id = id_twitter)
        AS max_retweets_last_week,
        
      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date_premier_tour - 7) AND date_premier_tour AND author_id = id_twitter)
        AS avg_retweets_last_week,

  -- on 1st concern:
      (SELECT SUM(tweets.retweet_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation1 || '%') AND author_id = id_twitter)       
        AS sum_retweets_1st_concern,

      (SELECT ROUND(MAX(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation1 || '%') AND author_id = id_twitter)       
        AS max_retweets_1st_concern,

      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation1 || '%') AND author_id = id_twitter)       
        AS avg_retweets_1st_concern,

  -- on 2nd concern:

      (SELECT SUM(tweets.retweet_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation2 || '%') AND author_id = id_twitter)       
        AS sum_retweets_2nd_concern,

      (SELECT ROUND(MAX(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation2 || '%') AND author_id = id_twitter)       
        AS max_retweets_2nd_concern,

      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation2 || '%') AND author_id = id_twitter)       
        AS avg_retweets_2nd_concern,

  -- on 3rd concern:
      (SELECT SUM(tweets.retweet_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation3 || '%') AND author_id = id_twitter)       
        AS sum_retweets_3rd_concern,

      (SELECT ROUND(MAX(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation3 || '%') AND author_id = id_twitter)       
        AS max_retweets_3rd_concern,

      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND UPPER(tweets.content) LIKE UPPER('%' || preoccupation3 || '%') AND author_id = id_twitter)       
        AS avg_retweets_3rd_concern

        
FROM public.candidats
LEFT JOIN public.tweets ON candidats.id_twitter = tweets.author_id
LEFT JOIN public.tweets_tags ON tweets.tweet_id = tweets_tags.tweet_id
LEFT JOIN public.tags ON  tweets_tags.tag_id = tags.tag_id
INNER JOIN public.affiliation_elections ON candidats.id = affiliation_elections.id_candidat 
INNER JOIN public.denomination_partis ON affiliation_elections.nom_annee_election = denomination_partis.nom_annee_election
INNER JOIN public.contexte_elections ON affiliation_elections.annee_election = contexte_elections.annee

GROUP BY candidats.id, prenom, nom, id_twitter, annee_election, date_premier_tour, preoccupation1, preoccupation2, preoccupation3
""", conn)

# Closing of of cursor and connection
conn.close()

# Create the merges dataset from the two previous queries
df = pd.merge(candidates_df,tweets_aggr_df, on=['id_candidat','annee_election'], how='left', suffixes=('', '_y'))
df.drop(list(df.filter(regex = "_y")), axis=1, inplace=True)

df.to_csv('data/dataframe_to_use.csv', sep=";", index=False)