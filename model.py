
import pandas as pd
import psycopg2 #depreciated use instead from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")

dbname="twitter_elections"
user="admin"
password="password"
host = 'localhost'
port = '15432'

# Create a connection to the database
conn = psycopg2.connect(dbname=dbname, user=user, host=host, port=port, password=password)

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
      (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS nb_tweets_elec_period,
        
       (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        date_trunc('year', cast('2022-05-23' as timestamp)) AND ('2022-05-23') AND author_id = id_twitter)
        AS nb_tweets_reserve_period,
 
        (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date '2022-06-12'  - 7) AND ('2022-05-23') AND author_id = id_twitter)
        AS nb_tweets_last_week,

        (SELECT COUNT(DISTINCT tweets.tweet_id)
        FROM  public.tweets
        WHERE UPPER(tweets.content) LIKE UPPER('%immigration%') AND author_id = id_twitter)       
        AS nb_tweets_1st_concern,
        
      (SELECT SUM(tweets.like_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS sum_likes_elec_period,
        
      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS avg_likes_elec_period,
        
      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        date_trunc('year', cast('2022-05-23' as timestamp)) AND ('2022-05-23') AND author_id = id_twitter)
        AS avg_likes_reserve_period,
        
      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date '2022-06-12'  - 7) AND ('2022-05-23') AND author_id = id_twitter)
        AS avg_likes_last_week,

      (SELECT ROUND(AVG(tweets.like_counts))
        FROM  public.tweets
        WHERE UPPER(tweets.content) LIKE UPPER('%immigration%') AND author_id = id_twitter)       
        AS avg_likes_1st_concern,

      (SELECT SUM(tweets.retweet_counts)
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS sum_retweets_elec_period,
        
      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE EXTRACT(YEAR FROM DATE(publication_date)) = annee_election AND author_id = id_twitter)
        AS avg_retweets_elec_period,
        
      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        date_trunc('year', cast('2022-05-23' as timestamp)) AND ('2022-05-23') AND author_id = id_twitter)
        AS avg_retweets_reserve_period,
        
      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE DATE(publication_date) BETWEEN
        (date '2022-06-12'  - 7) AND ('2022-05-23') AND author_id = id_twitter)
        AS avg_retweets_last_week,

      (SELECT ROUND(AVG(tweets.retweet_counts))
        FROM  public.tweets
        WHERE UPPER(tweets.content) LIKE UPPER('%immigration%') AND author_id = id_twitter)       
        AS avg_retweets_1st_concern
         
FROM public.candidats
LEFT JOIN public.tweets ON candidats.id_twitter = tweets.author_id
LEFT JOIN public.tweets_tags ON tweets.tweet_id = tweets_tags.tweet_id
LEFT JOIN public.tags ON  tweets_tags.tag_id = tags.tag_id
INNER JOIN public.affiliation_elections ON candidats.id = affiliation_elections.id_candidat 
INNER JOIN public.denomination_partis ON affiliation_elections.nom_annee_election = denomination_partis.nom_annee_election
INNER JOIN public.contexte_elections ON affiliation_elections.annee_election = contexte_elections.annee

GROUP BY candidats.id, prenom, nom, id_twitter, annee_election
""", conn)

# Closing of of cursor and connection
conn.close()

# Create the merges dataset from the two previous queries
df = pd.merge(candidates_df,tweets_aggr_df,on=['id_candidat','annee_election'],how='left')