import csv
import string
import pandas as pd
import numpy as np
from pathlib import Path
from pandas import json_normalize
import psycopg2
from psycopg2.extensions import register_adapter
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# Create a connection to the database
conn = psycopg2.connect(dbname="twitter_elections", user="admin", host="database", password="password")

# Create a cursor to perform database operations
cur = conn.cursor()

# Delete all the tables from the database if there are any
cur.execute("""
    DROP SCHEMA public CASCADE;
    CREATE SCHEMA public;
    GRANT ALL ON SCHEMA public TO admin;
    GRANT ALL ON SCHEMA public TO public;
""")

# Create the tables from the SQL script
with open("src/db_creation.sql", "r") as f:
    cur.execute(f.read())

print("Tables successfully created")

# Reset the SERIAL primary key to make it start at 1
cur.execute("ALTER SEQUENCE candidats_id_seq RESTART")

# Insert data from a csv file
with open("data/candidats.csv", newline="") as f:
    reader = csv.reader(f, delimiter=";")
    headers = next(reader, None)
    for row in reader:
        row = [item if item!='' else None for item in row ]
        cur.execute("""
            INSERT INTO candidats (prenom, nom, sexe, date_naissance, id_twitter, username, compte_verifie, date_creation_compte) 
            VALUES (%s, %s, %s, TO_DATE(%s, 'DD/MM/YYYY'), %s, %s, %s, TO_DATE(%s, 'DD/MM/YYYY'))
            """, row)
        
conn.commit()

with open("data/denomination_partis.csv", newline="") as f:
    reader = csv.reader(f, delimiter=";")
    headers = next(reader, None)
    for row in reader:
        row = [item if item!='' else None for item in row ]
        cur.execute("""
            INSERT INTO denomination_partis
                (nom_annee_election, nom_derniere_election) 
            VALUES (%s, %s)
            """, row)
        
conn.commit()

# Insert data from a csv file
with open("data/contexte_elections.csv", newline="") as f:
    reader = csv.reader(f, delimiter=";")
    headers = next(reader, None)
    for row in reader:
        row = [item if item!='' else None for item in row ]
        cur.execute("""
            INSERT INTO contexte_elections (annee, date_premier_tour, parti_vainqueur_presidentielles, preoccupation1, preoccupation2, preoccupation3) 
            VALUES (%s, TO_DATE(%s, 'DD/MM/YYYY'), %s, %s, %s, %s)
            """, row)
        
conn.commit()

# Reset the SERIAL primary key to make it start at 1
cur.execute("ALTER SEQUENCE affiliation_elections_id_seq RESTART")

# Insert data from a csv file
with open("data/affiliation_elections.csv", newline="") as f:
    reader = csv.reader(f, delimiter=";")
    headers = next(reader, None)
    for row in reader:
        row = [item if item!='' else None for item in row ]
        cur.execute("""
            INSERT INTO affiliation_elections (
                id_candidat, 
                annee_election,
                nom_annee_election,
                code_departement,
                code_circonscription,
                sortant,
                dissident,
                resultat_election) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, row)

# Create field if null value
def get_attribute(data, attribute, default_value):
    if data.get(attribute) is None:
        data[attribute] = default_value
    return data

# Json do df with path data
def json_to_df(json_file_path):
    
    data = pd.read_json(json_file_path)
    if not (data.empty):
        public_metric =json_normalize(data['public_metrics'])
        data = pd.merge(data, public_metric, left_index=True, right_index=True)
        data.drop(['public_metrics','conversation_id','context_annotations','geo','attachments','lang','source',
                       'withheld','in_reply_to_user_id'], inplace=True, axis=1)
        data.rename(columns={"id": "tweet_id", "created_at": "publication_date", "text":"content","like_count":"like_counts",
                             "reply_count":"reply_counts","retweet_count":"retweet_counts","quote_count":"quote_counts"}, inplace = True)
        
        list_label = ['tweet_id', 'author_id', 'publication_date', 'like_counts', 'reply_counts', 'retweet_counts',
                      'quote_counts','reply_settings', 'possibly_sensitive', 'label', 'polarity', 'content', 'entities']
        for i in list_label:
            get_attribute(data, i, None)

        data = data[list_label]
        data['publication_date'] = data['publication_date'].dt.strftime('%Y-%m-%d')
        data = data.convert_dtypes()
        data['possibly_sensitive'] = data['possibly_sensitive'].astype(str)

    return data

# Insert data from json to table tweets
for file in Path("data/").glob("*.json"):
    data_json = open(file,encoding="utf8")

    tweet_data = json_to_df(data_json)
    for i in range(0 ,len(tweet_data)):
        values = (tweet_data['tweet_id'][i],tweet_data['author_id'][i],tweet_data['publication_date'][i],tweet_data['like_counts'][i],
        tweet_data['reply_counts'][i],tweet_data['retweet_counts'][i],tweet_data['quote_counts'][i],tweet_data['reply_settings'][i],
        tweet_data['possibly_sensitive'][i],tweet_data['label'][i],tweet_data['polarity'][i],tweet_data['content'][i])
        cur.execute("""INSERT INTO tweets (
                 tweet_id,
                 author_id, 
                 publication_date,
                 like_counts,
                 reply_counts,
                 retweet_counts,
                 quote_counts,
                 reply_settings,
                 possibly_sensitive,
                 label,
                 polarity,
                 content
                 ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", values)
				 
        entities = tweet_data['entities'][i]
        if entities is not None:
            get_attribute(entities, 'hashtags', [])

            for j in range(len(entities['hashtags'])):
                if entities.get('hashtags')[j] is not None:
                    element = entities.get('hashtags')[j].get('tag').upper()
                    cur.execute(f"INSERT INTO tags (tag) VALUES ('{element}') ON CONFLICT DO NOTHING")	 
		
        conn.commit()

print('Data successfully inserted')

# Closing of of cursor and connection
cur.close()
conn.close()
