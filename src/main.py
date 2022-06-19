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

def add_tags_column(data):
    data['tags'] = np.empty((len(data), 0)).tolist()
    for i in range(len(data)):
        if data.loc[i,'entities']:
            if data.loc[i,'entities'].get('hashtags'):
                for tags in data.loc[i,'entities'].get('hashtags'):
                    data.loc[i,'tags'].append(tags.get('tag').upper())
    return data['tags']

def json_to_df(json_file_path):
    data = pd.read_json(json_file_path)
    if not (data.empty):
        public_metric =json_normalize(data['public_metrics'])
        data = pd.merge(data, public_metric, left_index=True, right_index=True)
        data.rename(columns={"id": "tweet_id", "created_at": "publication_date", "text":"content","like_count":"like_counts",
                             "reply_count":"reply_counts","retweet_count":"retweet_counts","quote_count":"quote_counts"}, inplace = True) 
        list_label = ['tweet_id', 'author_id', 'publication_date', 'like_counts', 'reply_counts', 'retweet_counts',
                      'quote_counts','reply_settings', 'possibly_sensitive', 'content', 'entities']
        for i in list_label:
            get_attribute(data, i, None)
        data = data[list_label]
        data['publication_date'] = data['publication_date'].dt.strftime('%Y-%m-%d')
        data = data.convert_dtypes()
        data['possibly_sensitive'] = data['possibly_sensitive'].astype(str)
        data['tags'] = add_tags_column(json_file_path)
    
    return data

# Insert data from json to table tweets
for file in Path("data/").glob("*.json"):
    data_json = open(file,encoding="utf8")

    tweet_data = json_to_df(data_json)
    for i in range(0 ,len(tweet_data)):
        tag_data = tweet_data['tags'][i]
        for tag in tag_data:
            cur.execute(f"INSERT INTO tags (tag) VALUES ('{tag}') ON CONFLICT DO NOTHING""")
            conn.commit()
            cur.execute(f"SELECT tag_id FROM tags WHERE tag='{tag}';")
            index = cur.fetchone()[0]
            cur.execute(f"INSERT INTO tweets_tags (tweet_id,tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING""", (tweet_data['tweet_id'][i], index))
            conn.commit()
        values = (tweet_data['tweet_id'][i],tweet_data['author_id'][i],tweet_data['publication_date'][i],tweet_data['like_counts'][i],
    	tweet_data['reply_counts'][i],tweet_data['retweet_counts'][i],tweet_data['quote_counts'][i],tweet_data['reply_settings'][i],
    	tweet_data['possibly_sensitive'][i],tweet_data['content'][i])
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
            content
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", values)
        conn.commit()

print('Data successfully inserted')

# Closing of of cursor and connection
cur.close()
conn.close()
