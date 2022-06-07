import csv
import string
import pandas as pd
import numpy as np
from pathlib import Path
from pandas import json_normalize
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extensions import register_adapter
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# Create a connection to the database
conn = psycopg2.connect(dbname="twitter_elections", user="admin", host="database", password="password")
engine = create_engine("postgresql://admin:password@database:5432/twitter_elections")

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
with open("db_creation.sql", "r") as f:
    cur.execute(f.read())

# Get all the tables names
cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE';
   """)

for table in cur.fetchall():
    print(table)

# candidat
# Delete all entries from candidats if there are already any
cur.execute("DELETE FROM candidats")

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

candidats = pd.read_sql_query("""
        SELECT 
            id,
            prenom,
            nom,
            sexe,
            date_naissance,
            CAST(id_twitter AS VARCHAR),
            username,
            compte_verifie,
            date_creation_compte
        FROM candidats
        """,con=engine, coerce_float=False)

#denomination partis
cur.execute("DELETE FROM denomination_partis")

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

denomination_partis = pd.read_sql_query("""
        SELECT *
        FROM denomination_partis
        """,con=engine, coerce_float=False)

# Delete all entries from contexte_elections if there are already any
cur.execute("DELETE FROM contexte_elections")

# Insert data from a csv file
with open("data/contexte_elections.csv", newline="") as f:
    reader = csv.reader(f, delimiter=";")
    headers = next(reader, None)
    for row in reader:
        row = [item if item!='' else None for item in row ]
        cur.execute("""
            INSERT INTO contexte_elections (annee, parti_vainqueur_presidentielles) 
            VALUES (%s, %s)
            """, row)
        
conn.commit()

contexte_elections = pd.read_sql_query("""
        SELECT *
        FROM contexte_elections
        """,con=engine, coerce_float=False)

# Delete all entries from affiliation_elections if there are already any
cur.execute("DELETE FROM affiliation_elections")

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

contexte_elections = pd.read_sql_query("""
        SELECT *
        FROM affiliation_elections
        """,con=engine, coerce_float=False)


# Create field if null value
def get_attribute(data, attribute, default_value):
    if data.get(attribute) is None:
        data[attribute] = default_value
    return data

# Remove punctuations
def remove_punctuations(text):
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text

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
                      'quote_counts','reply_settings', 'possibly_sensitive', 'label', 'polarity', 'content']
        for i in list_label:
            get_attribute(data, i, None)

        data = data[list_label]
        data['publication_date'] = data['publication_date'].dt.strftime('%Y-%m-%d')
        data['content'] = data['content'].apply(remove_punctuations)
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
        conn.commit()

print('Data have been exported to PostGreSql Database')

# Closing of of cursor and connection
cur.close()
conn.close()