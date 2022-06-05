import csv
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

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


# Closing of of cursor and connection
cur.close()
conn.close()