from model import create_df_from_database

df = create_df_from_database(dbname="twitter_elections", user="admin", host="database", password="password")

df.to_csv('data/dataframe_to_use.csv', sep=";", index=False)