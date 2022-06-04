import json 
import os
import csv
import string

import pandas as pd
import numpy as np
from pandas import json_normalize

import psycopg2
import psycopg2.extras as extras
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

def remove_punctuations(text):
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text

def json_to_df(json_file_path):
    data = pd.read_json('PA721498__@StanGuerini__Guerini__Stanislas.json')
    public_metric =json_normalize(data['public_metrics'])
    data = pd.merge(data, public_metric, left_index=True, right_index=True)
    data.drop(['public_metrics','conversation_id','context_annotations','geo','attachments',
    'lang','source','withheld','in_reply_to_user_id'], inplace=True, axis=1)
    data.rename(columns={"id": "tweet_id", "created_at": "publication_date", 
    "text":"content","like_count":"like_counts", "reply_count":"reply_counts",
    "retweet_count":"retweet_counts","quote_count":"quote_counts"}, inplace = True)
    data = data[['tweet_id', 'author_id', 'publication_date', 'like_counts', 'reply_counts','retweet_counts',
    'quote_counts','reply_settings','possibly_sensitive','label','polarity','content']]
    data['publication_date'] = data['publication_date'].dt.strftime('%Y-%m-%d')
    data['content'] = data['content'].apply(remove_punctuations)
    data = data.convert_dtypes()
    data['possibly_sensitive'] = data['possibly_sensitive'].astype(str)
    return data

def import_data_into_table_tweets(json_file_path, password, user):
    conn = psycopg2.connect(dbname="Twitter-Elections", password = password,user=user, host="localhost", port="5432")
    cursor = conn.cursor()
    tweet_data = json_to_df(json_file_path)
    for i in range(0 ,len(tweet_data)):
        values = (tweet_data['tweet_id'][i],tweet_data['author_id'][i],tweet_data['publication_date'][i],tweet_data['like_counts'][i],
        tweet_data['reply_counts'][i],tweet_data['retweet_counts'][i],tweet_data['quote_counts'][i],tweet_data['reply_settings'][i],
        tweet_data['possibly_sensitive'][i],tweet_data['label'][i],tweet_data['polarity'][i],tweet_data['content'][i])
        cursor.execute("""INSERT INTO tweets (
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
    conn.close()
    print('Data have been exported to PostGreSql Database')

    