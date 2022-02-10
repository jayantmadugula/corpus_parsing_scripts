'''
This script parses the Stanford Sentiment Treebank (partially).
Only "dictionary.txt" and "labels.txt" are parsed. None of the other
parts of the SST dataset are parsed.

This script will create a single table in a SQLite3 database called
"sst_phrases". If another table exists in the database with the same name, running this script will replace the table.

Citation: 
Recursive Deep Models for Semantic Compositionality Over a Sentiment Treebank
Richard Socher, Alex Perelygin, Jean Wu, Jason Chuang, Christopher Manning, Andrew Ng and Christopher Potts
Conference on Empirical Methods in Natural Language Processing (EMNLP 2013)

More information: https://nlp.stanford.edu/sentiment/
'''

import json
import sqlite3
from tokenize import String
import pandas as pd

def process_sst_data(datapath):
    '''
    This function parses and normalizes the data in "dictionary.txt" in 
    the Stanford Sentiment Treebank dataset.
    '''
    sentiment_labels_filename = 'sentiment_labels.txt'
    phrase_dictionary_filename = 'dictionary.txt'

    # Read text data
    phrase_data = pd.read_csv(datapath+phrase_dictionary_filename, sep='|', names=['phrase', 'phrase_id'], index_col='phrase_id')
    phrase_data.loc[:, 'phrase'] = phrase_data['phrase'].str.replace(r'[^\w\s]+', '', regex=True).str.lower()
    phrase_data.loc[:, 'phrase'] = phrase_data['phrase'].str.split().str.join(' ')
    phrase_data = phrase_data[phrase_data['phrase'].str.len() > 0]

    # Read label data
    sentiment_labels = pd.read_csv(datapath+sentiment_labels_filename, sep='|', index_col='phrase ids')

    # Join label and text data into one DataFrame, then return
    sst_data = phrase_data.join(sentiment_labels, how='inner')
    sst_data.columns = sst_data.columns.str.replace(' ', '_')

    return sst_data

def save_phrases(df: pd.DataFrame, db_path: String):
    '''
    Saves the texts to a SQLite3 database.
    '''
    conn = sqlite3.connect(db_path)
    df.to_sql('sst_phrases', conn, if_exists='replace')
    conn.commit()
    conn.close()


if __name__ == '__main__':
    with open('./parameters.json') as params_fp:
        params = json.load(params_fp)

    db_path = params['database_path']
    dataset_path = params['sst_path']

    sst_df = process_sst_data(dataset_path)
    save_phrases(sst_df, db_path)
    
    print(sst_df.shape)