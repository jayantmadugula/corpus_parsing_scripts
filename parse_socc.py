'''
This script parses the SOCC Corpus containing articles from
the Globe and Mail.

Note: this script current only parses the raw "gnm_articles.csv" file.

Creates a single table in a SQLite3 database called "socc_reviews".
This script will replace an existing table with the same name.

Citation for SOCC dataset: https://github.com/sfu-discourse-lab/SOCC
'''

import json
import string
from tokenize import String
import pandas as pd
import sqlite3

def extract_socc_data(filepath) -> pd.DataFrame:
    '''
    Parses SOCC's raw "gnm_articles.csv" data file.
    '''
    article_data = pd.read_csv(filepath)
    
    text = article_data['article_text']
    text = text.str.replace('<p>', '')
    text = text.str.replace('</p>', ' ')
    text = text.str.translate(str.maketrans('', '', string.punctuation))

    article_data.loc[:, 'words'] = text

    return article_data

def save_texts(df: pd.DataFrame, db_path: String):
    '''
    Saves the texts to a SQLite3 database.
    '''
    conn = sqlite3.connect(db_path)
    df.to_sql('socc_articles', conn, if_exists='replace')
    conn.commit()
    conn.close()


if __name__ == '__main__':
    with open('./parameters.json') as params_fp:
        params = json.load(params_fp)

    db_path = params['database_path']
    dataset_path = params['socc_path']

    socc_dataset = extract_socc_data(dataset_path)
    save_texts(socc_dataset, db_path)
    print(socc_dataset.shape)