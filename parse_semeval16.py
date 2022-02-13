'''
This script parses the ABSA task's Restaurants datafile from SemEval16.

Two tables are created in a SQLite3 database. 
1. "semeval16_reviews": contains the review texts from the corpus
2. "semeval16_opinion_data": contains each target word, along with
the related data (sentiment, from character, to character and category).
This table also includes "review_idx", which corresponds to a review
index in the "semeval16_reviews".

This script will overwrite existing tables in the database.
No relationships are established between the tables.

Citation:
Pontiki, Maria, et al. "Semeval-2016 task 5: Aspect based sentiment analysis." International workshop on semantic evaluation. 2016.

More information:  https://alt.qcri.org/semeval2016/task5/
'''

from tokenize import String
import xml.etree.ElementTree as ET
import json

import pandas as pd
from multiprocessing import Pool
import sqlite3

# Corpus Parsing
def parse_xml(filename):
    '''
    Parses SemEval16 ABSA XML data file.
    Returns a list of XML elements.
    '''
    xml_tree = ET.parse(filename)
    root = xml_tree.getroot()
    return list(root)

def extract_all_data(xml_reviews, num_processes, chunksize):
    ''' Parses all SemEval16 reviews using multiprocessing. '''
    with Pool(num_processes) as p:
        res = p.map(_extract_data, xml_reviews, chunksize)
    return pd.concat(res, ignore_index=True)

def _extract_data(xml_review):
    '''
    Given a XML object, the review and all related
    target information is extracted and placed into DataFrames.
    
    Returns pandas DataFrame with columns:
    * `review` (String): raw text of a review
    * `opinion_data` (DataFrame): specific information about the target
    '''
    sents = []
    opinion_attribs = []
    for r in xml_review[0]:
        if len(r) <= 1: continue

        opinion_df = pd.DataFrame([a.attrib for a in r[1]])
        opinion_df.loc[:, 'from'] = opinion_df['from'].astype(int)
        opinion_df.loc[:, 'to'] = opinion_df['to'].astype(int)
        
        sents.append(r[0].text)
        opinion_attribs.append(opinion_df)

    return pd.DataFrame({
        'review': sents, 
        'opinion_data': opinion_attribs
        })

def flatten_dataframe(df: pd.DataFrame):
    '''
    For each target (and associated data), the 
    corresponding review index is saved.

    All of the target DataFrames are flattened into
    a new DataFrame and returned.
    '''
    for i in range(0, df.shape[0]):
        opinion_data = df.loc[i, 'opinion_data']
        opinion_data.loc[:, 'review_idx'] = i
    
    return pd.concat(df['opinion_data'].tolist(), ignore_index=True)

# Database Functions
def save_reviews(df: pd.DataFrame, db_path: String):
    '''
    Saves the review texts to a SQLite3 database.
    '''
    conn = sqlite3.connect(db_path)
    df[['review']].to_sql("semeval16_reviews", conn, if_exists='replace')
    conn.commit()
    conn.close()

def save_opinion_data(df: pd.DataFrame, db_path: String):
    '''
    Saves target and related data to a SQLite3 database.
    The `review_idx` column can be used to associated a row
    in this table to a row in the `semeval16_reviews` table.
    '''
    conn = sqlite3.connect(db_path)
    df.to_sql("semeval16_opinion_data", conn, if_exists='replace')
    conn.commit()
    conn.close()


if __name__ == '__main__':
    with open('./parameters.json') as params_fp:
        params = json.load(params_fp)

    n_processes = params['batch_processing']['num_processes']
    chunksize = params['batch_processing']['chunksize']
    db_path = params['database_path']
    dataset_path = params['semeval16_path']

    xml_data = parse_xml(dataset_path)
    semeval_df = extract_all_data(xml_data, n_processes, chunksize)
    flattened_df = flatten_dataframe(semeval_df)

    save_reviews(semeval_df, db_path)
    save_opinion_data(flattened_df, db_path)
    
    print(semeval_df.shape, flattened_df.shape)