'''
This script parses the XML files comprising the Restaurant Reviews
dataset.

The parsed review data is placed into two SQLite tables:
1. "restaurantreviews_reviews": contains review text, rating, pro tags, con tags, and restaurant id
2. "restaurantreviews_restaurants": contains restaurant id and restaurant name

This script will overwrite existing tables in the database.
No relationships are established between the tables.

Citation for the Restaurant Reviews data:
Beyond the stars: improving rating predictions using review text content.
G Ganu, N Elhadad, A Marian. Proc. WebDB. 1-6. 2009.
'''

from glob import glob
from tokenize import String
import xml.etree.ElementTree as ET
import json

from multiprocessing import Pool
import pandas as pd
import sqlite3

# File Parsing
def parse_all_xml_files(datapath, n_procs, chunksize):
    '''
    Parses all of the raw XML files in the Restaurant Reviews dataset
    into XML data objects.
    This function uses multiprocessing to efficiently
    read the XML files into memory.
    The XML data is returned as a list of Elements.
    '''
    xml_files = glob(datapath + '*.xml')
    with Pool(n_procs) as p:
        xml_data = p.map(_parse_xml_file, xml_files, chunksize)
    return xml_data

def _parse_xml_file(filename):
    '''
    Returns the iterable of reviews contained 
    in a single XML file.
    Specifically, the restaurant name and unique id 
    (from dataset) are returned along with the 
    reviews in XML format.
    '''
    xml_tree = ET.parse(filename)
    root = xml_tree.getroot()
    restaurant_id = root.attrib['id']
    restaurant_name = root.find('Name').text
    return root.find('Reviews'), restaurant_id, restaurant_name

# Data Extraction
def extract_all_review_data(xml_data):
    '''
    Handles review extraction from all given XML data files.
    Returns a DataFrame with the schema:
    - review: text of the restaurant review
    - rating: provided rating
    - pro_tags: positive tags associated with the review
    - con_tags: negative tags associated with the review
    - id: unique idenfier for the restaurant associated with the review (from the dataset)
    - restaurant_name: name of the restaurant associated with the review (from the dataset)
    '''
    review_data = [_extract_review_data(d) for d in xml_data]

    return pd.concat(review_data, ignore_index=True)

def _extract_review_data(restaurant_data):
    '''
    Extracts data from a single XML data file.
    The data includes one or more reviews for a single
    establishment.
    - reviews: list of strings
    - rating: list of ints
    - list of "pros" (in plain text)
    - list of "cons" (in plain text)
    - restaurant id (from dataset)
    - restaurant name
    '''
    xml_review = restaurant_data[0]
    restaurant_id = restaurant_data[1]
    restaurant_name = restaurant_data[2]

    texts = []
    ratings = []
    pro_tags, con_tags = [], []
    for review_data in list(xml_review):
        text_data = review_data.find('Body').text
        rating = review_data.find('Rating').text
        pro_tag = review_data.find('Pros').text
        con_tag = review_data.find('Cons').text

        texts.append(text_data)
        ratings.append(rating)
        pro_tags.append(pro_tag)
        con_tags.append(con_tag)

    restaurant_ids = [restaurant_id] * len(texts)
    restaurant_names = [restaurant_name] * len(texts)
        
    return pd.DataFrame({
        'review': texts, 
        'rating': ratings, 
        'pro_tags': pro_tags, 
        'con_tags': con_tags,
        'id': restaurant_ids,
        'restaurant_name': restaurant_names
        })

# Database Functions
def save_reviews(df: pd.DataFrame, db_path: String):
    '''
    Saves only the information directly related to 
    individual reviews to a SQLite3 database.
    
    Columns: review (String), rating (Int), pro_tags
    (String, comma-delimited), con_tags (String, 
    comma-delimited), and unique restaurant id (Int).
    '''
    conn = sqlite3.connect(db_path)
    df[['review', 'rating', 'pro_tags', 'con_tags', 'id']].to_sql("restaurantreviews_reviews", conn, if_exists='replace')
    conn.commit()
    conn.close()

def save_restaurants(df: pd.DataFrame, db_path: String):
    '''
    Saves the names and unique ids for each restaurant
    found in the Restaurant Reviews corpus to a SQLite3 database.
    '''
    conn = sqlite3.connect(db_path)
    df[['id', 'restaurant_name']].drop_duplicates(subset="id", ignore_index=True).to_sql("restaurantreviews_restaurants", conn, if_exists='replace')
    conn.commit()
    conn.close()


if __name__ == '__main__':
    with open('./parameters.json') as params_fp:
        params = json.load(params_fp)

    n_processes = params['batch_processing']['num_processes']
    chunksize = params['batch_processing']['chunksize']
    db_path = params['database_path']
    dataset_path = params['restaurantreviews_path']

    dataset_xml = parse_all_xml_files(dataset_path, n_processes, chunksize)
    dataset_df = extract_all_review_data(dataset_xml)

    save_reviews(dataset_df, db_path)
    save_restaurants(dataset_df, db_path)

    print(len(dataset_df))