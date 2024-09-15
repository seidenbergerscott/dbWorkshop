import pandas as pd
from pymongo import MongoClient

def load_census_data():
    print("Loading Census data from flat file...")
    df = pd.read_csv('USCensus1990.data.txt', header=0)
    # If 'caseid' column exists, drop it
    if 'caseid' in df.columns:
        df = df.drop(columns=['caseid'])
    return df

def ingest_data_into_mongo(df):
    print("Connecting to MongoDB...")
    client = MongoClient('mongodb://localhost:27017/')
    db = client['demo_db']
    collection = db['census_data']

    print("Inserting data into MongoDB...")
    # Convert DataFrame to dictionary records
    data_dict = df.to_dict('records')
    # Insert data into MongoDB
    collection.insert_many(data_dict)
    print("Data ingestion completed successfully.")

def main():
    df = load_census_data()
    ingest_data_into_mongo(df)

if __name__ == '__main__':
    main()
