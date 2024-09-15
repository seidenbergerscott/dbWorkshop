import pandas as pd
from sqlalchemy import create_engine, inspect
from pymongo import MongoClient
import time

# PostgreSQL connection
engine = create_engine('postgresql://user:password@localhost:5432/demo_db')
inspector = inspect(engine)

# MongoDB connection
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['demo_db']
mongo_collection = mongo_db['census_data']

# Function to check if table exists in PostgreSQL
def table_exists(table_name):
    return inspector.has_table(table_name)

# Function to check if collection exists in MongoDB
def collection_exists(collection_name):
    return collection_name in mongo_db.list_collection_names()

# Ensure data is in both databases
def ensure_data_in_databases():
    # Check if data exists in PostgreSQL
    if not table_exists('census_data'):
        print("Ingesting data into PostgreSQL...")
        df = pd.read_csv('USCensus1990.data.txt', header=0)
        # Exclude 'caseid'
        df = df.drop(columns=['caseid'])
        df.to_sql('census_data', engine, if_exists='replace', index=False)
    else:
        print("Data already exists in PostgreSQL.")

    # Check if data exists in MongoDB
    if not collection_exists('census_data'):
        print("Ingesting data into MongoDB...")
        df = pd.read_csv('USCensus1990.data.txt', header=0)
        # Exclude 'caseid'
        df = df.drop(columns=['caseid'])
        data_json = df.to_dict(orient='records')
        mongo_collection.insert_many(data_json)
    else:
        print("Data already exists in MongoDB.")

ensure_data_in_databases()

# Function to perform SQL query
def sql_query():
    start_time = time.time()
    query = """
    SELECT "iSex", AVG("dIncome1") as avg_income
    FROM census_data
    WHERE "dAge" > 3
    GROUP BY "iSex";
    """
    result = pd.read_sql(query, engine)
    end_time = time.time()
    duration = end_time - start_time
    print(f"SQL query time: {duration:.2f} seconds")
    return duration, result

# Function to perform MongoDB aggregation
def mongo_query():
    start_time = time.time()
    pipeline = [
        {"$match": {"dAge": {"$gt": 3}}},
        {"$group": {"_id": "$iSex", "avg_income": {"$avg": "$dIncome1"}}}
    ]
    result = list(mongo_collection.aggregate(pipeline))
    end_time = time.time()
    duration = end_time - start_time
    print(f"MongoDB aggregation time: {duration:.2f} seconds")
    return duration, result

# Run NoSQL vs. SQL test
print("Running NoSQL vs. SQL Database Test...\n")

sql_time, sql_result = sql_query()
mongo_time, mongo_result = mongo_query()

# Calculate percentage difference
percentage_faster = ((sql_time - mongo_time) / sql_time) * 100
if percentage_faster > 0:
    print(f"\nMongoDB aggregation is {percentage_faster:.2f}% faster than SQL query.")
else:
    print(f"\nSQL query is {abs(percentage_faster):.2f}% faster than MongoDB aggregation.")

# Display results
print("\nSQL Query Result:")
print(sql_result)
print("\nMongoDB Aggregation Result:")
print(mongo_result)
