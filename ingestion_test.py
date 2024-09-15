# ingestion_test.py
import pandas as pd
from sqlalchemy import create_engine
import time
from sqlalchemy import inspect

# Database connection
engine = create_engine('postgresql://user:password@localhost:5432/demo_db')
inspector = inspect(engine)

# Function to check if table exists
def table_exists(table_name):
    return inspector.has_table(table_name)

# Function to perform ingestion
def db_ingestion():
    if table_exists('census_data'):
        print("Data already ingested into the database.")
        return 0  # No time taken since data is already there
    else:
        start_time = time.time()
        # Load data from CSV, excluding 'caseid'
        df = pd.read_csv('USCensus1990.data.txt', header=0, usecols=lambda column: column != 'caseid')
        # Ingest data into PostgreSQL
        df.to_sql('census_data', engine, if_exists='replace', index=False)
        end_time = time.time()
        duration = end_time - start_time
        print(f"Database ingestion time: {duration:.2f} seconds")
        return duration

# Function to measure time and perform flat file ingestion
def flat_file_ingestion():
    start_time = time.time()
    # Load data from CSV, excluding 'caseid'
    df = pd.read_csv('USCensus1990.data.txt', header=0, usecols=lambda column: column != 'caseid')
    # Save data to a new CSV (simulating ingestion)
    df.to_csv('census_data_copy.csv', index=False)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Flat file ingestion time: {duration:.2f} seconds")
    return duration

# Run ingestion tests
print("Running Ingestion Test...\n")

flat_time = flat_file_ingestion()
db_time = db_ingestion()

# Calculate percentage difference
if db_time == 0:
    print("\nData already exists in the database. Skipping database ingestion.")
else:
    percentage_faster = ((flat_time - db_time) / flat_time) * 100
    if percentage_faster > 0:
        print(f"\nDatabase ingestion is {percentage_faster:.2f}% faster than flat file ingestion.")
    else:
        print(f"\nFlat file ingestion is {abs(percentage_faster):.2f}% faster than database ingestion.")
