# indexing_test.py
import pandas as pd
from sqlalchemy import create_engine, text
import time

# Database connection
engine = create_engine('postgresql://user:password@localhost:5432/demo_db')

# Remove ensure_data_in_db() function as we assume data is already ingested

# Function to perform search without index
def db_search_no_index():
    start_time = time.time()
    query = text("""
    SELECT *
    FROM census_data
    WHERE "dAge" = 25;
    """)
    result = pd.read_sql(query, engine)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Database search time without index: {duration:.2f} seconds")
    return duration, result

# Function to create index
def create_index():
    with engine.connect() as conn:
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_dAge ON census_data("dAge");'))
    print("Index on 'dAge' created successfully.")

# Function to perform search with index
def db_search_with_index():
    start_time = time.time()
    query = text("""
    SELECT *
    FROM census_data
    WHERE "dAge" = 25;
    """)
    result = pd.read_sql(query, engine)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Database search time with index: {duration:.2f} seconds")
    return duration, result

# Run indexing tests
print("Running Indexing Test...\n")

# Search without index
no_index_time, no_index_result = db_search_no_index()

# Create index
create_index()

# Search with index
with_index_time, with_index_result = db_search_with_index()

# Calculate percentage difference
percentage_faster = ((no_index_time - with_index_time) / no_index_time) * 100
if percentage_faster > 0:
    print(f"\nSearch with index is {percentage_faster:.2f}% faster than search without index.")
else:
    print(f"\nSearch without index is {abs(percentage_faster):.2f}% faster than search with index.")
