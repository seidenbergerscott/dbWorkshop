import pandas as pd
from sqlalchemy import create_engine
import time

# Database connection
engine = create_engine('postgresql://user:password@localhost:5432/demo_db')

# Function to perform and time flat file query
def flat_file_query():
    # Load data
    df = pd.read_csv('USCensus1990.data.txt', header=0, usecols=lambda column: column != 'caseid')
    start_time = time.time()
    # Perform query: Average 'dIncome1' by 'iSex' where 'dAge' > 30
    result = df[df['dAge'] > 3].groupby('iSex')['dIncome1'].mean()
    end_time = time.time()
    duration = end_time - start_time
    print(f"Flat file query time: {duration:.2f} seconds")
    return duration, result

# Function to perform and time database query
def db_query():
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
    print(f"Database query time: {duration:.2f} seconds")
    return duration, result

# Run querying tests
print("Running Querying Test...\n")

flat_time, flat_result = flat_file_query()
db_time, db_result = db_query()

# Calculate percentage difference
percentage_faster = ((flat_time - db_time) / flat_time) * 100
if percentage_faster > 0:
    print(f"\nDatabase query is {percentage_faster:.2f}% faster than flat file query.")
else:
    print(f"\nFlat file query is {abs(percentage_faster):.2f}% faster than database query.")

# Display results
print("\nFlat File Query Result:")
print(flat_result)
print("\nDatabase Query Result:")
print(db_result)
