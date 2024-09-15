import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import time

# Database connection
engine = create_engine('postgresql://user:password@localhost:5432/demo_db')

# Function to perform flat file update
def flat_file_update():
    # Load data
    df = pd.read_csv('USCensus1990.data.txt', header=0, usecols=lambda column: column != 'caseid')
    start_time = time.time()
    # Update operation
    df.loc[(df['iLooking'] == 0) & (df['dAge'] > 3), 'iLooking'] = 1
    # Save back to CSV
    df.to_csv('census_data_updated.csv', index=False)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Flat file update time: {duration:.2f} seconds")
    return duration

# Function to perform database update
def db_update():
    connection = psycopg2.connect(
        host='localhost',
        database='demo_db',
        user='user',
        password='password'
    )
    cursor = connection.cursor()
    start_time = time.time()
    try:
        # Begin transaction
        cursor.execute("BEGIN;")
        # Update operation
        cursor.execute("""
        UPDATE census_data
        SET "iLooking" = 1
        WHERE "iLooking" = 0 AND "dAge" > 3;
        """)
        # Commit transaction
        connection.commit()
    except Exception as e:
        # Rollback in case of error
        connection.rollback()
        print("An error occurred:", e)
    finally:
        end_time = time.time()
        cursor.close()
        connection.close()
    duration = end_time - start_time
    print(f"Database update time: {duration:.2f} seconds")
    return duration

# Run updating tests
print("Running Updating Test...\n")

flat_time = flat_file_update()
db_time = db_update()

# Calculate percentage difference
percentage_faster = ((flat_time - db_time) / flat_time) * 100
if percentage_faster > 0:
    print(f"\nDatabase update is {percentage_faster:.2f}% faster than flat file update.")
else:
    print(f"\nFlat file update is {abs(percentage_faster):.2f}% faster than database update.")
