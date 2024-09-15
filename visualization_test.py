import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import time

# Database connection
engine = create_engine('postgresql://user:password@localhost:5432/demo_db')

# Visualization using flat file
def flat_file_visualization():
    # Load data
    df = pd.read_csv('USCensus1990.data.txt', header=0, usecols=lambda column: column != 'caseid')
    start_time = time.time()
    # Aggregate data
    data = df.groupby('dAge')['dIncome1'].mean().reset_index().sort_values('dAge')
    end_time = time.time()
    duration = end_time - start_time
    print(f"Flat file aggregation time: {duration:.2f} seconds")
    # Visualization
    plt.figure(figsize=(12, 6))
    sns.lineplot(x='dAge', y='dIncome1', data=data)
    plt.title('Average Income by Age (Flat File)')
    plt.xlabel('Age')
    plt.ylabel('Average Income')
    plt.show()
    return duration

# Visualization using database
def db_visualization():
    start_time = time.time()
    query = """
    SELECT "dAge", AVG("dIncome1") as avg_income
    FROM census_data
    GROUP BY "dAge"
    ORDER BY "dAge" ASC;
    """
    data = pd.read_sql(query, engine)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Database aggregation time: {duration:.2f} seconds")
    # Visualization
    plt.figure(figsize=(12, 6))
    sns.lineplot(x='dAge', y='avg_income', data=data)
    plt.title('Average Income by Age (Database)')
    plt.xlabel('Age')
    plt.ylabel('Average Income')
    plt.show()
    return duration

# Run visualization tests
print("Running Visualization Test...\n")

flat_time = flat_file_visualization()
db_time = db_visualization()

# Calculate percentage difference
percentage_faster = ((flat_time - db_time) / flat_time) * 100
print(f"\nDatabase aggregation is {percentage_faster:.2f}% faster than flat file aggregation.") if percentage_faster > 0 else print(f"\nFlat file aggregation is {abs(percentage_faster):.2f}% faster than database aggregation.")
