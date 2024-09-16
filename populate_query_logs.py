import time
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
import pymysql
from sqlalchemy.exc import SQLAlchemyError
import traceback

# Database connections

# PostgreSQL connection
pg_engine = create_engine('postgresql://user:password@localhost:5432/demo_db')

# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['demo_db']
mongo_census_collection = mongo_db['census_data']
mongo_products_collection = mongo_db['products']

# MySQL connection (for query logs)
mysql_engine = create_engine(
    'mysql+pymysql://user:password@127.0.0.1:3306/demo_db',
    echo=False,  # Set to False to reduce console output
    isolation_level='AUTOCOMMIT'
)

# Function to log query execution times
def log_query(data_source, query_complexity, dataset, duration):
    print(f"Logging query: data_source={data_source}, query_complexity={query_complexity}, dataset={dataset}, duration={duration}")
    try:
        with mysql_engine.connect() as conn:
            insert_query = text("""
                INSERT INTO query_logs (timestamp, data_source, query_complexity, dataset, duration)
                VALUES (NOW(), :data_source, :query_complexity, :dataset, :duration)
            """)
            result = conn.execute(insert_query, {
                'data_source': data_source,
                'query_complexity': query_complexity,
                'dataset': dataset,
                'duration': duration
            })
            print(f"Insert result: {result.rowcount} rows inserted.")
    except Exception as e:
        print(f"An error occurred while logging the query: {e}")
        print(traceback.format_exc())

# Function to execute queries on PostgreSQL
def execute_postgres_query(dataset, query_complexity, params):
    start_time = time.time()
    try:
        if dataset == "Census Data":
            if query_complexity == "Simple":
                query = text("""
                    SELECT "iSex", COUNT(*) as count
                    FROM census_data
                    WHERE "dAge" BETWEEN :age_min AND :age_max
                    AND "dIncome1" >= :income_threshold
                    AND "iSex" = ANY(:sex_options)
                    GROUP BY "iSex";
                """)
            elif query_complexity == "Moderate":
                query = text("""
                    SELECT "iSex", AVG("dIncome1") as avg_income
                    FROM census_data
                    WHERE "dAge" BETWEEN :age_min AND :age_max
                    AND "dIncome1" >= :income_threshold
                    AND "iSex" = ANY(:sex_options)
                    GROUP BY "iSex";
                """)
            else:  # Complex
                query = text("""
                    SELECT "iSex", "iMarital", AVG("dIncome1") as mean, COUNT(*) as count
                    FROM census_data
                    WHERE "dAge" BETWEEN :age_min AND :age_max
                    AND "dIncome1" >= :income_threshold
                    AND "iSex" = ANY(:sex_options)
                    GROUP BY "iSex", "iMarital";
                """)
        else:  # E-commerce Data
            if query_complexity == "Simple":
                query = text("""
                    SELECT category, AVG(price) as average_price
                    FROM products
                    WHERE price BETWEEN :price_min AND :price_max
                    GROUP BY category;
                """)
            elif query_complexity == "Moderate":
                query = text("""
                    SELECT p.category, AVG(r.rating) as average_rating
                    FROM products p
                    JOIN reviews r ON p.product_id = r.product_id
                    WHERE p.price BETWEEN :price_min AND :price_max
                    AND p.category = ANY(:categories)
                    GROUP BY p.category;
                """)
            else:  # Complex
                query = text("""
                    SELECT
                        p.category,
                        p.color,
                        AVG(r.rating) as average_rating,
                        AVG(p.price) as average_price,
                        COUNT(r.review_id) as total_reviews
                    FROM
                        products p
                    LEFT JOIN
                        reviews r ON p.product_id = r.product_id
                    WHERE
                        p.price BETWEEN :price_min AND :price_max
                        AND p.category = ANY(:categories)
                    GROUP BY
                        p.category,
                        p.color;
                """)
        with pg_engine.connect() as conn:
            conn.execute(query, params)  # Corrected way to pass parameters
        end_time = time.time()
        duration = end_time - start_time
        log_query("PostgreSQL", query_complexity, dataset, duration)
    except Exception as e:
        print(f"An error occurred during PostgreSQL query execution: {e}")
        print(traceback.format_exc())

# Function to execute queries on MongoDB
def execute_mongo_query(dataset, query_complexity, filters):
    start_time = time.time()
    try:
        if dataset == "Census Data":
            collection = mongo_census_collection
            if query_complexity == "Simple":
                pipeline = [
                    {'$match': {
                        'dAge': {'$gte': filters['age_min'], '$lte': filters['age_max']},
                        'dIncome1': {'$gte': filters['income_threshold']},
                        'iSex': {'$in': filters['sex_options']}
                    }},
                    {'$group': {'_id': '$iSex', 'count': {'$sum': 1}}}
                ]
            elif query_complexity == "Moderate":
                pipeline = [
                    {'$match': {
                        'dAge': {'$gte': filters['age_min'], '$lte': filters['age_max']},
                        'dIncome1': {'$gte': filters['income_threshold']},
                        'iSex': {'$in': filters['sex_options']}
                    }},
                    {'$group': {'_id': '$iSex', 'avg_income': {'$avg': '$dIncome1'}}}
                ]
            else:  # Complex
                pipeline = [
                    {'$match': {
                        'dAge': {'$gte': filters['age_min'], '$lte': filters['age_max']},
                        'dIncome1': {'$gte': filters['income_threshold']},
                        'iSex': {'$in': filters['sex_options']}
                    }},
                    {'$group': {
                        '_id': {'iSex': '$iSex', 'iMarital': '$iMarital'},
                        'mean': {'$avg': '$dIncome1'},
                        'count': {'$sum': 1}
                    }}
                ]
        else:  # E-commerce Data
            collection = mongo_products_collection
            if query_complexity == "Simple":
                pipeline = [
                    {'$match': {'price': {'$gte': filters['price_min'], '$lte': filters['price_max']}}},
                    {'$group': {'_id': '$category', 'average_price': {'$avg': '$price'}}}
                ]
            elif query_complexity == "Moderate":
                pipeline = [
                    {'$match': {
                        'price': {'$gte': filters['price_min'], '$lte': filters['price_max']},
                        'category': {'$in': filters['categories']}
                    }},
                    {'$unwind': '$reviews'},
                    {'$group': {'_id': '$category', 'average_rating': {'$avg': '$reviews.rating'}}}
                ]
            else:  # Complex
                pipeline = [
                    {'$match': {
                        'price': {'$gte': filters['price_min'], '$lte': filters['price_max']},
                        'category': {'$in': filters['categories']}
                    }},
                    {'$unwind': '$reviews'},
                    {'$group': {
                        '_id': {
                            'category': '$category',
                            'color': '$attributes.color'
                        },
                        'average_rating': {'$avg': '$reviews.rating'},
                        'average_price': {'$avg': '$price'}
                    }}
                ]
        list(collection.aggregate(pipeline))
        end_time = time.time()
        duration = end_time - start_time
        log_query("MongoDB", query_complexity, dataset, duration)
    except Exception as e:
        print(f"An error occurred during MongoDB query execution: {e}")
        print(traceback.format_exc())

def main():
    datasets = ["Census Data", "E-commerce Data"]
    data_sources = ["PostgreSQL", "MongoDB"]
    query_complexities = ["Simple", "Moderate", "Complex"]
    
    # Define default parameters for queries
    census_params = {
        'age_min': 20,
        'age_max': 60,
        'income_threshold': 30000,
        'sex_options': [0, 1]
    }
    
    ecommerce_params = {
        'price_min': 10.0,
        'price_max': 500.0,
        'categories': ['Electronics', 'Books', 'Clothing']
    }
    
    # Set the number of iterations you want to run
    iterations = 100  # Adjust this number as needed
    
    for i in range(iterations):
        print(f"Iteration {i + 1}/{iterations}")
        for dataset in datasets:
            for data_source in data_sources:
                for query_complexity in query_complexities:
                    print(f"Executing {query_complexity} query on {data_source} for {dataset}...")
                    if dataset == "Census Data":
                        if data_source == "PostgreSQL":
                            params = {
                                'age_min': census_params['age_min'],
                                'age_max': census_params['age_max'],
                                'income_threshold': census_params['income_threshold'],
                                'sex_options': census_params['sex_options']
                            }
                            execute_postgres_query(dataset, query_complexity, params)
                        elif data_source == "MongoDB":
                            filters = census_params
                            execute_mongo_query(dataset, query_complexity, filters)
                    elif dataset == "E-commerce Data":
                        if data_source == "PostgreSQL":
                            if query_complexity == "Simple":
                                params = {
                                    'price_min': ecommerce_params['price_min'],
                                    'price_max': ecommerce_params['price_max']
                                }
                            else:
                                params = {
                                    'price_min': ecommerce_params['price_min'],
                                    'price_max': ecommerce_params['price_max'],
                                    'categories': ecommerce_params['categories']
                                }
                            execute_postgres_query(dataset, query_complexity, params)
                        elif data_source == "MongoDB":
                            filters = ecommerce_params
                            execute_mongo_query(dataset, query_complexity, filters)
                    else:
                        print(f"Invalid dataset: {dataset}")
                    
                    # Optional: Wait between queries to simulate real usage
                    time.sleep(0.1)  # Shorter sleep time for faster execution
    
        # Optional: Wait between iterations
        time.sleep(0.5)
    
    print("Data population completed.")

if __name__ == "__main__":
    main()
