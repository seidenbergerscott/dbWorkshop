import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from faker import Faker
import random

# Database connection parameters
DB_HOST = 'localhost'
DB_NAME = 'demo_db'
DB_USER = 'user'
DB_PASS = 'password'
DB_PORT = '5432'

# Function to generate synthetic e-commerce data
def generate_ecommerce_data(num_products=10000):
    fake = Faker()
    products = []
    reviews = []

    for _ in range(num_products):
        product_id = fake.uuid4()
        product = {
            'product_id': product_id,
            'name': fake.word(),
            'price': round(random.uniform(10, 1000), 2),
            'category': random.choice(['Electronics', 'Books', 'Clothing', 'Home', 'Toys']),
            'color': random.choice(['Red', 'Blue', 'Green', 'Black', 'White']),
            'size': random.choice(['S', 'M', 'L', 'XL']),
            'weight': round(random.uniform(0.1, 10.0), 2)
        }
        products.append(product)

        num_reviews = random.randint(0, 5)
        for _ in range(num_reviews):
            review = {
                'product_id': product_id,
                'user_name': fake.name(),
                'rating': random.randint(1, 5),
                'comment': fake.sentence()
            }
            reviews.append(review)

    products_df = pd.DataFrame(products)
    reviews_df = pd.DataFrame(reviews)
    return products_df, reviews_df

def create_tables(conn):
    with conn.cursor() as cursor:
        # Create products table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id UUID PRIMARY KEY,
                name TEXT,
                price NUMERIC,
                category TEXT,
                color TEXT,
                size TEXT,
                weight NUMERIC
            );
        """)

        # Create reviews table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                review_id SERIAL PRIMARY KEY,
                product_id UUID REFERENCES products(product_id),
                user_name TEXT,
                rating INTEGER,
                comment TEXT
            );
        """)

        conn.commit()

def insert_data(conn, products_df, reviews_df):
    with conn.cursor() as cursor:
        # Insert products
        products_tuples = list(products_df.itertuples(index=False, name=None))
        products_query = """
            INSERT INTO products (product_id, name, price, category, color, size, weight)
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING;
        """
        execute_values(cursor, products_query, products_tuples)

        # Insert reviews
        reviews_tuples = list(reviews_df.itertuples(index=False, name=None))
        reviews_query = """
            INSERT INTO reviews (product_id, user_name, rating, comment)
            VALUES %s;
        """
        execute_values(cursor, reviews_query, reviews_tuples)

        conn.commit()

def main():
    print("Generating synthetic e-commerce data...")
    products_df, reviews_df = generate_ecommerce_data()
    print(f"Generated {len(products_df)} products and {len(reviews_df)} reviews.")

    print("Connecting to PostgreSQL database...")
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

    try:
        print("Creating tables...")
        create_tables(conn)

        print("Inserting data into PostgreSQL...")
        insert_data(conn, products_df, reviews_df)

        print("Data ingestion completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
