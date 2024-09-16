import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from faker import Faker
import random
import json
import uuid  # Import uuid to correctly cast related products

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
        product_id = str(uuid.uuid4())  # Convert UUID to string
        product = {
            'product_id': product_id,
            'name': fake.word(),
            'price': round(random.uniform(10, 1000), 2),
            'category': random.choice(['Electronics', 'Books', 'Clothing', 'Home', 'Toys']),
            'color': random.choice(['Red', 'Blue', 'Green', 'Black', 'White']),
            'size': random.choice(['S', 'M', 'L', 'XL']),
            'weight': round(random.uniform(0.1, 10.0), 2),
            'specifications': json.dumps({  # Nested specifications as JSON
                'battery_life': f"{random.randint(1, 24)} hours" if random.choice([True, False]) else None,
                'warranty': f"{random.randint(1, 5)} years",
                'manufacturer': fake.company(),
                'features': [fake.word() for _ in range(random.randint(2, 5))],
                'dimensions': {
                    'length': round(random.uniform(5.0, 50.0), 2),
                    'width': round(random.uniform(5.0, 50.0), 2),
                    'height': round(random.uniform(5.0, 50.0), 2)
                },
                'tags': [fake.word() for _ in range(random.randint(5, 15))]
            }),
            'related_products': [str(uuid.uuid4()) for _ in range(random.randint(1, 5))]  # Convert each UUID to string
        }
        products.append(product)

        num_reviews = random.randint(0, 10)
        for _ in range(num_reviews):
            review = {
                'product_id': product_id,
                'user_name': fake.name(),
                'rating': random.randint(1, 5),
                'comment': fake.sentence(),
                'timestamp': fake.date_time_this_decade().isoformat(),  # Convert datetime to ISO format string
                'likes': random.randint(0, 50),
                'dislikes': random.randint(0, 50),
                'responses': json.dumps([  # Nested responses as JSON
                    {
                        'user': fake.name(),
                        'response': fake.sentence(),
                        'timestamp': fake.date_time_this_year().isoformat()  # Convert datetime to ISO format string
                    } for _ in range(random.randint(0, 3))
                ])
            }
            reviews.append(review)

    products_df = pd.DataFrame(products)
    reviews_df = pd.DataFrame(reviews)
    return products_df, reviews_df

def create_tables(conn):
    with conn.cursor() as cursor:
        # Drop tables if they exist to avoid schema mismatch issues
        cursor.execute("DROP TABLE IF EXISTS reviews;")
        cursor.execute("DROP TABLE IF EXISTS products;")

        # Create products table with nested JSON fields
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id UUID PRIMARY KEY,
                name TEXT,
                price NUMERIC,
                category TEXT,
                color TEXT,
                size TEXT,
                weight NUMERIC,
                specifications JSONB,  -- Store specifications as JSONB
                related_products UUID[]  -- Array of related product IDs
            );
        """)

        # Create reviews table with nested JSON responses
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                review_id SERIAL PRIMARY KEY,
                product_id UUID REFERENCES products(product_id),
                user_name TEXT,
                rating INTEGER,
                comment TEXT,
                timestamp TIMESTAMP,
                likes INTEGER,
                dislikes INTEGER,
                responses JSONB  -- Store responses as JSONB
            );
        """)

        conn.commit()

def insert_data(conn, products_df, reviews_df):
    with conn.cursor() as cursor:
        # Insert products with explicit casting of related_products to UUID[]
        products_tuples = [(p.product_id, p.name, p.price, p.category, p.color, p.size, p.weight, p.specifications, 
                           p.related_products) for p in products_df.itertuples(index=False)]
        products_query = """
            INSERT INTO products (product_id, name, price, category, color, size, weight, specifications, related_products)
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING;
        """
        # Explicitly cast related_products to UUID[]
        execute_values(cursor, products_query, products_tuples, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s::uuid[])")

        # Insert reviews
        reviews_tuples = list(reviews_df.itertuples(index=False, name=None))
        reviews_query = """
            INSERT INTO reviews (product_id, user_name, rating, comment, timestamp, likes, dislikes, responses)
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
