import json
from pymongo import MongoClient
from faker import Faker
import random

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['demo_db']
collection = db['products']

# Drop the existing collection to avoid duplicate data
collection.drop()

# Initialize Faker for generating fake data
fake = Faker()
data = []

# Generate synthetic E-commerce data
for _ in range(10000):  # Adjust the number for desired data volume
    product = {
        'product_id': fake.unique.uuid4(),
        'name': fake.word(),
        'price': round(random.uniform(10, 1000), 2),
        'category': random.choice(['Electronics', 'Books', 'Clothing', 'Home', 'Toys']),
        'attributes': {
            'color': random.choice(['Red', 'Blue', 'Green', 'Black', 'White']),
            'size': random.choice(['S', 'M', 'L', 'XL']),
            'weight': round(random.uniform(0.1, 10.0), 2),
            'specifications': {  # Add deeply nested specifications
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
            }
        },
        'reviews': [
            {
                'user': fake.name(),
                'rating': random.randint(1, 5),
                'comment': fake.sentence(),
                'timestamp': fake.date_time_this_decade(),
                'likes': random.randint(0, 50),
                'dislikes': random.randint(0, 50),
                'responses': [  # Add nested responses within reviews
                    {
                        'user': fake.name(),
                        'response': fake.sentence(),
                        'timestamp': fake.date_time_this_year()
                    } for _ in range(random.randint(0, 3))
                ]
            } for _ in range(random.randint(0, 10))
        ],
        'related_products': [  # Add an array of related product IDs
            fake.unique.uuid4() for _ in range(random.randint(1, 5))
        ]
    }
    data.append(product)

# Insert data into MongoDB
collection.insert_many(data)

print("E-commerce data generated and inserted into MongoDB successfully.")
