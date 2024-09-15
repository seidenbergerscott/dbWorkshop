import json
from pymongo import MongoClient
from faker import Faker
import random

client = MongoClient('mongodb://localhost:27017/')
db = client['demo_db']
collection = db['products']

# Generate synthetic data
fake = Faker()
data = []

for _ in range(10000):  # Adjust the number for desired data volume
    product = {
        'product_id': fake.unique.uuid4(),
        'name': fake.word(),
        'price': round(random.uniform(10, 1000), 2),
        'category': random.choice(['Electronics', 'Books', 'Clothing', 'Home', 'Toys']),
        'attributes': {
            'color': random.choice(['Red', 'Blue', 'Green', 'Black', 'White']),
            'size': random.choice(['S', 'M', 'L', 'XL']),
            'weight': round(random.uniform(0.1, 10.0), 2)
        },
        'reviews': [
            {
                'user': fake.name(),
                'rating': random.randint(1, 5),
                'comment': fake.sentence()
            } for _ in range(random.randint(0, 5))
        ]
    }
    data.append(product)

# Insert data into MongoDB
collection.insert_many(data)
