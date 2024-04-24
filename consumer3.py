import json
from kafka import KafkaConsumer
import pandas as pd
import networkx as nx
import pymongo

# MongoDB connection settings
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["Assosiation_Rules"]
products_collection = mongo_db["Rules"]

# Load data from MongoDB
data = list(products_collection.find({}))  # Assuming the products are stored as documents in MongoDB

# Convert the data to a DataFrame
products = pd.DataFrame(data)

# Graph Analysis
G = nx.Graph()
G.add_nodes_from(products['product_id'])

# Load also_viewed information from data.json
for product in data:
    product_id = product["product_id"]
    also_viewed = product.get("also_viewed", [])
    for viewed_product in also_viewed:
        G.add_edge(product_id, viewed_product)

# Combine Recommendations based on user's buying pattern using graph
def hybrid_recommendation(user_purchases):
    recommendations = []
    for product_id in user_purchases:
        # Use graph traversal to find related products
        related_products = nx.neighbors(G, product_id)
        recommendations.extend(related_products)
    return recommendations

# Kafka Consumer Configuration
bootstrap_servers = ['localhost:9092']
topics = 'test-topic' # Modified topics

# Create Kafka consumer
consumer = KafkaConsumer(topics, bootstrap_servers=bootstrap_servers)

# Subscribe to topic
consumer.subscribe([topics])

# Function for consumer to receive and save messages
def consume_and_save(consumer):
    user_purchases = []
    for message in consumer:
        message_value = json.loads(message.value.decode('utf-8'))  # Assuming message is in JSON format
        print('Received:', message_value)
        # Extract user purchases from message and append to user_purchases list
        user_purchases.extend(message_value.get("purchases", []))
    return user_purchases

# Execute consumer and get user purchases
user_purchases = consume_and_save(consumer)

# Get sample recommendations based on user's buying pattern
print("You liked the following products:")
print("---------------------------------------------------------")
for product_id in user_purchases:
    product_title = products[products['product_id'] == product_id]['title'].iloc[0]
    print(f"- {product_title}")
    recommendations = hybrid_recommendation([product_id])
    if recommendations:
        rec_product_title = products[products['product_id'] == recommendations[0]]['title'].iloc[0]
        print(f" Since you liked {product_title}, you might also like {rec_product_title}.")
print("---------------------------------------------------------")

