import json
from kafka import KafkaConsumer
import itertools
import pymongo

bootstrap_servers = ['localhost:9092']
topic = 'test-topic'

# MongoDB connection settings
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["Assosiation_Rules"]
mongo_collection = mongo_db["Rules"]

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)
consumer.subscribe([topic])

# PCY Algorithm Parameters
hash_table_size = 1000
min_support = 0.2  # Adjust according to your requirement

# Initialize hash table
hash_table = [0] * hash_table_size

# Function to hash items
def hash_item(item):
    return hash(item) % hash_table_size

# Function to update hash table
def update_hash_table(basket):
    for item in basket:
        hash_value = hash_item(item)
        hash_table[hash_value] += 1

# Function to check if item passes PCY threshold
def check_pcy(item):
    hash_value = hash_item(item)
    return hash_table[hash_value] >= min_support

# PCY Algorithm
def pcy_algorithm(basket):
    frequent_items = set()
    item_counts = {}
    update_hash_table(basket)
    
    # Count individual items
    for item in basket:
        if check_pcy(item):
            frequent_items.add(item)
            item_counts[item] = item_counts.get(item, 0) + 1
    
    # Generate itemsets of size 2 and count their occurrences
    for size in range(2, len(frequent_items) + 1):
        itemsets = itertools.combinations(frequent_items, size)
        for itemset in itemsets:
            count = min(item_counts[item] for item in itemset)
            if count >= min_support:
                remaining_items = frequent_items - set(itemset)
                confidence = count / item_counts[itemset[0]]
                if confidence >= min_confidence:
                    rule = {
                        "association_rule": f"{', '.join(itemset)} => {', '.join(remaining_items)}",
                        "support_count": count,
                        "total_count": item_counts[itemset[0]],
                        "confidence": confidence
                    }
                    # Insert into MongoDB
                    mongo_collection.insert_one(rule)
                    print('Inserted into MongoDB:', rule)

# Association Rule Parameters
min_confidence = 0.5  # Adjust according to your requirement

while True:
    for msg in consumer:
        try:
            # Process message
            value = json.loads(msg.value.decode('utf-8'))
            basket = value  # Assuming each message is a basket of items
            pcy_algorithm(basket)
        except Exception as e:
            print("Error processing message:", e)

