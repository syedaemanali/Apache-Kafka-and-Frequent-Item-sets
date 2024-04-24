import subprocess
import json
from kafka import KafkaConsumer
import itertools
import pymongo

bootstrap_servers = ['localhost:9092']
topic = 'test-topic'
hdfs_path = 'result.txt'

# MongoDB connection settings
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["Assosiation_Rules"]
mongo_collection = mongo_db["Rules"]

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)
consumer.subscribe([topic])


def generate_subsets(items, k):
    subsets = []
    for i in range(len(items) - k + 1):
        for subset in itertools.combinations(items, k):
            subsets.append(subset)
    return subsets

for message in consumer:
    value = json.loads(message.value.decode('utf-8'))
    
    # Extract item codes from the message
    item_codes = []
    if isinstance(value, list):
        for item in value:
            item_codes.append(item)
    
    # Initialize item_counts dictionary
    item_counts = {}
    
    # Generate subsets and count item occurrences
    for k in range(1, len(item_codes) + 1):
        subsets = generate_subsets(item_codes, k)
        for subset in subsets:
            item_counts[subset] = item_counts.get(subset, 0) + 1
    
    # Generate association rules
    for item, count in item_counts.items():
        items = item
        for i in range(1, len(items)):
            for subset in itertools.combinations(items, i):
                remaining_items = set(items) - set(subset)
                remaining_items_str = ' '.join(sorted(list(remaining_items)))
                count_freq = item_counts.get(' '.join(sorted(subset)), 0)
                
                threshold = 0.3
                
                # Calculate confidence
                confidence = count / count_freq if count_freq > 0 else 0
                
                if confidence > threshold or confidence == threshold:
                    rule = f"{', '.join(sorted(list(subset)))} => {remaining_items_str}\t{count} \t{count_freq}"
                     # Insert into MongoDB
                    mongo_collection.insert_one({"rule": rule})
                    print('Received and inserted into MongoDB:', rule)
