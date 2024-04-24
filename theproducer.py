from kafka import KafkaProducer
import json

bootstrap_servers = ['localhost:9092']
topic = 'test-topic'

def produce_dictionary(json_file, producer, topic):
    with open(json_file, 'r') as f:
        data = json.load(f)

    for item in data:
        related = item.get("related", {})
        related_dict = {
            "also_bought": related.get("also_bought", []),
            #"also_viewed": related.get("also_viewed", [])
        }

        # Append related items to the ID
        related_items = []
        related_items.append(item.get("asin", ""))
        for key, value in related_dict.items():
            related_items.extend(value)

        # Produce the JSON data to Kafka topic
        producer.send(topic, json.dumps(related_items).encode('utf-8'))
        print('product details:', related_items)

    # Flush the producer to ensure all messages are sent
    producer.flush()

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Example usage
if __name__ == "__main__":
    json_file_path = "data.json"
    produce_dictionary(json_file_path, producer, topic)

