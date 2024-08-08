from kafka import KafkaProducer
import json
import random

# Function to generate dummy order data
def data_gen(i):
    return {
        'order_id': i,
        'customer_id': random.randint(1000, 99999),
        'item': random.choice(["Laptop", "Phone", "Book", "Tablet", "Monitor"]),
        'quantity': random.randint(1, 10),
        'price': round(random.uniform(50, 1000), 2),
        'shipping_address': random.choice(['JP Nagar', 'Jayanagar', 'Indiranagar', 'Whitefield']),
        'order_status': random.choice(["Shipped", "Pending", "Delivered", "Cancelled"]),
        'creation_date': '2024-07-04'
    }

# Callback for successful message delivery
def on_success(metadata):
    print(f"Message delivered to {metadata.topic} partition {metadata.partition} offset {metadata.offset}")

# Callback for message delivery failure
def on_error(exception):
    print(f"Message delivery failed: {exception}")

# Set up Kafka producer with JSON serialization
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Produce 50 messages and send them to the 'orders-topic'
try:
    for i in range(50):
        data = data_gen(i)  
        future = producer.send('orders-topic', value=data)  
        future.add_callback(on_success)  
        future.add_errback(on_error)  
    producer.flush()  
except Exception as e:
    print(f"Exception occurred: {e}")  
finally:
    producer.close()  
