from kafka import KafkaProducer
import json
import random
#function to produce dummy data
def payment_gen(i):
   return({
      'order_id' : i,
      'payment_id' : random.randint(10000,99999),
      'card_last_four' : str(random.randint(1000,9999)),
      'payment_method' : random.choice(["Credit Card", "Debit Card", "PayPal", "Google Pay", "Apple Pay"]),
      'payment_status': 'Completed',
      'payment_datetime' : f"2024-07-04T{str(i).zfill(2)}:01:30Z"
   })
#callback function 
def on_success(metadata):
   print(f"Message delivered to {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
def on_error(exception):
   print(f"Message delivery failed: {exception}")
producer = KafkaProducer(
   bootstrap_servers=['localhost:9092'],
   value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
try:
   for i in range(100):
       data = payment_gen(i)
       future = producer.send('payments-topic', value=data)
       future.add_callback(on_success)
       future.add_errback(on_error)
   producer.flush()  
except Exception as e:
   print(f"Exception occurred: {e}")
finally:
   producer.close()  