from kafka import KafkaProducer
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from kafka import KafkaConsumer
import time

consumer = KafkaConsumer('payments-topic',bootstrap_servers=['localhost:9092'],max_poll_records= 5,value_deserializer=lambda m: json.loads(m.decode('utf8')),
                         group_id = 'test1',auto_offset_reset='earliest', enable_auto_commit=True)

cloud_config= {
    'secure_connect_bundle': 'secure-connect-transactions-db.zip'
}
with open("transactions_db-token.json") as f:
    secrets = json.load(f)
CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]
auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

producer = KafkaProducer(
   bootstrap_servers=['localhost:9092'],
   value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
def on_success(metadata):
   print(f"Message delivered to {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
def on_error(exception):
   print(f"Message delivery failed: {exception}")

def check(ob:dict):
    query=session.prepare('select order_id from transactions_info.orders_payments_facts where order_id = ?;')
    result=session.execute(query,[ob['order_id']])
    if not result._current_rows:
        print('moving to dlq')
        move_to_dlq(ob)
    else:
        print('moving to updation')
        update_record(ob)
        

def move_to_dlq(ob):
    try:
        future=producer.send('dlq-topic',value=ob)
        future.add_callback(on_success)
        future.add_errback(on_error)
    except Exception:
        future.add_errback(on_error)
def update_record(ob):
    order_id=ob['order_id']
    query=session.prepare('''update transactions_info.orders_payments_facts set payment_id=?,
    payment_method=?,card_last_four=?,payment_status=?,payment_datetime=? where order_id=?;''')
    session.execute(query,[ob['payment_id'],ob['payment_method'],ob['card_last_four'],ob['payment_status'],ob['payment_datetime'],ob['order_id']])
    print(f'row with {order_id} is updated')
try:
    while True:
        for message in consumer:
            print(message.value)
            check(message.value)
except Exception as e:
    print(f'{e}')
finally:
    consumer.close()
    cluster.shutdown()
    producer.flush()
    producer.close()