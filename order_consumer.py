from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from kafka import KafkaConsumer
import time
consumer = KafkaConsumer('orders-topic',bootstrap_servers=['localhost:9092'],max_poll_records= 5,value_deserializer=lambda m: json.loads(m.decode('utf8')),
                         group_id = 'test69',auto_offset_reset='earliest', enable_auto_commit=True)
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
def insert(ob:dict):
    order_id=ob['order_id']
    query=session.prepare('''insert into transactions_info.orders_payments_facts (order_id,customer_id,item,quantity,price,shipping_address,order_status,creation_date) values 
    (?,?,?,?,?,?,?,?);''')
    try:
        session.execute(query,[ob['order_id'],ob['customer_id'],ob['item'],ob['quantity'],ob['price'],ob['shipping_address'],ob['order_status'],ob['creation_date']])
        print(f'Row with id {order_id} has been inserted into the table')
    except Exception as e:
        print(e)
try:
    while True:
        for message in consumer:
            print(message.value)
            insert(message.value)
            time.sleep(3)
except Exception as e:
    print(f'{e}')
finally:
    consumer.close()
    cluster.shutdown()