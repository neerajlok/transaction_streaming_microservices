# transaction_streaming_microservices
This project implements a data engineering pipeline leveraging a microservices architecture. It comprises four distinct Python-based microservices, each responsible for a specific functionality within the pipeline.

## Table of Contents
- Overview
- Architecture
- Components
 - Producers
 - Consumers
- Setup
- Usage
- Contributing
- License
## Overview
This pipeline generates and processes dummy order and payment data using Kafka and Cassandra. The architecture is designed to be modular, with each component handling a specific task.

## Architecture
The pipeline follows a microservices architecture with four main components:

1. **Order Producer**: Generates dummy order data and pushes it to a Kafka topic.
2. **Payment Producer**: Generates dummy payment data and pushes it to a Kafka topic.
3. **Order Consumer**: Consumes order data from Kafka and inserts it into a Cassandra database.
4. **Payment Consumer**: Consumes payment data from Kafka, updates records in Cassandra, or pushes data to a Dead Letter Queue (DLQ) Kafka topic if the order ID is not found.
# Components
## Producers
1. Order Producer
  - Generates dummy order JSON data with fields:
    - order_id
    - consumer_id
    - item
    - quantity
    - price
    - shipping_address
    - order_status
    - creation_date
 - Pushes data to a Kafka topic.
2. Payment Producer
 -  Generates dummy payment JSON data with fields:
    - order_id
    - payment_id
    - last_4_digits_of_card
    - payment_method
    - payment_status
    - payment_date_time
  - Pushes data to a Kafka topic.
# Consumers
1. Order Consumer
  - Consumes order data from the Kafka topic.
  - Inserts data into a Cassandra database hosted on the cloud.
2. Payment Consumer
  - Consumes payment data from the Kafka topic.
  - Updates records in the Cassandra table if the order_id is found.
  - Pushes data to a DLQ Kafka topic if the order_id is not found.
# Setup
1. Clone the repository:
git clone https://github.com/neerajlok/transaction_streaming_microservices.git

2. Install dependencies:
pip install -r requirements.txt

3. Configure Kafka and Cassandra settings in the configuration files.
# Usage
1. Start the Kafka server.
2. Run the producers:
 - python order_producer.py
 - python payment_producer.py
3. Run the consumers:
 - python order_consumer.py
 - python payment_consumer.py

# Contributing
Contributions are welcome! Please open an issue or submit a pull request.

# License
This project is licensed under the MIT License.
