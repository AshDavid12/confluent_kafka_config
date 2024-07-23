from aiokafka import AIOKafkaConsumer
import asyncio
from aiokafka.helpers import create_ssl_context

### works for outputting the messy msg 
SASL_USERNAME= "bla"
SASL_PASSWORD= "bla"
KAFKA_BROKER_2= "bla"
SCHEMA_REGISTRY= "bla"
SCHEMA_REGISTRY_USER_INFO= "bla"
# Kafka configuration variables

# Function to get Kafka configuration
def get_kafka_config() -> dict:
    config = {
        "bootstrap.servers": KAFKA_BROKER_2,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD
    }
    return config

# Function to set up the Kafka consumer
async def setup_consumer():
    kafka_config = get_kafka_config()
    consumer = AIOKafkaConsumer(
        'ai_factcheck_summaries',
        bootstrap_servers=kafka_config['bootstrap.servers'],
        security_protocol=kafka_config['security.protocol'],
        sasl_mechanism=kafka_config['sasl.mechanisms'],
        sasl_plain_username=kafka_config['sasl.username'],
        sasl_plain_password=kafka_config['sasl.password'],
        group_id= 'gid', ## needs to change everytime kinda(?)
        auto_offset_reset='earliest',
        ssl_context =create_ssl_context(),
        session_timeout_ms=60000,  # 60 seconds
        heartbeat_interval_ms=20000,  # 20 seconds
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,  # 5 seconds
        max_poll_interval_ms=300000,  # 5 minutes
        fetch_max_wait_ms=500,  # 0.5 seconds
        fetch_min_bytes=1,
        max_poll_records=100  # Adjust based on your processing capability
    )
    await consumer.start()
    return consumer

# Function to consume messages from Kafka
async def consume_messages(consumer):
    try:
        async for msg in consumer:
            print(f"Consumed message: {msg.value}")
    finally:
        await consumer.stop()

# Main function to run the consumer
async def main():
    consumer = await setup_consumer()
    await consume_messages(consumer)

# Entry point to run the script
if __name__ == '__main__':
    asyncio.run(main())
