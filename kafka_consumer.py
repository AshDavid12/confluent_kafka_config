# my_project/kafka_consumer.py
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
from config import get_kafka_config
from models import FactCheckEvent
from kafka_schema_registry import KafkaSchemaRegistrySerializer

async def consume_messages(consumer):
    serializer = KafkaSchemaRegistrySerializer('ai_factcheck_summaries', FactCheckEvent)
    async for msg in consumer:
        deserialized_model = serializer.deserialize(msg.value)
        print(f"Consumed message: {deserialized_model.statement}")

async def start_consumer():
    kafka_config = get_kafka_config()
    consumer = AIOKafkaConsumer(
        'ai_factcheck_summaries',
        bootstrap_servers=kafka_config['bootstrap.servers'],
        security_protocol=kafka_config['security.protocol'],
        sasl_mechanism=kafka_config['sasl.mechanisms'],
        sasl_plain_username=kafka_config['sasl.username'],
        sasl_plain_password=kafka_config['sasl.password'],
        group_id='poopy',
        auto_offset_reset='earliest',
        ssl_context=create_ssl_context(),
    )

    try:
        await consumer.start()
        await consume_messages(consumer)
    finally:
        await consumer.stop()
