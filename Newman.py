import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel
from typing import Optional, Type, TypeVar, Generic
import py_avro_schema
from enum import Enum
from datetime import datetime

SASL_USERNAME = ""
SASL_PASSWORD = ""
KAFKA_BROKER_2 = ""
SCHEMA_REGISTRY = ""
SCHEMA_REGISTRY_USER_INFO = ""


class FactCheckEventStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    WORKING = "WORKING"

def get_kafka_schema_registry_config() -> dict:
    config = {"url": SCHEMA_REGISTRY}
    if SCHEMA_REGISTRY_USER_INFO:
        config["basic.auth.user.info"] = SCHEMA_REGISTRY_USER_INFO
    return config

def avro_schema_str(cls) -> str:
    """
    Returns the Avro schema as a string for the given Pydantic model.
    """
    return py_avro_schema.generate(
        cls,
        options=py_avro_schema.Option.MILLISECONDS
        | py_avro_schema.Option.JSON_APPEND_NEWLINE
        | py_avro_schema.Option.JSON_INDENT_2,
    ).decode()

T = TypeVar("T", bound=BaseModel)

class KafkaSchemaRegistrySerializer(Generic[T]):
    """
    A generic serializer/deserializer class for integrating Pydantic models with Kafka
    and Confluent Schema Registry using Avro Schema.
    """
    def __init__(self, topic_name: str, model_class: Type[T]):
        self.topic_name = topic_name
        self.model_class = model_class
        self.schema_registry_client = SchemaRegistryClient(get_kafka_schema_registry_config())
        self.subject_name = f"{topic_name}-value"
        schema = avro_schema_str(model_class)

        self.value_serializer = AvroSerializer(
            schema_str=schema,
            schema_registry_client=self.schema_registry_client,
            conf={
                "auto.register.schemas": True,
                "normalize.schemas": True,
            },
        )
        self.value_deserializer = AvroDeserializer(
            schema_str=schema,
            schema_registry_client=self.schema_registry_client,
        )

    def serialize(self, model: T) -> bytes:
        """
        Serialize a Pydantic model into bytes.
        """
        model_dict = model.dict()
        context = SerializationContext(self.topic_name, MessageField.VALUE)
        return self.value_serializer(model_dict, context)

    def deserialize_dict(self, message: bytes) -> dict:
        """
        Deserialize bytes back into a dictionary.
        """
        context = SerializationContext(self.topic_name, MessageField.VALUE)
        return self.value_deserializer(message, context)

    def deserialize(self, message: bytes) -> T:
        """
        Deserialize bytes back into a Pydantic model instance.
        """
        return self.model_class.model_validate(self.deserialize_dict(message))

class FactCheckEvent(BaseModel):
    channel_id: Optional[str] = None
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    start_time: Optional[datetime]  
    statement: str
    rationale: str
    score: float
    context: Optional[str] = None
    soundbite_id: Optional[str] = None
    display_duration_seconds: Optional[int] = 15
    force_factcheck: Optional[bool] = False
    is_command: Optional[bool] = False
    sentence_summary: str
    paragraph_summary: str
    three_paragraph_summary: Optional[str] = None
    concise_one_sentence_summary: Optional[str] = None
    support_score: float
    factcheck_status: Optional[FactCheckEventStatus]

def get_kafka_config() -> dict:
    return {
        "bootstrap.servers": KAFKA_BROKER_2,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD
    }

async def consume_messages(consumer):
    serializer = KafkaSchemaRegistrySerializer('ai_factcheck_summaries', FactCheckEvent)
    async for msg in consumer:
        deserialized_model = serializer.deserialize(msg.value)
        print(f"Consumed message: {deserialized_model.statement}")

async def main():
    kafka_config = get_kafka_config()
    consumer = AIOKafkaConsumer(
        'ai_factcheck_summaries',
        bootstrap_servers=kafka_config['bootstrap.servers'],
        security_protocol=kafka_config['security.protocol'],
        sasl_mechanism=kafka_config['sasl.mechanisms'],
        sasl_plain_username=kafka_config['sasl.username'],
        sasl_plain_password=kafka_config['sasl.password'],
        group_id='gaa',
        auto_offset_reset='earliest',
        ssl_context=create_ssl_context(),
    )

    try:
        await consumer.start()
        await consume_messages(consumer)
    finally:
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(main())
