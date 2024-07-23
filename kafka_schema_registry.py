import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
import py_avro_schema
from pydantic import BaseModel
from typing import Optional, Type, TypeVar, Generic
from config import get_kafka_schema_registry_config



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
