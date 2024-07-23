import os 
from dotenv import load_dotenv

load_dotenv('.env')

SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
KAFKA_BROKER_2 = os.getenv("KAFKA_BROKER_2")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY")
SCHEMA_REGISTRY_USER_INFO = os.getenv("SCHEMA_REGISTRY_USER_INFO")


def get_kafka_config() -> dict:
    return {
        "bootstrap.servers": KAFKA_BROKER_2,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD
    }

def get_kafka_schema_registry_config() -> dict:
    config = {"url": SCHEMA_REGISTRY}
    if SCHEMA_REGISTRY_USER_INFO:
        config["basic.auth.user.info"] = SCHEMA_REGISTRY_USER_INFO
    return config
