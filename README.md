# Kafka Consumer with Schema Registry Integration

This project demonstrates a Kafka consumer application using `aiokafka` for asynchronous message consumption and Confluent's Schema Registry for Avro serialization and deserialization. The consumer is designed to consume fact-checking events from a Kafka topic, deserialize them into Python models using Avro, and print the consumed messages.

## Project Structure

- **config.py**: Loads environment variables and provides Kafka configuration settings.
- **kafka_consumer.py**: Contains the Kafka consumer logic, including message consumption and deserialization.
- **kafka_schema_registry.py**: Handles Avro serialization and deserialization of Pydantic models with Schema Registry integration.
- **models.py**: Defines the Pydantic models used for deserializing Kafka messages.
- **main.py**: Entry point to start the Kafka consumer.

## Setup

1. **Clone the repository**:
    ```sh
    git clone https://github.com/AshDavid12/confluent_kafka_config.git
    cd kafka-consumer
    ```

2. **Create a `.env` file**:
    Create a `.env` file in the root directory with the following environment variables:
    ```ini
    SASL_USERNAME=your_sasl_username
    SASL_PASSWORD=your_sasl_password
    KAFKA_BROKER_2=your_kafka_broker
    SCHEMA_REGISTRY=your_schema_registry_url
    SCHEMA_REGISTRY_USER_INFO=your_schema_registry_user_info
    ```

3. **Install dependencies with Poetry**:
    ```sh
    poetry install
    ```

4. **Activate the virtual environment**:
    ```sh
    poetry shell
    ```

## Running the Consumer

Start the Kafka consumer by running the following command:
```sh
poetry run python main.py
