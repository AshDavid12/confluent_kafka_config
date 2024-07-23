# main.py
import asyncio
from kafka_consumer import start_consumer

if __name__ == '__main__':
    asyncio.run(start_consumer())
