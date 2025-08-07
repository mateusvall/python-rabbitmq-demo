import json

from aio_pika import Message

from app.rabbitmq import QUEUE_ENTRADA, get_rabbitmq_connection


async def publish_notification(payload: dict):
    connection = await get_rabbitmq_connection()
    channel = await connection.channel()
    await channel.declare_queue(QUEUE_ENTRADA, durable=True)

    message = Message(
        json.dumps(payload).encode("utf-8"), content_type="application/json"
    )
    await channel.default_exchange.publish(message, routing_key=QUEUE_ENTRADA)
