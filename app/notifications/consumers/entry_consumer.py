# app/notifications/consumer.py

import asyncio
import json
import random

import aio_pika
from aio_pika import IncomingMessage
from loguru import logger

from app.notifications.memory_store import memory_store
from app.rabbitmq import QUEUE_ENTRADA, QUEUE_RETRY, QUEUE_VALIDACAO, get_channel


async def process_message(message: IncomingMessage) -> None:
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            trace_id = payload["traceId"]

            if random.random() < 0.13:
                logger.warning(f"Falha simulada no processamento do traceId={trace_id}")

                memory_store[trace_id] = {
                    "status": "FALHA_PROCESSAMENTO_INICIAL",
                    "payload": payload,
                }

                await publish_to_retry_queue(payload)
                return

            await asyncio.sleep(random.uniform(1, 1.5))

            memory_store[trace_id] = {
                "status": "PROCESSADO_INTERMEDIARIO",
                "payload": payload,
            }
            logger.info(f"Processado com sucesso traceId={trace_id}")

            await publish_to_validacao_queue(payload)

        except Exception as e:
            logger.error(f"Erro ao processar mensagem: {e}")


async def publish_to_retry_queue(payload: dict):
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_RETRY, durable=True)
    await channel.default_exchange.publish(
        aio_pika.Message(body=json.dumps(payload).encode()),
        routing_key=queue.name,
    )
    logger.info(f"Mensagem publicada na fila retry: {queue.name}")


async def publish_to_validacao_queue(payload: dict):
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_VALIDACAO, durable=True)
    await channel.default_exchange.publish(
        aio_pika.Message(body=json.dumps(payload).encode()),
        routing_key=queue.name,
    )
    logger.info(f"Mensagem publicada na fila validacao: {queue.name}")


async def consume():
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_ENTRADA, durable=True)

    await queue.consume(process_message)
    logger.info(f"Consumidor 1 escutando na fila: {QUEUE_ENTRADA}")

    await asyncio.Future()
