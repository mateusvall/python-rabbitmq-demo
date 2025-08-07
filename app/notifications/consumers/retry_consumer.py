import asyncio
import json
import random

from aio_pika import IncomingMessage, Message
from loguru import logger

from app.notifications.memory_store import memory_store
from app.rabbitmq import QUEUE_DLQ, QUEUE_RETRY, QUEUE_VALIDACAO, get_channel


async def process_retry_message(message: IncomingMessage) -> None:
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            trace_id = payload["traceId"]

            logger.info(f"Iniciando reprocessamento para traceId={trace_id}")

            await asyncio.sleep(3)

            if random.random() < 0.20:
                logger.warning(f"Falha no reprocessamento do traceId={trace_id}")

                memory_store[trace_id] = {
                    "status": "FALHA_FINAL_REPROCESSAMENTO",
                    "payload": payload,
                }

                await publish_to_dlq_queue(payload)
                return

            memory_store[trace_id] = {
                "status": "REPROCESSADO_COM_SUCESSO",
                "payload": payload,
            }
            logger.info(f"Reprocessado com sucesso traceId={trace_id}")

            await publish_to_validacao_queue(payload)

        except Exception as e:
            logger.error(f"Erro ao processar retry message: {e}")


async def publish_to_dlq_queue(payload: dict):
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_DLQ, durable=True)
    await channel.default_exchange.publish(
        Message(body=json.dumps(payload).encode()),
        routing_key=queue.name,
    )
    logger.info(f"Mensagem publicada na DLQ: {queue.name}")


async def publish_to_validacao_queue(payload: dict):
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_VALIDACAO, durable=True)
    await channel.default_exchange.publish(
        Message(body=json.dumps(payload).encode()),
        routing_key=queue.name,
    )
    logger.info(f"Mensagem publicada na fila validacao: {queue.name}")


async def consume_retry():
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_RETRY, durable=True)

    await queue.consume(process_retry_message)
    logger.info(f"Consumidor Retry escutando na fila: {QUEUE_RETRY}")

    await asyncio.Future()
