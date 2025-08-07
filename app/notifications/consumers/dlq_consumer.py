import asyncio
import json

from aio_pika import IncomingMessage
from loguru import logger

from app.rabbitmq import QUEUE_DLQ, get_channel


async def process_dlq_message(message: IncomingMessage) -> None:
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            trace_id = payload.get("traceId", "unknown")

            logger.warning(
                f"Mensagem na DLQ (não será mais processada) traceId={trace_id} payload={payload}"
            )

        except Exception as e:
            logger.error(f"Erro ao processar mensagem da DLQ: {e}")


async def consume_dlq():
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_DLQ, durable=True)

    await queue.consume(process_dlq_message)
    logger.info(f"Consumidor DLQ escutando na fila: {QUEUE_DLQ}")

    await asyncio.Future()
