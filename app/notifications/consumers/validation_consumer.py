import asyncio
import json
import random

from aio_pika import IncomingMessage, Message
from loguru import logger

from app.notifications.memory_store import memory_store
from app.rabbitmq import QUEUE_DLQ, QUEUE_VALIDACAO, get_channel


async def process_validacao_message(message: IncomingMessage) -> None:
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            trace_id = payload["traceId"]
            tipo_notificacao = payload.get("tipoNotificacao", "").upper()

            logger.info(
                f"Processando validação para traceId={trace_id}, tipo={tipo_notificacao}"
            )

            await asyncio.sleep(random.uniform(0.5, 1))

            if random.random() < 0.05:
                logger.error(f"Falha no envio final para traceId={trace_id}")

                memory_store[trace_id] = {
                    "status": "FALHA_ENVIO_FINAL",
                    "payload": payload,
                }

                await publish_to_dlq_queue(payload)
                return

            memory_store[trace_id] = {
                "status": "ENVIADO_SUCESSO",
                "payload": payload,
            }
            logger.success(f"Envio final bem sucedido para traceId={trace_id}")

        except Exception as e:
            logger.error(f"Erro ao processar mensagem de validação: {e}")


async def publish_to_dlq_queue(payload: dict):
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_DLQ, durable=True)
    await channel.default_exchange.publish(
        Message(body=json.dumps(payload).encode()),
        routing_key=queue.name,
    )
    logger.info(f"Mensagem publicada na DLQ: {queue.name}")


async def consume_validacao():
    channel = await get_channel()
    queue = await channel.declare_queue(QUEUE_VALIDACAO, durable=True)

    await queue.consume(process_validacao_message)
    logger.info(f"Consumidor Validação escutando na fila: {QUEUE_VALIDACAO}")

    await asyncio.Future()
