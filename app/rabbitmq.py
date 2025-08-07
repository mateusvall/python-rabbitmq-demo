import asyncio
import os

from aio_pika import Channel, RobustConnection, connect_robust
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")

_connection: RobustConnection | None = None
_connection_lock = asyncio.Lock()

_channel: Channel | None = None


# Nomes das filas
QUEUE_ENTRADA = "fila.notificacao.entrada.mateus"
QUEUE_RETRY = "fila.notificacao.retry.mateus"
QUEUE_VALIDACAO = "fila.notificacao.validacao.mateus"
QUEUE_DLQ = "fila.notificacao.dlq.mateus"


async def get_rabbitmq_connection() -> RobustConnection:
    global _connection
    if _connection and not _connection.is_closed:
        return _connection

    async with _connection_lock:
        if _connection and not _connection.is_closed:
            return _connection

        logger.info("Estabelecendo nova conexão com o RabbitMQ...")
        _connection = await connect_robust(RABBITMQ_URL)
        logger.success("Conexão com RabbitMQ estabelecida com sucesso.")
        return _connection


async def get_channel() -> Channel:
    global _channel
    if _channel and not _channel.is_closed:
        return _channel

    connection = await get_rabbitmq_connection()
    _channel = await connection.channel()
    return _channel


async def declare_queues() -> None:
    """
    Declarar filas necessárias na inicialização da aplicação.
    """
    channel = await get_channel()
    await channel.declare_queue(QUEUE_ENTRADA, durable=True)
    await channel.declare_queue(QUEUE_RETRY, durable=True)
    await channel.declare_queue(QUEUE_VALIDACAO, durable=True)
    await channel.declare_queue(QUEUE_DLQ, durable=True)
    logger.info("Filas declaradas com sucesso.")
