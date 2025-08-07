import json
from unittest.mock import AsyncMock, patch

import pytest

from app.notifications.consumers.retry_consumer import (
    process_retry_message,
)


class DummyCtx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class DummyMessage:
    def __init__(self, body):
        self.body = body

    def process(self):
        return DummyCtx()


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
@patch("app.notifications.consumers.retry_consumer.random.random", return_value=0.3)
@patch("app.notifications.consumers.retry_consumer.memory_store", new_callable=dict)
@patch(
    "app.notifications.consumers.retry_consumer.publish_to_validacao_queue",
    new_callable=AsyncMock,
)
@patch(
    "app.notifications.consumers.retry_consumer.publish_to_dlq_queue",
    new_callable=AsyncMock,
)
async def test_retry_success(
    mock_dlq,
    mock_validacao,
    mock_memory_store,
    mock_random,
    mock_sleep,
):
    payload = {"traceId": "retry-trace-success"}
    message = DummyMessage(body=json.dumps(payload).encode())

    await process_retry_message(message)

    print("memory_store after success:", mock_memory_store)
    assert (
        mock_memory_store["retry-trace-success"]["status"] == "REPROCESSADO_COM_SUCESSO"
    )
    mock_validacao.assert_called_once_with(payload)
    mock_dlq.assert_not_called()


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
@patch("app.notifications.consumers.retry_consumer.random.random", return_value=0.1)
@patch("app.notifications.consumers.retry_consumer.memory_store", new_callable=dict)
@patch(
    "app.notifications.consumers.retry_consumer.publish_to_validacao_queue",
    new_callable=AsyncMock,
)
@patch(
    "app.notifications.consumers.retry_consumer.publish_to_dlq_queue",
    new_callable=AsyncMock,
)
async def test_retry_failure(
    mock_dlq,
    mock_validacao,
    mock_memory_store,
    mock_random,
    mock_sleep,
):
    payload = {"traceId": "retry-trace-fail"}
    message = DummyMessage(body=json.dumps(payload).encode())

    await process_retry_message(message)

    print("memory_store after failure:", mock_memory_store)
    assert (
        mock_memory_store["retry-trace-fail"]["status"] == "FALHA_FINAL_REPROCESSAMENTO"
    )
    mock_dlq.assert_called_once_with(payload)
    mock_validacao.assert_not_called()
