import json
from unittest.mock import AsyncMock, patch

import pytest

from app.notifications.consumers.validation_consumer import (
    process_validacao_message,
)


class DummyMessage:
    def __init__(self, body):
        self.body = body

    def process(self):
        class DummyCtx:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

        return DummyCtx()


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
@patch(
    "app.notifications.consumers.validation_consumer.random.random", return_value=0.1
)
@patch(
    "app.notifications.consumers.validation_consumer.memory_store", new_callable=dict
)
@patch(
    "app.notifications.consumers.validation_consumer.publish_to_dlq_queue",
    new_callable=AsyncMock,
)
async def test_validation_success(mock_dlq, mock_memory_store, mock_random, mock_sleep):
    payload = {"traceId": "val-trace-success", "tipoNotificacao": "EMAIL"}
    message = DummyMessage(body=json.dumps(payload).encode())

    await process_validacao_message(message)

    assert mock_memory_store["val-trace-success"]["status"] == "ENVIADO_SUCESSO"
    mock_dlq.assert_not_called()


@pytest.mark.asyncio
@patch("asyncio.sleep", new_callable=AsyncMock)
@patch(
    "app.notifications.consumers.validation_consumer.random.random", return_value=0.01
)
@patch(
    "app.notifications.consumers.validation_consumer.memory_store", new_callable=dict
)
@patch(
    "app.notifications.consumers.validation_consumer.publish_to_dlq_queue",
    new_callable=AsyncMock,
)
async def test_validation_failure(mock_dlq, mock_memory_store, mock_random, mock_sleep):
    payload = {"traceId": "val-trace-fail", "tipoNotificacao": "EMAIL"}
    message = DummyMessage(body=json.dumps(payload).encode())

    await process_validacao_message(message)

    assert mock_memory_store["val-trace-fail"]["status"] == "FALHA_ENVIO_FINAL"
    mock_dlq.assert_called_once_with(payload)
