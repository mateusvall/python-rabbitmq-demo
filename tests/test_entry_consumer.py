import json
from unittest.mock import AsyncMock, patch

import pytest

from app.notifications.consumers.entry_consumer import memory_store, process_message


class DummyProcessContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class DummyMessage:
    def __init__(self, body):
        self.body = body

    def process(self):
        return DummyProcessContext()


@pytest.mark.asyncio
@patch(
    "app.notifications.consumers.entry_consumer.publish_to_retry_queue",
    new_callable=AsyncMock,
)
@patch(
    "app.notifications.consumers.entry_consumer.publish_to_validacao_queue",
    new_callable=AsyncMock,
)
@patch(
    "app.notifications.consumers.entry_consumer.random.random",
    return_value=0.2,
)
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_process_message_success(
    mock_sleep,
    mock_random,
    mock_validacao,
    mock_retry,
):
    memory_store.clear()

    payload = {
        "traceId": "test-trace",
        "mensagemId": "msg-01",
        "conteudoMensagem": "hello",
        "tipoNotificacao": "EMAIL",
    }
    message = DummyMessage(body=json.dumps(payload).encode())

    await process_message(message)

    assert memory_store["test-trace"]["status"] == "PROCESSADO_INTERMEDIARIO"
    mock_validacao.assert_called_once_with(payload)
    mock_retry.assert_not_called()


@pytest.mark.asyncio
@patch(
    "app.notifications.consumers.entry_consumer.publish_to_retry_queue",
    new_callable=AsyncMock,
)
@patch(
    "app.notifications.consumers.entry_consumer.publish_to_validacao_queue",
    new_callable=AsyncMock,
)
@patch(
    "app.notifications.consumers.entry_consumer.random.random",
    return_value=0.1,
)
@patch("asyncio.sleep", new_callable=AsyncMock)
async def test_process_message_failure(
    mock_sleep,
    mock_random,
    mock_validacao,
    mock_retry,
):
    memory_store.clear()

    payload = {
        "traceId": "test-trace-fail",
        "mensagemId": "msg-02",
        "conteudoMensagem": "fail test",
        "tipoNotificacao": "EMAIL",
    }
    message = DummyMessage(body=json.dumps(payload).encode())

    await process_message(message)

    assert memory_store["test-trace-fail"]["status"] == "FALHA_PROCESSAMENTO_INICIAL"
    mock_retry.assert_called_once_with(payload)
    mock_validacao.assert_not_called()
