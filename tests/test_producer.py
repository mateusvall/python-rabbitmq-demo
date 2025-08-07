import json
from unittest.mock import AsyncMock, patch

import pytest

from app.notifications.producer import publish_notification


@pytest.mark.asyncio
@patch("app.notifications.producer.get_rabbitmq_connection")
async def test_publish_notification(mock_get_connection):
    mock_channel = AsyncMock()
    mock_exchange = AsyncMock()
    mock_channel.default_exchange = mock_exchange
    mock_get_connection.return_value.channel = AsyncMock(return_value=mock_channel)

    payload = {
        "traceId": "123",
        "mensagemId": "abc",
        "conteudoMensagem": "teste",
        "tipoNotificacao": "EMAIL",
    }

    await publish_notification(payload)

    args, kwargs = mock_exchange.publish.call_args
    message_obj = args[0]
    routing_key = kwargs.get("routing_key")

    assert routing_key == "fila.notificacao.entrada.mateus"
    assert json.loads(message_obj.body.decode()) == payload
