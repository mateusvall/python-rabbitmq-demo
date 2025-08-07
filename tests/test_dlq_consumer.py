import json

import pytest

from app.notifications.consumers.dlq_consumer import process_dlq_message


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
async def test_process_dlq_message():
    payload = {"traceId": "dlq-trace"}
    message = DummyMessage(body=json.dumps(payload).encode())

    await process_dlq_message(message)
