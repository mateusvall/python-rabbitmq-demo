from uuid import UUID, uuid4

from app.notifications.memory_store import memory_store
from app.notifications.models import (
    NotificationRequest,
    NotificationResponse,
    NotificationStatusResponse,
)
from app.notifications.producer import publish_notification


async def process_notification(payload: NotificationRequest) -> NotificationResponse:
    trace_id = uuid4()
    mensagem_id = payload.mensagemId or uuid4()

    memory_store[str(trace_id)] = {
        "mensagemId": str(mensagem_id),
        "conteudoMensagem": payload.conteudoMensagem,
        "tipoNotificacao": payload.tipoNotificacao,
        "status": "RECEBIDO",
    }

    await publish_notification(
        {
            "traceId": str(trace_id),
            "mensagemId": str(mensagem_id),
            "conteudoMensagem": payload.conteudoMensagem,
            "tipoNotificacao": payload.tipoNotificacao,
        }
    )

    return NotificationResponse(traceId=trace_id, mensagemId=mensagem_id)


def get_notification_status(trace_id: UUID) -> NotificationStatusResponse | None:
    return memory_store.get(str(trace_id))
