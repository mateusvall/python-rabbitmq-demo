from uuid import UUID

from fastapi import APIRouter, HTTPException

from app.notifications.models import (
    NotificationRequest,
    NotificationResponse,
    NotificationStatusResponse,
)
from app.notifications.services import get_notification_status, process_notification

router = APIRouter()


@router.post("/api/notificar", response_model=NotificationResponse, status_code=202)
async def notificar(payload: NotificationRequest):
    return await process_notification(payload)


@router.get(
    "/api/notificacao/status/{trace_id}",
    response_model=NotificationStatusResponse,
)
async def consultar_status(trace_id: UUID):
    status_data = get_notification_status(trace_id)
    if not status_data:
        raise HTTPException(status_code=404, detail="Notificação não encontrada")

    return NotificationStatusResponse(traceId=trace_id, **status_data)
