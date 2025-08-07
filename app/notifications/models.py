from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class NotificationRequest(BaseModel):
    mensagemId: Optional[UUID] = Field(default_factory=uuid4)
    conteudoMensagem: str
    tipoNotificacao: str


class NotificationResponse(BaseModel):
    mensagemId: UUID
    traceId: UUID


class NotificationStatusResponse(BaseModel):
    traceId: UUID
    mensagemId: UUID
    conteudoMensagem: str
    tipoNotificacao: str
    status: str
