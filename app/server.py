from fastapi import FastAPI

from app.notifications.routes import router as notifications_router
from app.rabbitmq import declare_queues

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    await declare_queues()


app.include_router(notifications_router)
