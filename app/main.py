import logging

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.core.logging import setup_logging
from app.db.session import async_session_maker
from app.services.parser import parse_and_store
from app.services.scheduler import create_scheduler
from fastapi.concurrency import asynccontextmanager


logger = logging.getLogger(__name__)
_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler

    logger.info("Запуск приложения")
    await _run_parse_job()

    _scheduler = create_scheduler(_run_parse_job)
    _scheduler.start()

    yield

    logger.info("Остановка приложения")
    if _scheduler:
        _scheduler.shutdown(wait=False)


app = FastAPI(title="Selectel Vacancies API",
              lifespan=lifespan
              )
app.include_router(api_router)

setup_logging()


async def _run_parse_job() -> None:
    try:
        async with async_session_maker() as session:
            await parse_and_store(session)
    except Exception as exc:
        logger.exception("Ошибка фонового парсинга: %s", exc)
