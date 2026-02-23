from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse

from hooks.errors import AppError
from utils.logger_utils import get_logger

logger = get_logger("ErrorHandlers")


async def app_error_handler(request: Request, exc: Exception):
    err = (
        exc
        if isinstance(exc, AppError)
        else AppError.from_exc(exc, message="AppError handler received non-AppError")
    )
    payload = err.to_dict()
    status = err.http_status
    payload["path"] = str(request.url)
    return JSONResponse(status_code=status, content=payload)


async def unhandled_error_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception at %s", request.url.path, exc_info=exc)
    err = AppError.from_exc(exc, message="Unhandled server error")
    payload = err.to_dict()
    payload["path"] = str(request.url)
    return JSONResponse(status_code=err.http_status, content=payload)
