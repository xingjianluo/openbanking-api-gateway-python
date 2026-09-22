"""FastAPI 应用工厂：组装存储、账本、幂等存储与事件投递器。

错误映射严格区分三大类（响应体 category/reason 可机读）：
- AuthorizationError      -> 403, category=AUTHORIZATION,   reason=DenyReason
- IdempotencyConflictError-> 409/422, category=IDEMPOTENCY, reason=IdempotencyConflict
- 投递失败是异步发生的，不出现在同步响应里；通过 GET /events 查询
  category=DELIVERY 的记录状态，避免与前两者混为一谈。
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .errors import AuthorizationError, IdempotencyConflictError
from .events import EventDispatcher, Transport
from .idempotency import IdempotencyStore
from .ledger import Ledger
from .models import IdempotencyConflict
from .storage import LocalStore

logger = logging.getLogger("obgateway")


class Container:
    def __init__(self, store: LocalStore, ledger: Ledger,
                 idempotency: IdempotencyStore, dispatcher: EventDispatcher) -> None:
        self.store = store
        self.ledger = ledger
        self.idempotency = idempotency
        self.dispatcher = dispatcher


def create_app(
    *,
    transport: Transport | None = None,
    seed: bool = True,
    dispatcher_kwargs: dict | None = None,
) -> FastAPI:
    store = LocalStore(seed=seed)
    ledger = Ledger(store)
    idempotency = IdempotencyStore()
    dispatcher = EventDispatcher(store, transport, **(dispatcher_kwargs or {}))
    container = Container(store, ledger, idempotency, dispatcher)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        dispatcher.start()
        logger.info("开放银行集成层启动")
        try:
            yield
        finally:
            dispatcher.stop()
            logger.info("开放银行集成层停止")

    app = FastAPI(
        title="Open Banking Integration Gateway",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.container = container

    @app.exception_handler(AuthorizationError)
    async def on_authz_error(request: Request, exc: AuthorizationError) -> JSONResponse:
        logger.info("响应授权拒绝 path=%s reason=%s context=%s",
                    request.url.path, exc.reason.value, exc.context)
        return JSONResponse(
            status_code=403,
            content={"error": "authorization_denied", "category": "AUTHORIZATION",
                     "reason": exc.reason.value, "message": str(exc)},
        )

    @app.exception_handler(IdempotencyConflictError)
    async def on_idem_error(request: Request, exc: IdempotencyConflictError) -> JSONResponse:
        if exc.reason is IdempotencyConflict.KEY_IN_FLIGHT:
            status_code = 409
            error_name = "idempotency_in_flight"
        else:
            status_code = 422
            error_name = "idempotency_payload_mismatch"
        logger.info("响应幂等冲突 path=%s reason=%s key=%s",
                    request.url.path, exc.reason.value, exc.idempotency_key)
        return JSONResponse(
            status_code=status_code,
            content={"error": error_name, "category": "IDEMPOTENCY",
                     "reason": exc.reason.value, "message": str(exc),
                     "idempotencyKey": exc.idempotency_key},
        )

    @app.exception_handler(RequestValidationError)
    async def on_validation_error(request: Request,
                                  exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"error": "validation_error", "category": "VALIDATION",
                     "reason": "REQUEST_VALIDATION_FAILED",
                     "message": "请求参数校验失败", "details": exc.errors()},
        )

    from . import routes
    app.include_router(routes.router)
    return app
