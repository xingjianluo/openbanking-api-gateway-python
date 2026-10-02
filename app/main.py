"""FastAPI 入口：开放银行接口集成层。"""
from __future__ import annotations

import time
import uuid
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.consent import check_consent
from app.events import (
    AlreadyRedelivered,
    CallbackRejectedError,
    DeliveryNotFound,
    EventDispatcher,
    RedeliverNotAllowed,
)
from app.idempotency import IdempotencyConflict, run_idempotent
from app.models import (
    Account,
    Consent,
    ConsentStatus,
    DenialReason,
    LedgerEntry,
    Subscription,
)
from app.store import Store


class ConsentIn(BaseModel):
    partner_id: str
    customer_id: str
    scopes: list[str]
    ttl_seconds: float


class AccountIn(BaseModel):
    customer_id: str
    balance_cents: int


class PaymentIn(BaseModel):
    account_id: str
    amount_cents: int = Field(gt=0)
    memo: str = ""


class SubscriptionIn(BaseModel):
    partner_id: str
    event_types: list[str]
    callback_url: str
    secret: str


class InsufficientFunds(Exception):
    """余额不足：业务拒绝，不得改动账本。"""


def _http_sender(url: str, body: bytes, headers: dict[str, str]) -> None:
    resp = httpx.post(url, content=body, headers=headers, timeout=5.0)
    if resp.is_error:
        # 订阅方应答了但拒绝：与网络不可达区分开
        raise CallbackRejectedError(resp.status_code, resp.text[:200])


def create_app(
    sender=None, *, workers: int = 2, max_attempts: int = 3, backoff_base: float = 0.05
) -> FastAPI:
    app = FastAPI(title="Open Banking Integration Layer")
    store = Store()
    app.state.store = store
    app.state.dispatcher = EventDispatcher(
        store,
        sender=sender or _http_sender,
        workers=workers,
        max_attempts=max_attempts,
        backoff_base=backoff_base,
    )

    def deny(reason) -> JSONResponse:
        # 资源不存在与其余授权拒绝使用不同状态码，便于调用方区分
        status = 404 if reason == DenialReason.ACCOUNT_NOT_FOUND else 403
        return JSONResponse(
            status_code=status,
            content={"error": "consent_denied", "reason": reason.value},
        )

    def authorize(
        request: Request,
        required_scope: str,
        account_id: str | None = None,
    ):
        partner_id = request.headers.get("x-partner-id", "")
        consent_id = request.headers.get("x-consent-id", "")
        return check_consent(
            store,
            consent_id=consent_id,
            partner_id=partner_id,
            required_scope=required_scope,
            account_id=account_id,
            now=time.time(),
        )

    # ---- 本地模拟数据管理 ----

    @app.post("/consents", status_code=201)
    def create_consent(body: ConsentIn) -> dict:
        consent = Consent(
            consent_id=str(uuid.uuid4()),
            partner_id=body.partner_id,
            customer_id=body.customer_id,
            scopes=set(body.scopes),
            expires_at=time.time() + body.ttl_seconds,
        )
        with store.lock:
            store.consents[consent.consent_id] = consent
        return {"consent_id": consent.consent_id, "expires_at": consent.expires_at}

    @app.post("/consents/{consent_id}/revoke")
    def revoke_consent(consent_id: str):
        with store.lock:
            consent = store.consents.get(consent_id)
            if consent is None:
                return JSONResponse(
                    status_code=404,
                    content={"error": "consent_not_found"},
                )
            consent.status = ConsentStatus.REVOKED
        return {"consent_id": consent_id, "status": consent.status.value}

    @app.post("/accounts", status_code=201)
    def create_account(body: AccountIn) -> dict:
        account = Account(
            account_id=str(uuid.uuid4()),
            customer_id=body.customer_id,
            balance_cents=body.balance_cents,
        )
        with store.lock:
            store.accounts[account.account_id] = account
        return {"account_id": account.account_id}

    @app.post("/subscriptions", status_code=201)
    def create_subscription(body: SubscriptionIn):
        # 无效订阅请求：明确业务拒绝，不落库
        problems = []
        if not body.event_types:
            problems.append("event_types must not be empty")
        if not body.secret:
            problems.append("secret must not be empty")
        parsed = urlparse(body.callback_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            problems.append("callback_url must be a valid http(s) URL")
        if problems:
            return JSONResponse(
                status_code=400,
                content={"error": "invalid_subscription", "detail": problems},
            )
        sub = Subscription(
            subscription_id=str(uuid.uuid4()),
            partner_id=body.partner_id,
            event_types=set(body.event_types),
            callback_url=body.callback_url,
            secret=body.secret,
        )
        with store.lock:
            store.subscriptions[sub.subscription_id] = sub
        return {"subscription_id": sub.subscription_id}

    @app.get("/deliveries")
    def list_deliveries(status: str | None = Query(default=None)) -> list[dict]:
        with store.lock:
            deliveries = list(store.deliveries.values())
        if status is not None:
            deliveries = [d for d in deliveries if d.status.value == status]
        return [
            {
                "delivery_id": d.delivery_id,
                "event_id": d.event_id,
                "subscription_id": d.subscription_id,
                "status": d.status.value,
                "attempts": d.attempts,
                "last_error": d.last_error,
                "failure_kind": d.failure_kind.value if d.failure_kind else None,
                "redelivery_of": d.redelivery_of,
                "superseded_by": d.superseded_by,
            }
            for d in deliveries
        ]

    @app.post("/deliveries/{delivery_id}/redeliver", status_code=201)
    def redeliver_delivery(delivery_id: str):
        """对最终失败的投递发起重投：生成一条全新的、可追踪的投递记录。"""
        try:
            delivery = app.state.dispatcher.redeliver(delivery_id)
        except DeliveryNotFound:
            return JSONResponse(
                status_code=404,
                content={"error": "delivery_not_found"},
            )
        except RedeliverNotAllowed as exc:
            return JSONResponse(
                status_code=409,
                content={
                    "error": "redeliver_not_allowed",
                    "detail": str(exc),
                },
            )
        except AlreadyRedelivered as exc:
            return JSONResponse(
                status_code=409,
                content={
                    "error": "already_redelivered",
                    "detail": str(exc),
                    "superseded_by": exc.superseded_by,
                },
            )
        return {
            "delivery_id": delivery.delivery_id,
            "redelivery_of": delivery.redelivery_of,
        }

    # ---- 受保护入口 ----

    @app.get("/accounts/{account_id}")
    def get_account(account_id: str, request: Request):
        decision = authorize(request, "accounts:read", account_id)
        if not decision.allowed:
            return deny(decision.reason)
        with store.lock:
            account = store.accounts[account_id]
            return {
                "account_id": account.account_id,
                "balance_cents": account.balance_cents,
            }

    @app.post("/payments", status_code=201)
    def create_payment(
        body: PaymentIn,
        request: Request,
        idempotency_key: str = Header(default=""),
    ):
        decision = authorize(request, "payments:write", body.account_id)
        if not decision.allowed:
            return deny(decision.reason)
        if not idempotency_key:
            return JSONResponse(
                status_code=400,
                content={"error": "missing_idempotency_key"},
            )
        payload = body.model_dump()

        def action() -> tuple[dict, int]:
            with store.lock:
                account = store.accounts[body.account_id]
                if account.balance_cents < body.amount_cents:
                    # 余额不足：业务拒绝，账本与事件投递都不发生
                    raise InsufficientFunds(
                        f"balance {account.balance_cents} < amount {body.amount_cents}"
                    )
                account.balance_cents -= body.amount_cents
                store.ledger.append(
                    LedgerEntry(
                        entry_id=str(uuid.uuid4()),
                        account_id=body.account_id,
                        amount_cents=-body.amount_cents,
                        memo=body.memo,
                    )
                )
                new_balance = account.balance_cents
            deliveries = app.state.dispatcher.publish(
                "payment.completed",
                {
                    "account_id": body.account_id,
                    "amount_cents": body.amount_cents,
                    "memo": body.memo,
                },
            )
            return {
                "payment_id": str(uuid.uuid4()),
                "account_id": body.account_id,
                "balance_cents": new_balance,
                "deliveries_created": len(deliveries),
            }, 201

        try:
            result = run_idempotent(
                store, key=idempotency_key, payload=payload, action=action
            )
        except IdempotencyConflict as exc:
            return JSONResponse(
                status_code=409,
                content={"error": "idempotency_conflict", "detail": str(exc)},
            )
        except InsufficientFunds as exc:
            return JSONResponse(
                status_code=422,
                content={"error": "insufficient_funds", "detail": str(exc)},
            )
        return JSONResponse(
            status_code=result.status_code,
            content=result.body,
            headers={"X-Idempotent-Replay": "true" if result.replayed else "false"},
        )

    return app


app = create_app()
