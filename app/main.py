"""FastAPI 入口：开放银行接口集成层。"""
from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.consent import check_consent
from app.errors import (
    AccountNotFound,
    BusinessError,
    ConsentNotFound,
    DeliveryNotFound,
    InsufficientFunds,
    InvalidAmount,
    InvalidSubscription,
    SubscriptionNotFound,
)
from app.events import (
    DeliveryNetworkError,
    EventDispatcher,
    SubscriberRejected,
    SubscriberServerError,
)
from app.idempotency import IdempotencyConflict, run_idempotent
from app.models import (
    Account,
    Consent,
    ConsentStatus,
    Delivery,
    DeliveryStatus,
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
    amount_cents: int
    memo: str = ""


class SubscriptionIn(BaseModel):
    partner_id: str
    event_types: list[str]
    callback_url: str
    secret: str


def _http_sender(url: str, body: bytes, headers: dict[str, str]) -> None:
    """默认传输层：把合作方响应归类为可区分的投递失败。"""
    try:
        resp = httpx.post(url, content=body, headers=headers, timeout=5.0)
    except httpx.TimeoutException as exc:
        raise DeliveryNetworkError(f"timeout: {exc}") from exc
    except httpx.TransportError as exc:
        # 连接拒绝 / DNS 失败 / 不可达
        raise DeliveryNetworkError(f"{type(exc).__name__}: {exc}") from exc
    if 400 <= resp.status_code < 500:
        raise SubscriberRejected(f"HTTP {resp.status_code}: {resp.text[:200]}")
    if resp.status_code >= 500:
        raise SubscriberServerError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    if resp.status_code >= 300:
        raise SubscriberRejected(f"HTTP {resp.status_code}")


_TERMINAL_BAD = {
    DeliveryStatus.FAILED.value,
    DeliveryStatus.REJECTED.value,
    DeliveryStatus.INVALID_SUBSCRIPTION.value,
}
_INFLIGHT = {DeliveryStatus.PENDING.value, DeliveryStatus.RETRYING.value}


def _event_outcome(statuses: set[str]) -> str:
    """把一个事件下多条订阅投递汇总为单一可解释结论。"""
    delivered = DeliveryStatus.DELIVERED.value
    if statuses == {delivered}:
        return delivered
    has_bad = bool(statuses & _TERMINAL_BAD)
    if delivered in statuses and has_bad:
        return "partial"
    if has_bad:
        if DeliveryStatus.FAILED.value in statuses:
            return DeliveryStatus.FAILED.value
        if DeliveryStatus.REJECTED.value in statuses:
            return DeliveryStatus.REJECTED.value
        return DeliveryStatus.INVALID_SUBSCRIPTION.value
    if statuses & _INFLIGHT:
        return "pending"
    return delivered


def create_app(
    sender=None,
    *,
    max_attempts: int = 3,
    backoff_base: float = 0.05,
    worker_count: int = 4,
    queue_maxsize: int = 256,
    enqueue_timeout: float = 0.05,
    sweep_interval: float = 0.02,
    lease_timeout: float = 60.0,
) -> FastAPI:
    store = Store()
    dispatcher = EventDispatcher(
        store,
        sender=sender or _http_sender,
        max_attempts=max_attempts,
        backoff_base=backoff_base,
        worker_count=worker_count,
        queue_maxsize=queue_maxsize,
        enqueue_timeout=enqueue_timeout,
        sweep_interval=sweep_interval,
        lease_timeout=lease_timeout,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 后台投递器随应用生命周期启动与停止，避免“导入即启动”产生
        # 无人管理的常驻线程。
        dispatcher.start()
        try:
            yield
        finally:
            dispatcher.stop()

    app = FastAPI(title="Open Banking Integration Layer", lifespan=lifespan)
    app.state.store = store
    app.state.dispatcher = dispatcher

    @app.exception_handler(BusinessError)
    def on_business_error(request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.error_code, "detail": exc.detail},
        )

    def deny(reason: DenialReason) -> JSONResponse:
        # 不存在/已失效的资源给 404，其余授权类拒绝给 403，结论可区分。
        if reason == DenialReason.RESOURCE_NOT_FOUND:
            return JSONResponse(
                status_code=404,
                content={
                    "error": "resource_not_found",
                    "reason": reason.value,
                },
            )
        return JSONResponse(
            status_code=403,
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

    def _delivery_dict(d: Delivery) -> dict:
        return {
            "delivery_id": d.delivery_id,
            "event_id": d.event_id,
            "subscription_id": d.subscription_id,
            "status": d.status.value,
            "attempts": d.attempts,
            "last_error": d.last_error,
            "failure_kind": d.failure_kind.value if d.failure_kind else None,
            "origin_delivery_id": d.origin_delivery_id,
            "next_attempt_at": d.next_attempt_at or None,
            "created_at": d.created_at,
            "updated_at": d.updated_at,
        }

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
    def revoke_consent(consent_id: str) -> dict:
        with store.lock:
            consent = store.consents.get(consent_id)
            if consent is None:
                raise ConsentNotFound(f"consent {consent_id!r} not found")
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
    def create_subscription(body: SubscriptionIn) -> dict:
        if not body.partner_id or not body.partner_id.strip():
            raise InvalidSubscription("partner_id must not be empty")
        if not body.event_types or not all(t.strip() for t in body.event_types):
            raise InvalidSubscription("event_types must not be empty")
        if not body.secret or not body.secret.strip():
            raise InvalidSubscription("secret must not be empty")
        if not body.callback_url or "://" not in body.callback_url:
            raise InvalidSubscription("callback_url must be an absolute http(s) url")
        scheme = body.callback_url.split("://", 1)[0].lower()
        if scheme not in ("http", "https"):
            raise InvalidSubscription("callback_url scheme must be http or https")
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

    @app.get("/subscriptions/{subscription_id}")
    def get_subscription(subscription_id: str) -> dict:
        with store.lock:
            sub = store.subscriptions.get(subscription_id)
            if sub is None:
                raise SubscriptionNotFound(
                    f"subscription {subscription_id!r} not found"
                )
            return {
                "subscription_id": sub.subscription_id,
                "partner_id": sub.partner_id,
                "event_types": sorted(sub.event_types),
                "callback_url": sub.callback_url,
            }

    @app.get("/deliveries")
    def list_deliveries(status: str | None = None) -> list[dict]:
        with store.lock:
            rows = list(store.deliveries.values())
        if status is not None:
            rows = [d for d in rows if d.status.value == status]
        return [_delivery_dict(d) for d in rows]

    @app.get("/deliveries/{delivery_id}")
    def get_delivery(delivery_id: str) -> dict:
        with store.lock:
            d = store.deliveries.get(delivery_id)
            if d is None:
                raise DeliveryNotFound(f"delivery {delivery_id!r} not found")
            return _delivery_dict(d)

    @app.post("/deliveries/{delivery_id}/redrive", status_code=202)
    def redrive_delivery(delivery_id: str) -> dict:
        new_delivery = dispatcher.redrive(delivery_id)
        return {"redelivery_id": new_delivery.delivery_id}

    @app.get("/events/{event_id}/deliveries")
    def event_deliveries(event_id: str) -> dict:
        """事件级投递结论：每条订阅的状态；无订阅者时给出 no_subscriber。"""
        with store.lock:
            event = store.events.get(event_id)
            if event is None:
                raise DeliveryNotFound(f"event {event_id!r} not found")
            rows = [
                _delivery_dict(d)
                for d in store.deliveries.values()
                if d.event_id == event_id
            ]
        if not rows:
            return {
                "event_id": event_id,
                "outcome": DeliveryStatus.NO_SUBSCRIBER.value,
                "deliveries": [],
            }
        return {
            "event_id": event_id,
            "outcome": _event_outcome({r["status"] for r in rows}),
            "deliveries": rows,
        }

    # ---- 受保护入口 ----

    @app.get("/accounts/{account_id}")
    def get_account(account_id: str, request: Request):
        decision = authorize(request, "accounts:read", account_id)
        if not decision.allowed:
            return deny(decision.reason)
        with store.lock:
            account = store.accounts.get(account_id)
            if account is None:
                raise AccountNotFound(f"account {account_id!r} not found")
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
        # 明确的业务校验发生在登记幂等/账本之前，拒绝不留任何副作用。
        if body.amount_cents <= 0:
            raise InvalidAmount("amount_cents must be positive")
        with store.lock:
            account = store.accounts.get(body.account_id)
            if account is None:
                raise AccountNotFound(f"account {body.account_id!r} not found")
            if account.balance_cents < body.amount_cents:
                raise InsufficientFunds(
                    f"balance {account.balance_cents} < amount {body.amount_cents}"
                )
        payload = body.model_dump()

        def action() -> tuple[dict, int]:
            with store.lock:
                account = store.accounts[body.account_id]
                # 双重检查：并发下余额仍需充足，杜绝透支。
                if account.balance_cents < body.amount_cents:
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
            dispatcher.publish(
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
        return JSONResponse(
            status_code=result.status_code,
            content=result.body,
            headers={"X-Idempotent-Replay": "true" if result.replayed else "false"},
        )

    return app


app = create_app()
