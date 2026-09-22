"""FastAPI 入口：开放银行接口集成层。"""
from __future__ import annotations

import time
import uuid

import httpx
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.consent import check_consent
from app.events import EventDispatcher
from app.idempotency import IdempotencyConflict, run_idempotent
from app.models import Account, Consent, ConsentStatus, LedgerEntry, Subscription
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
    resp = httpx.post(url, content=body, headers=headers, timeout=5.0)
    resp.raise_for_status()


def create_app(sender=None) -> FastAPI:
    app = FastAPI(title="Open Banking Integration Layer")
    store = Store()
    app.state.store = store
    app.state.dispatcher = EventDispatcher(store, sender=sender or _http_sender)

    def deny(reason) -> JSONResponse:
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
            consent = store.consents[consent_id]
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
    def list_deliveries() -> list[dict]:
        with store.lock:
            return [
                {
                    "delivery_id": d.delivery_id,
                    "event_id": d.event_id,
                    "subscription_id": d.subscription_id,
                    "status": d.status.value,
                    "attempts": d.attempts,
                    "last_error": d.last_error,
                }
                for d in store.deliveries.values()
            ]

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
            app.state.dispatcher.publish(
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
