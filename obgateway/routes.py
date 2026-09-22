"""HTTP 路由：受保护入口、授权判定、幂等编排与事件发布。

关键顺序（支付类变更请求）：
  1) 解析合作方/同意/幂等键（头部）与请求体
  2) authorize() 授权判定（不同入口共用同一判定链，结论一致）
  3) IdempotencyStore.execute() 包裹实际业务
       - 同键同内容并发：业务恰好执行一次，事件也恰好发布一次
       - 同键异内容：明确 422 拒绝，绝不重复扣款
  4) 业务成功后发布 payment.completed 事件（异步至少一次投递）

授权拒绝（403/AUTHORIZATION）、幂等冲突（409|422/IDEMPOTENCY）、
投递失败（异步，DELIVERY，查 GET /events）三条失败链路互不混用。
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from .authz import authorize
from .ledger import AccountNotFound, InsufficientFunds
from .models import ConsentStatus, DenyReason
from .schemas import PaymentCreateRequest

logger = logging.getLogger("obgateway.routes")

router = APIRouter()

SCOPE_ACCOUNTS_READ = "accounts.read"
SCOPE_PAYMENTS_CREATE = "payments.create"


def _container(request: Request):
    return request.app.state.container


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/consents/{consent_id}")
def get_consent(
    request: Request,
    consent_id: str,
    x_partner_id: str = Header(alias="X-Partner-Id"),
) -> dict:
    """查询同意状态：仍需通过完整授权链（合作方归属一致）。"""
    c = _container(request)
    consent = authorize(
        c.store, partner_id=x_partner_id, consent_id=consent_id,
        required_scope=SCOPE_ACCOUNTS_READ,  # 查同意至少要求有任一已授权范围
    )
    return {
        "consentId": consent.consent_id,
        "partnerId": consent.partner_id,
        "customerId": consent.customer_id,
        "scopes": sorted(consent.scopes),
        "accountIds": sorted(consent.account_ids),
        "status": consent.status.value,
        "expiresAt": consent.expires_at.isoformat(),
        "revokedAt": consent.revoked_at.isoformat() if consent.revoked_at else None,
        "revokeReason": consent.revoke_reason,
    }


@router.post("/consents/{consent_id}/revoke", status_code=200)
def revoke_consent(
    request: Request,
    consent_id: str,
    x_partner_id: str = Header(alias="X-Partner-Id"),
) -> dict:
    """撤销同意：不要求 scope（撤销本身不需要已授权范围），但归属必须一致。

    已撤销的同意重复撤销保持结论一致（幂等视图）。
    """
    c = _container(request)
    inputs = {"partner_id": x_partner_id, "consent_id": consent_id}
    consent = c.store.consents.get(consent_id)
    if consent is None:
        from .errors import AuthorizationError
        logger.warning("撤销拒绝 reason=%s inputs=%s",
                       DenyReason.CONSENT_NOT_FOUND.value, inputs)
        raise AuthorizationError(
            DenyReason.CONSENT_NOT_FOUND, f"同意不存在: {consent_id}", **inputs)
    if consent.partner_id != x_partner_id:
        from .errors import AuthorizationError
        logger.warning("撤销拒绝 reason=%s inputs=%s",
                       DenyReason.CONSENT_PARTNER_MISMATCH.value, inputs)
        raise AuthorizationError(
            DenyReason.CONSENT_PARTNER_MISMATCH,
            f"同意 {consent_id} 不属于合作方 {x_partner_id}", **inputs)

    with c.store.lock:
        already = consent.status is ConsentStatus.REVOKED
        if not already:
            c.store.revoke_consent(consent_id, reason="revoked via API")
        consent = c.store.consents[consent_id]
    logger.info("撤销完成 inputs=%s already_revoked=%s", inputs, already)
    return {"consentId": consent_id, "status": consent.status.value,
            "revokedAt": consent.revoked_at.isoformat() if consent.revoked_at else None,
            "alreadyRevoked": already}


@router.get("/accounts/{account_id}")
def get_account(
    request: Request,
    account_id: str,
    x_partner_id: str = Header(alias="X-Partner-Id"),
    x_consent_id: str = Header(alias="X-Consent-Id"),
) -> dict:
    """受保护入口：账户查询，需要 accounts.read + 有效期 + 资源归属。"""
    c = _container(request)
    logger.info("账户查询入口 partner=%s consent=%s account=%s",
                x_partner_id, x_consent_id, account_id)
    authorize(
        c.store, partner_id=x_partner_id, consent_id=x_consent_id,
        required_scope=SCOPE_ACCOUNTS_READ, account_id=account_id,
    )
    account = c.ledger.get_account(account_id)  # 授权已保证归属，此处仅为存在性
    return c.ledger.account_view(account)


@router.post("/payments", status_code=201)
def create_payment(
    request: Request,
    body: PaymentCreateRequest,
    x_partner_id: str = Header(alias="X-Partner-Id"),
    x_consent_id: str = Header(alias="X-Consent-Id"),
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1),
) -> JSONResponse:
    """受保护变更入口：发起支付，需要 payments.create + 幂等保护。"""
    c = _container(request)
    payload = body.model_dump()
    logger.info(
        "支付入口 partner=%s consent=%s account=%s amount=%s idempotency_key=%s",
        x_partner_id, x_consent_id, body.accountId, body.amountMinor, idempotency_key)

    # 1) 授权先行：撤销/过期/范围不足/归属不匹配在幂等占用之前就被拒绝
    authorize(
        c.store, partner_id=x_partner_id, consent_id=x_consent_id,
        required_scope=SCOPE_PAYMENTS_CREATE, account_id=body.accountId,
    )

    # 2) 幂等包裹；业务与事件发布都只在“首次执行”内发生一次
    def business() -> tuple[int, dict]:
        try:
            payment = c.ledger.create_payment(
                consent_id=x_consent_id,
                account_id=body.accountId,
                amount_minor=body.amountMinor,
                currency=body.currency,
                idempotency_key=idempotency_key,
            )
        except AccountNotFound:
            return 404, {"error": "business_rejected", "category": "BUSINESS",
                         "reason": "ACCOUNT_NOT_FOUND",
                         "message": f"账户不存在: {body.accountId}"}
        except InsufficientFunds as exc:
            return 400, {"error": "business_rejected", "category": "BUSINESS",
                         "reason": "INSUFFICIENT_FUNDS", "message": str(exc)}
        except ValueError as exc:
            return 400, {"error": "business_rejected", "category": "BUSINESS",
                         "reason": "INVALID_PAYMENT", "message": str(exc)}

        view = c.ledger.payment_view(payment)
        # 事件只在实际扣款成功后发布一次（随幂等首次执行）
        records = c.dispatcher.publish(
            "payment.completed",
            {"paymentId": payment.payment_id, "accountId": payment.account_id,
             "amountMinor": payment.amount_minor, "currency": payment.currency},
        )
        logger.info("支付成功 payment_id=%s 生成事件 %d 条 idempotency_key=%s",
                    payment.payment_id, len(records), idempotency_key)
        return 201, view

    status_code, response_body = c.idempotency.execute(
        idempotency_key, payload, business,
    )
    return JSONResponse(status_code=status_code, content=response_body)


@router.get("/events")
def list_events(request: Request, x_partner_id: str = Header(alias="X-Partner-Id")) -> dict:
    """查询异步事件投递记录：投递失败/死信在此可观察，不与同步错误混淆。"""
    c = _container(request)
    with c.store.lock:
        records = [e for e in c.store.events.values() if e.partner_id == x_partner_id]
        return {
            "count": len(records),
            "events": [
                {
                    "eventId": e.event_id,
                    "eventType": e.event_type,
                    "subscriptionId": e.subscription_id,
                    "status": e.status.value,
                    "attempts": e.attempts,
                    "maxAttempts": e.max_attempts,
                    "lastFailureReason": e.last_failure_reason,
                    "deliveredAt": e.delivered_at.isoformat() if e.delivered_at else None,
                    "history": [
                        {"attempt": h.attempt, "status": h.status.value,
                         "httpStatus": h.http_status, "failureReason": h.failure_reason,
                         "at": h.at.isoformat()}
                        for h in e.history
                    ],
                }
                for e in records
            ],
        }
