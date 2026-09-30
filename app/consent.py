"""同意授权校验：依据范围、有效期与资源归属给出放行或可区分的拒绝理由。"""
from __future__ import annotations

from dataclasses import dataclass

from app.models import ConsentStatus, DenialReason
from app.store import Store


@dataclass
class ConsentDecision:
    allowed: bool
    reason: DenialReason | None = None


def check_consent(
    store: Store,
    *,
    consent_id: str,
    partner_id: str,
    required_scope: str,
    account_id: str | None = None,
    now: float,
) -> ConsentDecision:
    """校验同意；所有受保护入口共用此函数以保证判定一致。

    判定顺序固定，保证同一请求在不同入口得到相同结论：
    1. 同意存在且属于该合作方；
    2. 未被撤销；
    3. 未过期；
    4. 授权范围覆盖所需 scope；
    5. 若指定账户，账户须存在且归属该同意对应的客户。
    """
    with store.lock:
        consent = store.consents.get(consent_id)
        if consent is None or consent.partner_id != partner_id:
            return ConsentDecision(False, DenialReason.CONSENT_NOT_FOUND)
        if consent.status == ConsentStatus.REVOKED:
            return ConsentDecision(False, DenialReason.CONSENT_REVOKED)
        if now >= consent.expires_at:
            return ConsentDecision(False, DenialReason.CONSENT_EXPIRED)
        if required_scope not in consent.scopes:
            return ConsentDecision(False, DenialReason.SCOPE_INSUFFICIENT)
        if account_id is not None:
            account = store.accounts.get(account_id)
            if account is None:
                return ConsentDecision(False, DenialReason.ACCOUNT_NOT_FOUND)
            if account.customer_id != consent.customer_id:
                return ConsentDecision(False, DenialReason.RESOURCE_NOT_OWNED)
        return ConsentDecision(True)
