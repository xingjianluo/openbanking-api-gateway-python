"""同意与授权校验：范围、有效期、资源归属。

所有受保护入口都必须经过同一个 authorize()，判定顺序固定，保证不同入口
对同一请求得到完全一致的结论：

1. 合作方存在且启用
2. 同意存在
3. 同意归属该合作方（禁止跨合作方使用）
4. 同意状态：REVOKED 优先于一切；非 AUTHORISED 拒绝
5. 有效期：已到截止时刻即 CONSENT_EXPIRED（即使状态字段滞后未刷新）
6. 授权范围：必须包含入口要求的 scope
7. 资源归属：目标账户必须在同意的资源白名单内

注意第 4、5 步的优先级：撤销 > 过期 > 范围不足 > 资源不匹配。
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from .errors import AuthorizationError
from .models import Consent, ConsentStatus, DenyReason, utcnow
from .storage import LocalStore

logger = logging.getLogger("obgateway.authz")


def authorize(
    store: LocalStore,
    *,
    partner_id: str,
    consent_id: str,
    required_scope: str,
    account_id: str | None = None,
    now: datetime | None = None,
) -> Consent:
    """返回授权通过的 Consent；拒绝时抛 AuthorizationError（携带 DenyReason）。"""
    current = now or utcnow()
    inputs: dict[str, Any] = {
        "partner_id": partner_id,
        "consent_id": consent_id,
        "required_scope": required_scope,
        "account_id": account_id,
        "at": current.isoformat(),
    }

    def deny(reason: DenyReason, message: str) -> AuthorizationError:
        logger.warning("授权拒绝 reason=%s inputs=%s detail=%s", reason.value, inputs, message)
        return AuthorizationError(reason, message, **inputs)

    partner = store.partners.get(partner_id)
    if partner is None:
        raise deny(DenyReason.PARTNER_NOT_FOUND, f"合作方不存在: {partner_id}")
    if not partner.active:
        raise deny(DenyReason.PARTNER_INACTIVE, f"合作方已停用: {partner_id}")

    consent = store.consents.get(consent_id)
    if consent is None:
        raise deny(DenyReason.CONSENT_NOT_FOUND, f"同意不存在: {consent_id}")

    if consent.partner_id != partner_id:
        raise deny(
            DenyReason.CONSENT_PARTNER_MISMATCH,
            f"同意 {consent_id} 属于合作方 {consent.partner_id}，不属于 {partner_id}",
        )

    # 撤销优先判定（一个同意可能同时已过期，撤销是更强的客户意图表达）
    if consent.status is ConsentStatus.REVOKED:
        raise deny(
            DenyReason.CONSENT_REVOKED,
            f"同意已撤销: {consent.revoke_reason or '未注明原因'}",
        )
    if consent.status is not ConsentStatus.AUTHORISED:
        raise deny(
            DenyReason.CONSENT_STATUS_NOT_AUTHORISED,
            f"同意状态为 {consent.status.value}，非 AUTHORISED",
        )

    if current >= consent.expires_at:
        raise deny(
            DenyReason.CONSENT_EXPIRED,
            f"同意已于 {consent.expires_at.isoformat()} 过期",
        )

    if required_scope not in consent.scopes:
        raise deny(
            DenyReason.SCOPE_INSUFFICIENT,
            f"同意范围 {sorted(consent.scopes)} 不包含 {required_scope}",
        )

    if account_id is not None and account_id not in consent.account_ids:
        raise deny(
            DenyReason.RESOURCE_MISMATCH,
            f"账户 {account_id} 不在同意资源白名单 {sorted(consent.account_ids)} 内",
        )

    logger.info("授权通过 inputs=%s", inputs)
    return consent
