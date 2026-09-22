"""领域模型：合作方、同意授权、账户、支付、事件订阅与投递记录。

本地账本模拟开放银行对接方侧的数据，全部使用时区感知的 UTC 时间。
金额使用最小货币单位（分）的整数表示，避免浮点误差。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ConsentStatus(str, Enum):
    PENDING = "PENDING"
    AUTHORISED = "AUTHORISED"
    REVOKED = "REVOKED"
    EXPIRED = "EXPIRED"


class DenyReason(str, Enum):
    """受保护入口的可区分拒绝原因（授权策略层）。"""
    PARTNER_NOT_FOUND = "PARTNER_NOT_FOUND"
    PARTNER_INACTIVE = "PARTNER_INACTIVE"
    CONSENT_NOT_FOUND = "CONSENT_NOT_FOUND"
    CONSENT_PARTNER_MISMATCH = "CONSENT_PARTNER_MISMATCH"
    CONSENT_STATUS_NOT_AUTHORISED = "CONSENT_STATUS_NOT_AUTHORISED"
    CONSENT_REVOKED = "CONSENT_REVOKED"
    CONSENT_EXPIRED = "CONSENT_EXPIRED"
    SCOPE_INSUFFICIENT = "SCOPE_INSUFFICIENT"
    RESOURCE_MISMATCH = "RESOURCE_MISMATCH"


class IdempotencyState(str, Enum):
    IN_FLIGHT = "IN_FLIGHT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"  # 首次业务执行抛错：键不占用，等待者可 CAS 晋升后重试


class IdempotencyConflict(str, Enum):
    """幂等冲突的可区分原因（与授权拒绝、投递失败严格区分）。"""
    KEY_IN_FLIGHT = "IDEMPOTENCY_KEY_IN_FLIGHT"
    PAYLOAD_MISMATCH = "IDEMPOTENCY_PAYLOAD_MISMATCH"


class DeliveryStatus(str, Enum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class DeliveryFailure(str, Enum):
    """事件投递失败的可区分原因。"""
    SUBSCRIBER_NOT_FOUND = "SUBSCRIBER_NOT_FOUND"
    SUBSCRIBER_INACTIVE = "SUBSCRIBER_INACTIVE"
    TRANSPORT_ERROR = "TRANSPORT_ERROR"
    NON_SUCCESS_STATUS = "NON_SUCCESS_STATUS"
    SIGNATURE_REJECTED = "SIGNATURE_REJECTED"
    MAX_ATTEMPTS_EXHAUSTED = "MAX_ATTEMPTS_EXHAUSTED"


@dataclass
class Partner:
    partner_id: str
    name: str
    active: bool = True


@dataclass
class Consent:
    """客户对某个合作方的同意授权。

    scopes: 授权范围集合，如 {"accounts.read", "payments.create"}
    account_ids: 资源归属白名单，仅这些账户可被该同意访问/发起支付
    expires_at: 有效期截止时刻（含），状态为 AUTHORISED 且未过期才放行
    """
    consent_id: str
    partner_id: str
    customer_id: str
    scopes: frozenset[str]
    account_ids: frozenset[str]
    status: ConsentStatus
    expires_at: datetime
    created_at: datetime = field(default_factory=utcnow)
    revoked_at: datetime | None = None
    revoke_reason: str | None = None

    def is_expired(self, now: datetime | None = None) -> bool:
        return (now or utcnow()) >= self.expires_at


@dataclass
class Account:
    account_id: str
    customer_id: str
    currency: str
    balance_minor: int  # 最小货币单位（分）


@dataclass
class Payment:
    payment_id: str
    consent_id: str
    account_id: str
    amount_minor: int
    currency: str
    idempotency_key: str
    status: str = "COMPLETED"
    created_at: datetime = field(default_factory=utcnow)


@dataclass
class Subscription:
    """事件订阅：合作方针对一组事件类型注册回调地址与 HMAC 签名密钥。"""
    subscription_id: str
    partner_id: str
    event_types: frozenset[str]
    callback_url: str
    secret: str
    active: bool = True


@dataclass
class DeliveryAttempt:
    attempt: int
    at: datetime
    status: DeliveryStatus
    http_status: int | None = None
    failure_reason: str | None = None


@dataclass
class EventRecord:
    event_id: str
    event_type: str
    payload: dict[str, Any]
    subscription_id: str = ""
    partner_id: str = ""
    callback_url: str = ""
    occurred_at: datetime = field(default_factory=utcnow)
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempts: int = 0
    max_attempts: int = 5
    next_attempt_at: datetime | None = None
    last_failure_reason: str | None = None
    history: list[DeliveryAttempt] = field(default_factory=list)
    delivered_at: datetime | None = None
