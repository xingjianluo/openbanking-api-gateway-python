"""领域模型：合作方、同意授权、账本、幂等记录、事件订阅。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class ConsentStatus(str, enum.Enum):
    ACTIVE = "active"
    REVOKED = "revoked"
    EXPIRED = "expired"


class DenialReason(str, enum.Enum):
    """可区分的拒绝理由。"""

    CONSENT_NOT_FOUND = "consent_not_found"
    CONSENT_REVOKED = "consent_revoked"
    CONSENT_EXPIRED = "consent_expired"
    SCOPE_INSUFFICIENT = "scope_insufficient"
    RESOURCE_NOT_OWNED = "resource_not_owned"


@dataclass
class Consent:
    consent_id: str
    partner_id: str
    customer_id: str
    scopes: set[str]
    expires_at: float  # epoch 秒
    status: ConsentStatus = ConsentStatus.ACTIVE


@dataclass
class Account:
    account_id: str
    customer_id: str
    balance_cents: int


@dataclass
class LedgerEntry:
    entry_id: str
    account_id: str
    amount_cents: int  # 正为入账，负为扣减
    memo: str


class IdemStatus(str, enum.Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass
class IdempotencyRecord:
    key: str
    fingerprint: str  # 请求内容指纹，用于检测同键不同内容
    status: IdemStatus
    response_body: dict | None = None
    response_status: int = 200


@dataclass
class Subscription:
    subscription_id: str
    partner_id: str
    event_types: set[str]
    callback_url: str
    secret: str  # 用于投递签名


class DeliveryStatus(str, enum.Enum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"  # 重试耗尽


@dataclass
class Delivery:
    delivery_id: str
    event_id: str
    subscription_id: str
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempts: int = 0
    last_error: str | None = None


@dataclass
class Event:
    event_id: str
    event_type: str
    payload: dict = field(default_factory=dict)
