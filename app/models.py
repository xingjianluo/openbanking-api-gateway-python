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
    RESOURCE_NOT_FOUND = "resource_not_found"


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


@dataclass
class Event:
    event_id: str
    event_type: str
    payload: dict = field(default_factory=dict)
    created_at: float = 0.0


class DeliveryStatus(str, enum.Enum):
    # 已入队、等待后台 worker 首次处理
    PENDING = "pending"
    # 已被某个 worker 原子领取、正在投递（瞬时态，防止重复投递）
    IN_FLIGHT = "in_flight"
    # 尝试失败、等待退避窗口后再次入队
    RETRYING = "retrying"
    # 已成功投递（终态）
    DELIVERED = "delivered"
    # 重试耗尽的最终失败（终态，可作为重投入口）
    FAILED = "failed"
    # 订阅本身无效（回调地址非法等），重试无意义（终态）
    INVALID_SUBSCRIPTION = "invalid_subscription"
    # 订阅方明确拒绝（4xx），重试无意义（终态）
    REJECTED = "rejected"
    # 事件没有任何匹配订阅者（只出现在事件级查询中）
    NO_SUBSCRIBER = "no_subscriber"


class DeliveryFailureKind(str, enum.Enum):
    """投递失败的可区分分类（写入 Delivery.failure_kind）。"""

    SUBSCRIBER_REJECTED = "subscriber_rejected"  # 订阅方返回 4xx
    NETWORK_UNREACHABLE = "network_unreachable"  # 连接/超时等传输错误
    INVALID_SUBSCRIPTION = "invalid_subscription"  # 回调地址非法
    SERVER_ERROR = "server_error"  # 订阅方 5xx，按可重试错误处理
    UNKNOWN = "unknown"


@dataclass
class Delivery:
    delivery_id: str
    event_id: str
    subscription_id: str
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempts: int = 0
    last_error: str | None = None
    failure_kind: DeliveryFailureKind | None = None
    # 状态流转时间戳
    created_at: float = 0.0
    updated_at: float = 0.0
    # 退避后允许再次尝试的最早时间（仅 RETRYING 有意义）
    next_attempt_at: float = 0.0
    # IN_FLIGHT 租约到期时间：worker 崩溃时扫描器据此回收，超时投递可能重复，
    # 由订阅方按 event_id 去重（至少一次语义）
    lease_expires_at: float = 0.0
    # 重投时指向最初那条失败记录，形成可追踪的重投链
    origin_delivery_id: str | None = None
