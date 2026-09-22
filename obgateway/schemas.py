"""HTTP 请求/响应模型（Pydantic）。

合作方身份与同意通过请求头承载：
  X-Partner-Id: 合作方标识
  X-Consent-Id: 客户同意标识
  Idempotency-Key: 变更类请求的幂等键（支付必填）
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class PaymentCreateRequest(BaseModel):
    accountId: str = Field(min_length=1)
    amountMinor: int = Field(gt=0, description="最小货币单位（分），必须为正整数")
    currency: str = Field(min_length=3, max_length=3)


class ErrorBody(BaseModel):
    """统一错误体：category 区分三大类，reason 给出可区分原因码。"""
    error: str
    category: str          # AUTHORIZATION | IDEMPOTENCY | DELIVERY | VALIDATION
    reason: str
    message: str


class AccountResponse(BaseModel):
    accountId: str
    customerId: str
    currency: str
    balanceMinor: int


class PaymentResponse(BaseModel):
    paymentId: str
    consentId: str
    accountId: str
    amountMinor: int
    currency: str
    status: str
    createdAt: str


class ConsentView(BaseModel):
    consentId: str
    partnerId: str
    customerId: str
    scopes: list[str]
    accountIds: list[str]
    status: str
    expiresAt: str
    revokedAt: str | None = None
    revokeReason: str | None = None


class EventView(BaseModel):
    eventId: str
    eventType: str
    subscriptionId: str
    partnerId: str
    status: str
    attempts: int
    maxAttempts: int
    lastFailureReason: str | None = None
    deliveredAt: str | None = None
    history: list[dict]
