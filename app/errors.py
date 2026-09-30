"""业务错误：把可预期的拒绝显式化，避免服务端 500 或静默放行。"""
from __future__ import annotations


class BusinessError(Exception):
    """业务拒绝基类：携带 HTTP 状态码与稳定的错误码。"""

    status_code: int = 400
    error_code: str = "business_error"

    def __init__(self, detail: str | None = None) -> None:
        super().__init__(detail or self.error_code)
        self.detail = detail or self.error_code


class InsufficientFunds(BusinessError):
    status_code = 402
    error_code = "insufficient_funds"


class InvalidAmount(BusinessError):
    status_code = 400
    error_code = "invalid_amount"


class AccountNotFound(BusinessError):
    status_code = 404
    error_code = "account_not_found"


class ConsentNotFound(BusinessError):
    status_code = 404
    error_code = "consent_not_found"


class SubscriptionNotFound(BusinessError):
    status_code = 404
    error_code = "subscription_not_found"


class InvalidSubscription(BusinessError):
    status_code = 400
    error_code = "invalid_subscription"


class DeliveryNotFound(BusinessError):
    status_code = 404
    error_code = "delivery_not_found"


class RedriveNotAllowed(BusinessError):
    """只允许对最终失败的投递发起重投；其他状态明确拒绝。"""

    status_code = 409
    error_code = "redrive_not_allowed"
