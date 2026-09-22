"""统一错误类型：策略拒绝、幂等冲突、投递失败三类严格分开。

每类错误都携带结构化 reason 码（枚举）与可读 details，
保证日志、HTTP 响应与测试断言都能区分失败原因，不会混为一谈。
"""
from __future__ import annotations

from typing import Any

from .models import DeliveryFailure, DenyReason, IdempotencyConflict


class GatewayError(Exception):
    """所有网关业务错误的基类。"""


class AuthorizationError(GatewayError):
    """同意/授权校验被策略拒绝。"""

    def __init__(self, reason: DenyReason, message: str, **context: Any) -> None:
        self.reason = reason
        self.context = context
        super().__init__(f"{reason.value}: {message}")


class IdempotencyConflictError(GatewayError):
    """幂等键冲突：并发进行中，或同一幂等键承载了不同内容。"""

    def __init__(
        self,
        reason: IdempotencyConflict,
        message: str,
        *,
        idempotency_key: str,
        **context: Any,
    ) -> None:
        self.reason = reason
        self.idempotency_key = idempotency_key
        self.context = context
        super().__init__(f"{reason.value}: {message}")


class DeliveryError(GatewayError):
    """事件投递失败（单次尝试）。"""

    def __init__(self, reason: DeliveryFailure, message: str, **context: Any) -> None:
        self.reason = reason
        self.context = context
        super().__init__(f"{reason.value}: {message}")
