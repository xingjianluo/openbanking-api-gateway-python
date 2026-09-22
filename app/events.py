"""事件订阅与异步投递：至少一次、重试退避、HMAC 签名。"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from collections.abc import Callable

from app.models import Delivery, DeliveryStatus, Event
from app.store import Store


def sign_payload(secret: str, body: bytes) -> str:
    """对投递正文生成 HMAC-SHA256 签名（hex）。"""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify_signature(secret: str, body: bytes, signature: str) -> bool:
    """订阅方校验签名；篡改时返回 False。"""
    expected = sign_payload(secret, body)
    return hmac.compare_digest(expected, signature)


def canonical_body(event_id: str, event_type: str, payload: dict) -> bytes:
    return json.dumps(
        {"event_id": event_id, "event_type": event_type, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


# 传输层：接收 (callback_url, body, headers)，投递失败时抛异常。
Sender = Callable[[str, bytes, dict[str, str]], None]


class EventDispatcher:
    """将事件投递给匹配的订阅，至少一次，带重试与指数退避。

    投递失败（传输异常）只影响 Delivery 状态与 last_error，
    与策略拒绝、幂等冲突完全分离。
    """

    def __init__(
        self,
        store: Store,
        *,
        sender: Sender,
        max_attempts: int = 3,
        backoff_base: float = 0.05,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store = store
        self.sender = sender
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.sleep = sleep

    def publish(self, event_type: str, payload: dict) -> list[Delivery]:
        event = Event(event_id=str(uuid.uuid4()), event_type=event_type, payload=payload)
        with self.store.lock:
            targets = [
                sub
                for sub in self.store.subscriptions.values()
                if event_type in sub.event_types
            ]
        deliveries: list[Delivery] = []
        for sub in targets:
            delivery = Delivery(
                delivery_id=str(uuid.uuid4()),
                event_id=event.event_id,
                subscription_id=sub.subscription_id,
            )
            with self.store.lock:
                self.store.deliveries[delivery.delivery_id] = delivery
            self._deliver(delivery, sub.secret, sub.callback_url, event)
            deliveries.append(delivery)
        return deliveries

    def _deliver(self, delivery: Delivery, secret: str, url: str, event: Event) -> None:
        body = canonical_body(event.event_id, event.event_type, event.payload)
        headers = {"X-Signature": sign_payload(secret, body)}
        for attempt in range(1, self.max_attempts + 1):
            delivery.attempts = attempt
            try:
                self.sender(url, body, headers)
            except Exception as exc:  # 传输失败：记录原因并退避重试
                delivery.last_error = f"{type(exc).__name__}: {exc}"
                if attempt < self.max_attempts:
                    self.sleep(self.backoff_base * (2 ** (attempt - 1)))
                continue
            delivery.status = DeliveryStatus.DELIVERED
            delivery.last_error = None
            return
        delivery.status = DeliveryStatus.FAILED
