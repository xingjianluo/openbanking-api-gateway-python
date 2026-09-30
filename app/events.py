"""事件订阅与后台异步投递：至少一次、重试退避、HMAC 签名、失败可重投。

投递与对外请求彻底解耦：publish 只做"登记事件 + 登记 PENDING 投递"，
由后台 worker 线程按 FIFO 领取并执行实际的回调请求。因此合作方回调
慢、超时或不可达都不会拖慢支付等对外接口；积压在本地以 Delivery 记录
的形式存在，可查询、不丢失。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable

from app.models import Delivery, DeliveryStatus, Event, FailureKind
from app.store import Store

logger = logging.getLogger("openbanking.events")


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


class CallbackRejectedError(Exception):
    """订阅方应答了但返回错误状态码（4xx/5xx），与网络不可达相区分。"""

    def __init__(self, status_code: int, detail: str = "") -> None:
        super().__init__(f"callback responded with HTTP {status_code} {detail}".strip())
        self.status_code = status_code


class DeliveryNotFound(Exception):
    """重投目标投递记录不存在。"""


class RedeliverNotAllowed(Exception):
    """只有最终失败（failed）的投递才允许重投。"""

    def __init__(self, status: DeliveryStatus) -> None:
        super().__init__(f"only failed deliveries can be redelivered, got {status.value}")
        self.status = status


def _valid_callback_url(url: str) -> bool:
    return url.startswith(("http://", "https://")) and len(url) > len("https://")


class EventDispatcher:
    """后台异步分发器：至少一次投递、指数退避重试、失败可重投。

    - publish：登记 Event 与每个匹配订阅的 PENDING Delivery 后立即返回，
      不在请求线程内做任何网络调用。
    - worker：后台线程按插入序（FIFO）领取 PENDING 投递，置为 IN_PROGRESS
      后执行重试循环；重试耗尽置 FAILED 并保留 attempts / last_error /
      failure_kind，过程可通过 GET /deliveries 实时观察。
    - redeliver：仅允许 FAILED 投递；生成一条全新的 Delivery
      （redelivery_of 指向原记录），走一次完整可追踪的新投递过程。
      已 DELIVERED 的投递拒绝重投，保证不会对订阅方产生第二次成功投递。
    """

    def __init__(
        self,
        store: Store,
        *,
        sender: Sender,
        max_attempts: int = 3,
        backoff_base: float = 0.05,
        workers: int = 2,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store = store
        self.sender = sender
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.sleep = sleep
        self._wake = threading.Event()
        self._stop = False
        self._threads = [
            threading.Thread(
                target=self._worker_loop,
                name=f"event-dispatcher-{i}",
                daemon=True,  # 随进程退出，不阻塞关闭
            )
            for i in range(workers)
        ]
        for t in self._threads:
            t.start()

    # ---- 请求线程侧：只登记，不投递 ----

    def publish(self, event_type: str, payload: dict) -> list[Delivery]:
        """登记事件与投递记录后立即返回；实际投递由后台 worker 完成。"""
        event = Event(event_id=str(uuid.uuid4()), event_type=event_type, payload=payload)
        with self.store.lock:
            self.store.events[event.event_id] = event
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
                self.store.deliveries[delivery.delivery_id] = delivery
                deliveries.append(delivery)
        if not deliveries:
            logger.info(
                "event %s type=%s matched no subscriptions", event.event_id, event_type
            )
        self._wake.set()
        return deliveries

    def redeliver(self, delivery_id: str) -> Delivery:
        """对最终失败的投递发起一次新的、可追踪的投递。"""
        with self.store.lock:
            original = self.store.deliveries.get(delivery_id)
            if original is None:
                raise DeliveryNotFound(delivery_id)
            if original.status != DeliveryStatus.FAILED:
                raise RedeliverNotAllowed(original.status)
            delivery = Delivery(
                delivery_id=str(uuid.uuid4()),
                event_id=original.event_id,
                subscription_id=original.subscription_id,
                redelivery_of=original.delivery_id,
            )
            self.store.deliveries[delivery.delivery_id] = delivery
        logger.info(
            "redeliver %s -> new delivery %s", original.delivery_id, delivery.delivery_id
        )
        self._wake.set()
        return delivery

    def wait_idle(self, timeout: float = 5.0) -> bool:
        """等待积压投递全部到达终态（测试与运维观察用）；超时返回 False。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.store.lock:
                busy = any(
                    d.status
                    in (DeliveryStatus.PENDING, DeliveryStatus.IN_PROGRESS)
                    for d in self.store.deliveries.values()
                )
            if not busy:
                return True
            time.sleep(0.005)
        return False

    # ---- 后台 worker 侧 ----

    def _worker_loop(self) -> None:
        while not self._stop:
            delivery_id = self._claim_next()
            if delivery_id is None:
                self._wake.wait(timeout=0.5)  # 兜底轮询，防止丢唤醒
                self._wake.clear()
                continue
            try:
                self._process(delivery_id)
            except Exception:  # worker 异常不得拖垮线程，投递留待下轮
                logger.exception("worker failed processing delivery %s", delivery_id)

    def _claim_next(self) -> str | None:
        with self.store.lock:
            for d in self.store.deliveries.values():  # dict 保序 => FIFO
                if d.status == DeliveryStatus.PENDING:
                    d.status = DeliveryStatus.IN_PROGRESS
                    return d.delivery_id
        return None

    def _process(self, delivery_id: str) -> None:
        with self.store.lock:
            delivery = self.store.deliveries[delivery_id]
            event = self.store.events.get(delivery.event_id)
            sub = self.store.subscriptions.get(delivery.subscription_id)
        if event is None or sub is None or not _valid_callback_url(sub.callback_url):
            reason = (
                "event_not_found"
                if event is None
                else "subscription_not_found"
                if sub is None
                else f"invalid_callback_url: {sub.callback_url}"
            )
            self._finish(
                delivery,
                DeliveryStatus.FAILED,
                error=reason,
                kind=FailureKind.SUBSCRIPTION_INVALID,
            )
            return
        self._deliver(delivery, sub.secret, sub.callback_url, event)

    def _deliver(self, delivery: Delivery, secret: str, url: str, event: Event) -> None:
        body = canonical_body(event.event_id, event.event_type, event.payload)
        headers = {"X-Signature": sign_payload(secret, body)}
        for attempt in range(1, self.max_attempts + 1):
            with self.store.lock:
                delivery.attempts = attempt
            try:
                self.sender(url, body, headers)
            except CallbackRejectedError as exc:  # 订阅方明确拒绝
                self._record_failure(delivery, str(exc), FailureKind.CALLBACK_REJECTED)
            except Exception as exc:  # 网络不可达 / 超时等传输失败
                self._record_failure(
                    delivery,
                    f"{type(exc).__name__}: {exc}",
                    FailureKind.CALLBACK_UNREACHABLE,
                )
            else:
                self._finish(delivery, DeliveryStatus.DELIVERED)
                return
            if attempt < self.max_attempts:
                self.sleep(self.backoff_base * (2 ** (attempt - 1)))
        self._finish(
            delivery,
            DeliveryStatus.FAILED,
            error=delivery.last_error,
            kind=delivery.failure_kind,
        )

    def _record_failure(
        self, delivery: Delivery, error: str, kind: FailureKind
    ) -> None:
        with self.store.lock:
            delivery.last_error = error
            delivery.failure_kind = kind

    def _finish(
        self,
        delivery: Delivery,
        status: DeliveryStatus,
        error: str | None = None,
        kind: FailureKind | None = None,
    ) -> None:
        with self.store.lock:
            delivery.status = status
            if status == DeliveryStatus.DELIVERED:
                delivery.last_error = None
                delivery.failure_kind = None
            else:
                delivery.last_error = error
                delivery.failure_kind = kind
