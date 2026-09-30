"""事件订阅与后台异步投递。

投递与对外请求彻底解耦：
- ``publish`` 只在本地落库 Event/Delivery（PENDING）并入有界队列，
  入队等待有上限，回调多慢都不会明显拖慢对外接口；
- worker 池在后台执行真正的 HTTP 投递，队列积压有界，服务不会被压垮；
- 扫描线程兜底：入队时队列满而滞留的 PENDING、退避到期的 RETRYING
  都会被重新拾取，因此事件不会丢；
- 重试带指数退避，过程全程可通过 Delivery 记录观察；
- 只有成功才进入 DELIVERED 终态，DELIVERED 永远不会被任何后续处理
  再次投递；重投生成一条新的、可追踪的 Delivery（origin_delivery_id）。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import queue
import threading
import time
import uuid
from collections.abc import Callable
from urllib.parse import urlparse

from app.errors import (
    DeliveryNotFound,
    RedriveNotAllowed,
)
from app.models import (
    Delivery,
    DeliveryFailureKind,
    DeliveryStatus,
    Event,
)
from app.store import Store

# 传输层：接收 (callback_url, body, headers)，投递失败时抛异常。
Sender = Callable[[str, bytes, dict[str, str]], None]


# ---- 传输层可区分失败类型 ----


class DeliveryError(Exception):
    """投递失败基类，failure_kind 决定终态还是可重试。"""

    failure_kind: DeliveryFailureKind = DeliveryFailureKind.UNKNOWN
    retryable: bool = True


class SubscriberRejected(DeliveryError):
    """订阅方返回 4xx：明确拒绝，重试无意义。"""

    failure_kind = DeliveryFailureKind.SUBSCRIBER_REJECTED
    retryable = False


class SubscriberServerError(DeliveryError):
    """订阅方返回 5xx：暂时不可用，可重试。"""

    failure_kind = DeliveryFailureKind.SERVER_ERROR
    retryable = True


class DeliveryNetworkError(DeliveryError):
    """连接失败/超时/不可达：可重试。"""

    failure_kind = DeliveryFailureKind.NETWORK_UNREACHABLE
    retryable = True


class SubscriptionInvalid(DeliveryError):
    """订阅本身无效（如回调地址非法）：重试无意义。"""

    failure_kind = DeliveryFailureKind.INVALID_SUBSCRIPTION
    retryable = False


def classify_error(exc: Exception) -> DeliveryFailureKind:
    """把传输层异常归类为可区分的失败类型。"""
    if isinstance(exc, DeliveryError):
        return exc.failure_kind
    # 常见传输异常（超时、连接拒绝、DNS 失败等均为 OSError 家族）
    if isinstance(exc, TimeoutError | OSError):
        return DeliveryFailureKind.NETWORK_UNREACHABLE
    return DeliveryFailureKind.UNKNOWN


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, DeliveryError):
        return exc.retryable
    return isinstance(exc, TimeoutError | OSError)


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


class EventDispatcher:
    """后台异步投递器：有界队列 + worker 池 + 扫描兜底 + 退避重试。"""

    def __init__(
        self,
        store: Store,
        *,
        sender: Sender,
        max_attempts: int = 3,
        backoff_base: float = 0.05,
        worker_count: int = 4,
        queue_maxsize: int = 256,
        enqueue_timeout: float = 0.05,
        sweep_interval: float = 0.02,
        lease_timeout: float = 60.0,
        clock: Callable[[], float] = time.time,
        auto_start: bool = False,
    ) -> None:
        self.store = store
        self.sender = sender
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.worker_count = worker_count
        self.enqueue_timeout = enqueue_timeout
        self.sweep_interval = sweep_interval
        self.lease_timeout = lease_timeout
        self.clock = clock

        self._queue: queue.Queue[str] = queue.Queue(maxsize=queue_maxsize)
        # 已入队/处理中的 delivery，避免扫描器重复入队
        self._sched_lock = threading.Lock()
        self._scheduled: set[str] = set()
        self._busy = 0
        self._idle_cond = threading.Condition(self._sched_lock)

        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        if auto_start:
            self.start()

    # ---- 生命周期 ----

    def start(self) -> None:
        if self._threads:
            return
        self._stop_event.clear()
        for i in range(self.worker_count):
            t = threading.Thread(
                target=self._worker_loop, name=f"delivery-worker-{i}", daemon=True
            )
            t.start()
            self._threads.append(t)
        sweeper = threading.Thread(
            target=self._sweep_loop, name="delivery-sweeper", daemon=True
        )
        sweeper.start()
        self._threads.append(sweeper)

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=timeout)
        self._threads = []

    # ---- 发布 ----

    def publish(self, event_type: str, payload: dict) -> list[Delivery]:
        """落库事件与投递记录并入队。绝不执行同步 HTTP 调用。"""
        now = self.clock()
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            payload=payload,
            created_at=now,
        )
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
                    status=DeliveryStatus.PENDING,
                    created_at=now,
                    updated_at=now,
                )
                self.store.deliveries[delivery.delivery_id] = delivery
                deliveries.append(delivery)
        for delivery in deliveries:
            self._enqueue(delivery.delivery_id)
        return deliveries

    # ---- 重投 ----

    def redrive(self, delivery_id: str) -> Delivery:
        """对一条最终失败的投递发起一次全新的、可追踪的重投。

        - 目标不存在：DeliveryNotFound；
        - 目标不是 failed 终态：RedriveNotAllowed；
        - 该失败记录已经有过重投成功的后继：RedriveNotAllowed，
          避免订阅方对同一事件收到两次成功投递。
        """
        now = self.clock()
        with self.store.lock:
            origin = self.store.deliveries.get(delivery_id)
            if origin is None:
                raise DeliveryNotFound(f"delivery {delivery_id!r} not found")
            if origin.status != DeliveryStatus.FAILED:
                raise RedriveNotAllowed(
                    f"delivery {delivery_id!r} is {origin.status.value}, "
                    "only failed deliveries can be redriven"
                )
            for d in self.store.deliveries.values():
                if (
                    d.origin_delivery_id == origin.delivery_id
                    and d.status == DeliveryStatus.DELIVERED
                ):
                    raise RedriveNotAllowed(
                        f"delivery {delivery_id!r} already redelivered successfully "
                        f"via {d.delivery_id}"
                    )
            new_delivery = Delivery(
                delivery_id=str(uuid.uuid4()),
                event_id=origin.event_id,
                subscription_id=origin.subscription_id,
                status=DeliveryStatus.PENDING,
                created_at=now,
                updated_at=now,
                origin_delivery_id=origin.delivery_id,
            )
            self.store.deliveries[new_delivery.delivery_id] = new_delivery
        self._enqueue(new_delivery.delivery_id)
        return new_delivery

    # ---- 调度 ----

    def _enqueue(self, delivery_id: str, block_timeout: float | None = None) -> bool:
        """把 delivery 标记为已调度并入队。队列满时返回 False（由扫描器兜底）。"""
        timeout = self.enqueue_timeout if block_timeout is None else block_timeout
        with self._sched_lock:
            if delivery_id in self._scheduled:
                return True
            self._scheduled.add(delivery_id)
        try:
            self._queue.put(delivery_id, block=True, timeout=timeout)
            return True
        except queue.Full:
            # 不丢：记录保持 PENDING，扫描线程会重新拾取。
            with self._sched_lock:
                self._scheduled.discard(delivery_id)
            return False

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                delivery_id = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            with self._sched_lock:
                self._busy += 1
            try:
                self._process(delivery_id)
            finally:
                with self._sched_lock:
                    self._busy -= 1
                    self._scheduled.discard(delivery_id)
                    self._queue.task_done()
                    self._idle_cond.notify_all()

    def _claim(self, delivery_id: str) -> dict | None:
        """原子领取：仅当状态为 pending/retrying（或租约过期的 in_flight）
        时，把状态翻转为 in_flight。返回发送所需上下文；无法领取则返回 None。

        这是“同一事件不会被并发投递两次”的关键：状态翻转在同一把存储锁内
        完成，两个 worker 不可能同时领取同一条记录。
        """
        now = self.clock()
        with self.store.lock:
            delivery = self.store.deliveries.get(delivery_id)
            if delivery is None:
                return None
            claimable = delivery.status in (
                DeliveryStatus.PENDING,
                DeliveryStatus.RETRYING,
            ) or (
                delivery.status == DeliveryStatus.IN_FLIGHT
                and delivery.lease_expires_at <= now
            )
            if not claimable:
                return None
            sub = self.store.subscriptions.get(delivery.subscription_id)
            event = self.store.events.get(delivery.event_id)
            if sub is None:
                delivery.status = DeliveryStatus.INVALID_SUBSCRIPTION
                delivery.failure_kind = DeliveryFailureKind.INVALID_SUBSCRIPTION
                delivery.last_error = "subscription no longer exists"
                delivery.updated_at = now
                return None
            if event is None:
                delivery.status = DeliveryStatus.FAILED
                delivery.last_error = "event no longer exists"
                delivery.updated_at = now
                return None
            if not _valid_callback_url(sub.callback_url):
                delivery.attempts += 1
                delivery.status = DeliveryStatus.INVALID_SUBSCRIPTION
                delivery.failure_kind = DeliveryFailureKind.INVALID_SUBSCRIPTION
                delivery.last_error = (
                    f"invalid callback url: {sub.callback_url}"
                )
                delivery.updated_at = now
                return None
            # 领取成功：占用状态、记录尝试与租约。
            delivery.status = DeliveryStatus.IN_FLIGHT
            delivery.attempts += 1
            delivery.updated_at = now
            delivery.lease_expires_at = now + self.lease_timeout
            body = canonical_body(
                event.event_id, event.event_type, event.payload
            )
            headers = {"X-Signature": sign_payload(sub.secret, body)}
            return {"url": sub.callback_url, "body": body, "headers": headers}

    def _process(self, delivery_id: str) -> None:
        claim = self._claim(delivery_id)
        if claim is None:
            # 已被别的 worker 领取、已是终态或不可发送：绝不重复发送。
            return

        # HTTP 调用在锁外执行，回调多慢都不阻塞其他请求与其他投递。
        try:
            self.sender(claim["url"], claim["body"], claim["headers"])
        except Exception as exc:  # noqa: BLE001 - 任意传输异常都要归类落账，不向上抛
            kind = classify_error(exc)
            now = self.clock()
            with self.store.lock:
                d = self.store.deliveries.get(delivery_id)
                if d is None or d.status != DeliveryStatus.IN_FLIGHT:
                    return
                d.last_error = f"{type(exc).__name__}: {exc}"[:500]
                d.failure_kind = kind
                d.lease_expires_at = 0.0
                d.updated_at = now
                terminal_nonretryable = kind in (
                    DeliveryFailureKind.SUBSCRIBER_REJECTED,
                    DeliveryFailureKind.INVALID_SUBSCRIPTION,
                )
                if terminal_nonretryable:
                    d.status = (
                        DeliveryStatus.REJECTED
                        if kind == DeliveryFailureKind.SUBSCRIBER_REJECTED
                        else DeliveryStatus.INVALID_SUBSCRIPTION
                    )
                elif d.attempts >= self.max_attempts:
                    d.status = DeliveryStatus.FAILED
                else:
                    d.status = DeliveryStatus.RETRYING
                    d.next_attempt_at = now + self.backoff_base * (
                        2 ** (d.attempts - 1)
                    )
        else:
            with self.store.lock:
                d = self.store.deliveries.get(delivery_id)
                if d is None:
                    return
                # 成功是唯一终态来源，之后任何处理都不会再次投递它。
                # 即使租约已过期被别人重复领取，DELIVERED 也不会被覆盖。
                d.status = DeliveryStatus.DELIVERED
                d.last_error = None
                d.failure_kind = None
                d.next_attempt_at = 0.0
                d.lease_expires_at = 0.0
                d.updated_at = self.clock()

    def _sweep_loop(self) -> None:
        """兜底扫描：队列满而滞留的 PENDING、退避到期的 RETRYING 重新入队。"""
        while not self._stop_event.is_set():
            self._sweep_once()
            self._stop_event.wait(self.sweep_interval)

    def _sweep_once(self) -> None:
        now = self.clock()
        due: list[str] = []
        with self._sched_lock:
            scheduled_snapshot = frozenset(self._scheduled)
        with self.store.lock:
            for d in self.store.deliveries.values():
                # 已在队列中或正被 worker 处理（租约未过期）的不再入队，
                # 避免同一投递被重复调度。
                if d.delivery_id in scheduled_snapshot:
                    continue
                if d.status == DeliveryStatus.PENDING or (
                    d.status == DeliveryStatus.RETRYING
                    and d.next_attempt_at <= now
                ):
                    due.append(d.delivery_id)
                elif (
                    d.status == DeliveryStatus.IN_FLIGHT
                    and d.lease_expires_at <= now
                ):
                    # worker 崩溃/长时间卡死的租约回收（可能造成重复投递，
                    # 属至少一次语义，订阅方按 event_id 去重）。
                    due.append(d.delivery_id)
        for delivery_id in due:
            self._enqueue(delivery_id)

    # ---- 观察辅助（测试/运维） ----

    def wait_idle(self, timeout: float = 5.0) -> bool:
        """等待所有投递进入终态（delivered/failed/rejected/invalid_subscription）。"""
        deadline = self.clock() + timeout
        while True:
            with self._sched_lock:
                queued_or_busy = bool(self._scheduled) or self._busy > 0
            if not queued_or_busy and not self._has_pending_work():
                return True
            remaining = deadline - self.clock()
            if remaining <= 0:
                return False
            with self._idle_cond:
                self._idle_cond.wait(timeout=min(0.05, remaining))

    def _has_pending_work(self) -> bool:
        with self.store.lock:
            return any(
                d.status
                in (
                    DeliveryStatus.PENDING,
                    DeliveryStatus.RETRYING,
                    DeliveryStatus.IN_FLIGHT,
                )
                for d in self.store.deliveries.values()
            )


def _valid_callback_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)
