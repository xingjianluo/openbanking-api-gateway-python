"""事件订阅与异步投递。

交付保证：至少一次（at-least-once）。每个匹配的订阅生成一条独立投递记录，
后台工作线程串行重试，指数退避；达到最大次数进入死信（DEAD_LETTER），
状态、每次尝试的 HTTP 状态与失败原因都落账可查。

签名：投递体为规范化 JSON，HMAC-SHA256 签名内容为
    f"{event_id}.{event_type}.{timestamp}." + body_bytes
通过 X-OBG-Signature 头下发（格式 sha256=<hex>），订阅方可用共享密钥验签，
任何字节被篡改都会导致验签失败（hmac.compare_digest 常量时间比较）。

三类失败原因互不混用：
- 授权拒绝见 errors.AuthorizationError / models.DenyReason
- 幂等冲突见 errors.IdempotencyConflictError / models.IdempotencyConflict
- 投递失败见 models.DeliveryFailure（本模块）
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import random
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from heapq import heappop, heappush
from typing import Any, Protocol

from .models import (
    DeliveryAttempt,
    DeliveryFailure,
    DeliveryStatus,
    EventRecord,
    utcnow,
)
from .storage import LocalStore
from datetime import datetime

logger = logging.getLogger("obgateway.events")

SIGNATURE_HEADER = "X-OBG-Signature"
EVENT_ID_HEADER = "X-OBG-Event-Id"
EVENT_TYPE_HEADER = "X-OBG-Event-Type"
TIMESTAMP_HEADER = "X-OBG-Timestamp"
# 订阅方验签失败时返回 401/403，投递器据此判定为 SIGNATURE_REJECTED（不重试）
SIGNATURE_REJECT_STATUSES = frozenset({401, 403})


def canonical_body(event_id: str, event_type: str, timestamp: str,
                   payload: dict[str, Any]) -> bytes:
    """投递体的规范化字节序列，签名与发送都基于同一串字节。"""
    doc = {
        "eventId": event_id,
        "eventType": event_type,
        "timestamp": timestamp,
        "data": payload,
    }
    return json.dumps(doc, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")


def signing_content(event_id: str, event_type: str, timestamp: str,
                    body: bytes) -> bytes:
    return f"{event_id}.{event_type}.{timestamp}.".encode("utf-8") + body


def sign_payload(secret: str, content: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), content, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_signature(secret: str, content: bytes, signature: str) -> bool:
    """校验 X-OBG-Signature 头；签名或内容被篡改均返回 False。"""
    if not signature or not signature.startswith("sha256="):
        return False
    expected = sign_payload(secret, content)
    return hmac.compare_digest(expected, signature)


@dataclass
class TransportResponse:
    status_code: int


class Transport(Protocol):
    def post(self, url: str, headers: dict[str, str], body: bytes) -> TransportResponse: ...


class HttpxTransport:
    """生产用传输层；网络异常向上抛出，由投递器归类为 TRANSPORT_ERROR。"""

    def __init__(self, timeout: float = 5.0) -> None:
        import httpx
        self._client = httpx.Client(timeout=timeout)

    def post(self, url: str, headers: dict[str, str], body: bytes) -> TransportResponse:
        resp = self._client.post(url, headers=headers, content=body)
        return TransportResponse(resp.status_code)


class _Scheduled(tuple):
    """堆元素 (not_before, seq, event_id)，元组天然支持字典序比较。"""
    def __new__(cls, not_before: float, seq: int, event_id: str):
        return tuple.__new__(cls, (not_before, seq, event_id))

    @property
    def not_before(self) -> float:
        return self[0]

    @property
    def seq(self) -> int:
        return self[1]

    @property
    def event_id(self) -> str:
        return self[2]


class EventDispatcher:
    def __init__(
        self,
        store: LocalStore,
        transport: Transport | None = None,
        *,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
        jitter: float = 0.0,
        default_max_attempts: int = 5,
    ) -> None:
        self.store = store
        self.transport: Transport = transport or HttpxTransport()
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.default_max_attempts = default_max_attempts

        self._cond = threading.Condition()
        self._heap: list[_Scheduled] = []
        self._seq = 0
        self._thread: threading.Thread | None = None
        self._stopping = False

    # ---- 发布 ----
    def publish(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        max_attempts: int | None = None,
    ) -> list[EventRecord]:
        """为每个匹配订阅各生成一条投递记录并入队（至少一次）。"""
        records: list[EventRecord] = []
        with self.store.lock:
            # 发布时只投给启用中的订阅；订阅若在发布之后、投递之前被停用，
            # 由 _attempt 判定为 SUBSCRIBER_INACTIVE 进入死信。
            subs = [s for s in self.store.subscriptions_for(event_type) if s.active]
            for sub in subs:
                record = EventRecord(
                    event_id=f"evt-{uuid.uuid4().hex[:16]}",
                    event_type=event_type,
                    payload=dict(payload),
                    subscription_id=sub.subscription_id,
                    partner_id=sub.partner_id,
                    callback_url=sub.callback_url,
                    status=DeliveryStatus.PENDING,
                    max_attempts=max_attempts or self.default_max_attempts,
                )
                self.store.events[record.event_id] = record
                records.append(record)
        for record in records:
            self._enqueue(record.event_id, delay=0.0)
        logger.info("事件发布 type=%s 生成投递记录 %d 条", event_type, len(records))
        return records

    # ---- 生命周期 ----
    def start(self) -> None:
        if self._thread is not None:
            return
        # 以存储中尚未终态的记录为准重建堆（进程重启 / 先发布后启动场景），
        # 避免与 publish() 已入队的堆项重复。
        with self.store.lock:
            pending = [e for e in self.store.events.values()
                       if e.status in (DeliveryStatus.PENDING,
                                       DeliveryStatus.IN_FLIGHT,
                                       DeliveryStatus.FAILED)]
        now_mono = time.monotonic()
        with self._cond:
            self._heap.clear()
            for record in pending:
                not_before = now_mono
                if record.next_attempt_at is not None:
                    delta = (record.next_attempt_at - utcnow()).total_seconds()
                    not_before = now_mono + max(0.0, delta)
                self._push_locked(record.event_id, not_before)
            self._stopping = False
            self._thread = threading.Thread(
                target=self._run, name="obg-event-dispatcher", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        with self._cond:
            self._stopping = True
            self._cond.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def drain(self, timeout: float = 5.0) -> bool:
        """测试辅助：等待所有记录进入终态（DELIVERED / DEAD_LETTER）。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._cond:
                heap_empty = not self._heap
            with self.store.lock:
                all_terminal = all(
                    e.status in (DeliveryStatus.DELIVERED, DeliveryStatus.DEAD_LETTER)
                    for e in self.store.events.values()
                ) and any(True for _ in self.store.events.values())
            if heap_empty and all_terminal:
                return True
            time.sleep(0.01)
        return False

    # ---- 调度 ----
    def _push_locked(self, event_id: str, not_before: float) -> None:
        self._seq += 1
        heappush(self._heap, _Scheduled(not_before, self._seq, event_id))

    def _enqueue(self, event_id: str, delay: float) -> None:
        with self._cond:
            self._push_locked(event_id, time.monotonic() + delay)
            self._cond.notify_all()

    def _run(self) -> None:
        while True:
            with self._cond:
                while not self._heap and not self._stopping:
                    self._cond.wait(timeout=1.0)
                if self._stopping and not self._heap:
                    return
                if not self._heap:
                    continue
                head = self._heap[0]
                wait = head.not_before - time.monotonic()
                if wait > 0:
                    self._cond.wait(timeout=wait)
                    continue
                heappop(self._heap)
            self._attempt(head.event_id)

    def _backoff_delay(self, attempts_so_far: int) -> float:
        delay = min(self.base_delay * (2 ** (attempts_so_far - 1)), self.max_delay)
        if self.jitter:
            delay += random.uniform(0, self.jitter)
        return delay

    def _attempt(self, event_id: str) -> None:
        with self.store.lock:
            record = self.store.events.get(event_id)
            if record is None or record.status in (
                DeliveryStatus.DELIVERED, DeliveryStatus.DEAD_LETTER
            ):
                return
            sub = self.store.subscriptions.get(record.subscription_id)
            if sub is None:
                self._terminal(record, DeliveryFailure.SUBSCRIBER_NOT_FOUND,
                               "订阅已不存在，进入死信")
                return
            if not sub.active:
                self._terminal(record, DeliveryFailure.SUBSCRIBER_INACTIVE,
                               "订阅已停用，进入死信")
                return

            record.status = DeliveryStatus.IN_FLIGHT
            record.attempts += 1
            attempt_no = record.attempts
            timestamp = record.occurred_at.isoformat() if record.attempts == 1 else utcnow().isoformat()
            body = canonical_body(record.event_id, record.event_type,
                                  timestamp, record.payload)
            content = signing_content(record.event_id, record.event_type,
                                      timestamp, body)
            signature = sign_payload(sub.secret, content)
            headers = {
                "Content-Type": "application/json",
                EVENT_ID_HEADER: record.event_id,
                EVENT_TYPE_HEADER: record.event_type,
                TIMESTAMP_HEADER: timestamp,
                SIGNATURE_HEADER: signature,
            }
            url = record.callback_url

        # 网络调用在锁外执行
        try:
            resp = self.transport.post(url, headers, body)
        except Exception as exc:  # 传输层异常（连接拒绝、超时等）
            self._handle_failure(record, DeliveryFailure.TRANSPORT_ERROR,
                                 f"传输异常: {exc!r}", http_status=None)
            return

        if 200 <= resp.status_code < 300:
            with self.store.lock:
                record.status = DeliveryStatus.DELIVERED
                record.delivered_at = utcnow()
                record.last_failure_reason = None
                record.history.append(DeliveryAttempt(
                    attempt=attempt_no, at=utcnow(),
                    status=DeliveryStatus.DELIVERED, http_status=resp.status_code))
            logger.info("事件投递成功 event=%s attempt=%s http=%s",
                        record.event_id, attempt_no, resp.status_code)
            return

        if resp.status_code in SIGNATURE_REJECT_STATUSES:
            # 订阅方验签拒绝：同内容重试无意义，直接死信
            with self.store.lock:
                self._terminal(record, DeliveryFailure.SIGNATURE_REJECTED,
                               f"订阅方拒绝签名 http={resp.status_code}",
                               http_status=resp.status_code)
            return

        self._handle_failure(record, DeliveryFailure.NON_SUCCESS_STATUS,
                             f"订阅方返回非成功状态 http={resp.status_code}",
                             http_status=resp.status_code)

    def _handle_failure(self, record: EventRecord, reason: DeliveryFailure,
                        detail: str, *, http_status: int | None) -> None:
        with self.store.lock:
            record.history.append(DeliveryAttempt(
                attempt=record.attempts, at=utcnow(), status=DeliveryStatus.FAILED,
                http_status=http_status, failure_reason=reason.value))
            record.last_failure_reason = reason.value
            if record.attempts >= record.max_attempts:
                logger.error(
                    "事件投递达到最大次数 event=%s attempts=%s last_reason=%s(%s)",
                    record.event_id, record.attempts, reason.value, detail)
                record.status = DeliveryStatus.DEAD_LETTER
                record.history.append(DeliveryAttempt(
                    attempt=record.attempts, at=utcnow(),
                    status=DeliveryStatus.DEAD_LETTER, http_status=http_status,
                    failure_reason=DeliveryFailure.MAX_ATTEMPTS_EXHAUSTED.value))
                record.next_attempt_at = None
                return

            delay = self._backoff_delay(record.attempts)
            record.status = DeliveryStatus.FAILED
            record.next_attempt_at = utcnow() + timedelta(seconds=delay)
        logger.warning(
            "事件投递失败将重试 event=%s attempt=%s reason=%s detail=%s retry_in=%.3fs",
            record.event_id, record.attempts, reason.value, detail, delay)
        self._enqueue(record.event_id, delay)

    def _terminal(self, record: EventRecord, reason: DeliveryFailure,
                  detail: str, *, http_status: int | None = None) -> None:
        """调用方已持有 store.lock。"""
        record.status = DeliveryStatus.DEAD_LETTER
        record.last_failure_reason = reason.value
        record.next_attempt_at = None
        record.history.append(DeliveryAttempt(
            attempt=record.attempts, at=utcnow(),
            status=DeliveryStatus.DEAD_LETTER, http_status=http_status,
            failure_reason=reason.value))
        logger.error("事件进入死信 event=%s reason=%s detail=%s",
                     record.event_id, reason.value, detail)
