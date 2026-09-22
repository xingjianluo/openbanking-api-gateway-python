"""幂等与并发控制。

语义：
- 同一幂等键 + 相同内容：并发重复请求只有第一次执行业务，其余请求阻塞等待
  首次完成，然后返回与首次完全一致的结果（状态码 + 响应体），不产生第二次
  业务影响。
- 同一幂等键 + 不同内容：无论首次是否仍在执行，都立即拒绝
  （IDEMPOTENCY_PAYLOAD_MISMATCH），绝不静默覆盖或重复扣减。
- 首次业务执行抛异常时标记 FAILED 并通知等待者；失败不占键，相同内容的
  等待者通过记录上的 CAS 恰好晋升一个去重试，其余继续等待其结果。

实现：一把全局锁保护插入与晋升，每个记录自带 Condition；业务函数在锁外执行。
"""
from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from threading import Condition, Lock
from typing import Any

from .errors import IdempotencyConflictError
from .models import IdempotencyConflict, IdempotencyState, utcnow

logger = logging.getLogger("obgateway.idempotency")

# 业务函数返回 (HTTP 状态码, 响应体)
BusinessResult = tuple[int, dict[str, Any]]
BusinessFunc = Callable[[], BusinessResult]


def canonical_payload_hash(payload: Any) -> str:
    """对请求负荷做规范化 JSON 后取 SHA-256，内容不同则哈希不同。"""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class IdempotencyRecord:
    key: str
    payload_hash: str
    state: IdempotencyState
    created_at: datetime = field(default_factory=utcnow)
    completed_at: datetime | None = None
    result: BusinessResult | None = None
    condition: Condition = field(default_factory=Condition)


class IdempotencyStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._records: dict[str, IdempotencyRecord] = {}

    def reset(self) -> None:
        with self._lock:
            self._records.clear()

    def execute(
        self,
        key: str,
        payload: Any,
        business: BusinessFunc,
        *,
        in_flight_timeout: float | None = None,
    ) -> BusinessResult:
        """以幂等方式执行 business，返回 (status_code, body)。"""
        payload_hash = canonical_payload_hash(payload)

        with self._lock:
            record = self._records.get(key)
            if record is None:
                record = IdempotencyRecord(
                    key=key, payload_hash=payload_hash, state=IdempotencyState.IN_FLIGHT
                )
                self._records[key] = record
                is_leader = True
            else:
                is_leader = False

        if is_leader:
            return self._run_attempt(record, business)

        # ---- 跟随者：与首次请求的同一记录协调 ----
        while True:
            with record.condition:
                if record.payload_hash != payload_hash:
                    logger.warning(
                        "幂等冲突(异负荷) key=%s stored=%s incoming=%s",
                        key, record.payload_hash[:12], payload_hash[:12],
                    )
                    raise IdempotencyConflictError(
                        IdempotencyConflict.PAYLOAD_MISMATCH,
                        "同一幂等键已用于不同的请求内容",
                        idempotency_key=key,
                    )

                if record.state is IdempotencyState.COMPLETED:
                    assert record.result is not None
                    logger.info("幂等重放 key=%s status=%s", key, record.result[0])
                    return record.result

                if record.state is IdempotencyState.IN_FLIGHT:
                    notified = record.condition.wait_for(
                        lambda: record.state is not IdempotencyState.IN_FLIGHT,
                        timeout=in_flight_timeout,
                    )
                    if not notified:
                        raise IdempotencyConflictError(
                            IdempotencyConflict.KEY_IN_FLIGHT,
                            "同一幂等键的首次请求仍在处理中，请稍后重试",
                            idempotency_key=key,
                        )
                    continue  # 状态已变化，循环重新判定 COMPLETED / FAILED

                # FAILED：尝试 CAS 晋升，恰好一个等待者成功
                promoted = False
                with self._lock:
                    current = self._records.get(key)
                    if current is record and record.state is IdempotencyState.FAILED:
                        record.state = IdempotencyState.IN_FLIGHT
                        promoted = True
                    elif current is not None and current is not record:
                        record = current  # 已有新尝试，转去跟随它
                if promoted:
                    return self._run_attempt(record, business)
                continue  # 未晋升则循环检查新记录的状态

    def _run_attempt(self, record: IdempotencyRecord, business: BusinessFunc) -> BusinessResult:
        key = record.key
        try:
            # 业务在幂等锁之外执行，避免与账本锁/其他键互相阻塞
            result = business()
        except Exception:
            with record.condition:
                record.state = IdempotencyState.FAILED
                record.result = None
                record.condition.notify_all()
            logger.exception("幂等首次执行业务失败，已释放键等待重试 key=%s", key)
            raise

        with record.condition:
            record.state = IdempotencyState.COMPLETED
            record.result = result
            record.completed_at = utcnow()
            record.condition.notify_all()
        logger.info("幂等首次执行完成 key=%s status=%s", key, result[0])
        return result
