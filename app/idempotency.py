"""幂等与并发控制：同一幂等键只产生一次业务影响。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass

from app.models import IdempotencyRecord, IdemStatus
from app.store import Store


def fingerprint(payload: dict) -> str:
    """对请求内容生成稳定指纹。"""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class IdempotencyConflict(Exception):
    """同一幂等键承载了不同内容。"""

    def __init__(self, key: str) -> None:
        super().__init__(f"idempotency key {key!r} reused with different payload")
        self.key = key


@dataclass
class IdemResult:
    body: dict
    status_code: int
    replayed: bool  # True 表示命中既有记录，未再次执行业务逻辑


def run_idempotent(
    store: Store,
    *,
    key: str,
    payload: dict,
    action: Callable[[], tuple[dict, int]],
) -> IdemResult:
    """在 store.lock 保护下执行幂等语义：

    - 无记录：登记 IN_PROGRESS 后执行 action，成功则落 COMPLETED 与响应；
      action 抛错则清除记录，允许调用方修正后重试。
    - 已有记录且指纹不同：抛 IdempotencyConflict，绝不静默覆盖。
    - 已有记录且指纹相同：直接返回首次响应（replayed=True），不重复执行。
    """
    fp = fingerprint(payload)
    with store.cond:
        while True:
            record = store.idempotency.get(key)
            if record is None:
                # 无记录：登记 IN_PROGRESS，由本调用方执行 action。
                store.idempotency[key] = IdempotencyRecord(
                    key=key, fingerprint=fp, status=IdemStatus.IN_PROGRESS
                )
                break
            if record.fingerprint != fp:
                # 同键不同内容：明确拒绝，绝不静默覆盖。
                raise IdempotencyConflict(key)
            if record.status == IdemStatus.COMPLETED:
                # 重复提交：直接返回首次响应，不重复执行。
                return IdemResult(
                    record.response_body or {}, record.response_status, True
                )
            # 并发重复：等待首个调用方完成后再返回其结果。
            store.cond.wait()
    try:
        body, status_code = action()
    except Exception:
        with store.cond:
            store.idempotency.pop(key, None)
            store.cond.notify_all()
        raise
    with store.cond:
        store.idempotency[key] = IdempotencyRecord(
            key=key,
            fingerprint=fp,
            status=IdemStatus.COMPLETED,
            response_body=body,
            response_status=status_code,
        )
        store.cond.notify_all()
    return IdemResult(body, status_code, False)
