"""幂等与并发控制测试。

重点场景：
- 同一幂等键的并发重复请求只产生一次业务影响，返回完全一致结果
- 串行重放返回一致结果
- 同键不同内容明确拒绝（PAYLOAD_MISMATCH），不重复扣款
- 首次失败后等待者恰好一个晋升重试
- HTTP 级并发支付：余额只扣一次、只生成一个 paymentId、事件只有一条
"""
from __future__ import annotations

import threading

from obgateway.errors import IdempotencyConflictError
from obgateway.idempotency import IdempotencyStore, canonical_payload_hash
from obgateway.models import IdempotencyConflict


def test_canonical_hash_is_content_stable():
    a = {"b": 1, "a": [1, 2]}
    b = {"a": [1, 2], "b": 1}
    assert canonical_payload_hash(a) == canonical_payload_hash(b)
    assert canonical_payload_hash({"v": 1}) != canonical_payload_hash({"v": 2})


def test_serial_replay_returns_identical_result():
    idem = IdempotencyStore()
    calls = 0

    def business():
        nonlocal calls
        calls += 1
        return 201, {"id": f"x-{calls}"}

    r1 = idem.execute("key", {"v": 1}, business)
    r2 = idem.execute("key", {"v": 1}, business)
    print(f"[串行重放] 首次={r1} 重放={r2} 业务执行次数={calls}")
    assert r1 == r2 == (201, {"id": "x-1"})
    assert calls == 1


def test_concurrent_same_key_executes_once():
    idem = IdempotencyStore()
    n = 8
    calls = 0
    call_lock = threading.Lock()
    gate = threading.Event()
    outcomes: list = []

    def business():
        nonlocal calls
        with call_lock:
            calls += 1
        import time; time.sleep(0.15)  # 放大并发窗口
        return 201, {"paymentId": "only-one"}

    def worker():
        gate.wait()
        try:
            outcomes.append(idem.execute("dup-key", {"amount": 100}, business))
        except IdempotencyConflictError as exc:
            outcomes.append(("conflict", exc.reason.value))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()

    print(f"[并发重复] 线程数={n} 业务执行次数={calls} 结果集={set(map(str, outcomes))}")
    assert calls == 1
    assert outcomes == [(201, {"paymentId": "only-one"})] * n


def test_same_key_different_payload_rejected_without_side_effect():
    idem = IdempotencyStore()
    calls = 0

    def business():
        nonlocal calls
        calls += 1
        return 201, {"calls": calls}

    assert idem.execute("k", {"amount": 100}, business) == (201, {"calls": 1})
    for _ in range(3):
        try:
            idem.execute("k", {"amount": 200}, business)
            raise AssertionError("同键异内容必须被拒绝")
        except IdempotencyConflictError as exc:
            assert exc.reason is IdempotencyConflict.PAYLOAD_MISMATCH
    print("[异负荷拒绝] 3 次异负荷请求全部 PAYLOAD_MISMATCH，业务仍只执行 1 次")
    assert calls == 1


def test_concurrent_same_key_different_payload_rejected():
    """并发场景：领导者执行中，异负荷跟随者立即拒绝且不等待。"""
    idem = IdempotencyStore()
    entered = threading.Event()
    release = threading.Event()

    def slow_business():
        entered.set()
        release.wait(timeout=5)
        return 201, {"ok": True}

    result_holder = {}

    def leader():
        idem.execute("race", {"v": 1}, slow_business)

    def mismatcher():
        entered.wait()
        try:
            idem.execute("race", {"v": 2}, lambda: (200, {}))
            result_holder["outcome"] = "not-rejected"
        except IdempotencyConflictError as exc:
            result_holder["outcome"] = exc.reason

    t1 = threading.Thread(target=leader)
    t2 = threading.Thread(target=mismatcher)
    t1.start(); t2.start()
    t2.join(timeout=2)
    release.set()
    t1.join()
    print(f"[并发异负荷] 领导者执行中异负荷请求判定={result_holder.get('outcome')}")
    assert result_holder["outcome"] is IdempotencyConflict.PAYLOAD_MISMATCH


def test_leader_failure_allows_single_follower_retry():
    idem = IdempotencyStore()
    calls = 0
    gate = threading.Event()
    outcomes: list = []

    def flaky():
        nonlocal calls
        calls += 1
        import time; time.sleep(0.05)
        if calls == 1:
            raise RuntimeError("ledger temporarily unavailable")
        return 201, {"paymentId": "recovered"}

    def worker():
        gate.wait()
        try:
            outcomes.append(idem.execute("flaky", {"v": 1}, flaky))
        except RuntimeError as exc:
            outcomes.append(("error", str(exc)))

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()

    print(f"[失败重试] 业务调用次数={calls} 各线程结果={outcomes}")
    assert calls == 2  # 首次失败 + 恰好一次晋升重试
    assert outcomes.count(("error", "ledger temporarily unavailable")) == 1
    assert outcomes.count((201, {"paymentId": "recovered"})) == 2

    # 之后的请求重放成功结果，不再触达业务
    assert idem.execute("flaky", {"v": 1}, flaky) == (201, {"paymentId": "recovered"})
    assert calls == 2


# ---------------- HTTP 级并发支付：余额只扣一次 ----------------

PAYLOAD = {"accountId": "acct-1", "amountMinor": 1000, "currency": "GBP"}


def test_http_concurrent_duplicate_payment_debits_once(client, container):
    n = 6
    gate = threading.Event()
    responses: list = []
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok",
               "Idempotency-Key": "http-concurrent-key"}

    def fire():
        gate.wait()
        responses.append(client.post("/payments", json=PAYLOAD, headers=headers))

    threads = [threading.Thread(target=fire) for _ in range(n)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()

    statuses = [r.status_code for r in responses]
    bodies = [r.json() for r in responses]
    payment_ids = {b["paymentId"] for b in bodies}
    balance = container.store.accounts["acct-1"].balance_minor
    print(f"[HTTP并发] 状态码={statuses} 唯一paymentId={payment_ids} 扣款后余额={balance}")

    assert statuses == [201] * n
    assert len(payment_ids) == 1
    assert balance == 1_000_00 - 1000  # 只扣一次
    assert len(container.store.payments) == 1


def test_http_same_key_different_payload_is_422(client, container):
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok",
               "Idempotency-Key": "http-mismatch-key"}
    r1 = client.post("/payments", json=PAYLOAD, headers=headers)
    r2 = client.post("/payments",
                     json={**PAYLOAD, "amountMinor": 2000}, headers=headers)
    print(f"[HTTP异负荷] 首次={r1.status_code} 异负荷={r2.status_code} "
          f"category={r2.json().get('category')} reason={r2.json().get('reason')}")
    assert r1.status_code == 201
    assert r2.status_code == 422
    body = r2.json()
    assert body["category"] == "IDEMPOTENCY"
    assert body["reason"] == "IDEMPOTENCY_PAYLOAD_MISMATCH"
    # 第二次绝不能产生扣款
    assert container.store.accounts["acct-1"].balance_minor == 1_000_00 - 1000
