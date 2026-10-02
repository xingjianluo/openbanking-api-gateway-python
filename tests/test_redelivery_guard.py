"""重投防护：同一失败记录最多被一次成功重投补救。

覆盖：
- 串行：同一条失败记录重投成功后，再次重投被明确冲突拒绝，订阅方只成功收到一次；
- 并发：同一条失败记录被几乎同时重复发起多次重投，最多只有一次真正生效；
- 可恢复：重投仍然失败时，原记录保持 failed，仍可继续重投直到成功；
- 链式追踪：每条记录当前状态、redelivery_of 来源可查询、可追踪。
"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.events import EventDispatcher, RedeliverConflict
from tests.conftest import FakeSender

logger = logging.getLogger("openbanking.tests.redelivery_guard")

SECRET = "sub-secret-redrive"


def _subscribe(client: TestClient) -> str:
    return client.post(
        "/subscriptions",
        json={
            "partner_id": "partner-1",
            "event_types": ["payment.completed"],
            "callback_url": "https://partner.example/hook",
            "secret": SECRET,
        },
    ).json()["subscription_id"]


def _pay(client: TestClient, seeded: dict, key: str, amount: int = 100):
    return client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": amount},
        headers={**seeded["headers"], "Idempotency-Key": key},
    )


def _make_failed_delivery(client, sender: FakeSender, dispatcher: EventDispatcher, seeded):
    """制造一条重试耗尽的 failed 投递，返回该记录（此时订阅方已被调 3 次）。"""
    sender.always_fail = True
    _subscribe(client)
    resp = _pay(client, seeded, "k-guard")
    assert resp.status_code == 201
    assert dispatcher.wait_idle(5)
    original = client.get("/deliveries?status=failed").json()[0]
    assert original["status"] == "failed"
    assert sender.call_count() == 3
    return original


def test_second_redeliver_after_success_is_conflict(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """同一条失败记录：重投成功后再点一次重投，第二次必须被拒且无任何副作用。"""
    original = _make_failed_delivery(client, sender, dispatcher, seeded)

    # 第一次重投：成功
    sender.always_fail = False
    first = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    logger.info("first redeliver -> %s %s", first.status_code, first.json())
    assert first.status_code == 201
    first_id = first.json()["delivery_id"]
    assert dispatcher.wait_idle(5)
    assert sender.call_count() == 4, "首次重投恰好追加一次成功回调"

    before = {d["delivery_id"]: d for d in client.get("/deliveries").json()}
    logger.info("after first redeliver: %s", list(before.values()))
    assert before[original["delivery_id"]]["status"] == "redelivered"
    assert before[first_id]["status"] == "delivered"

    # 第二次对**同一条原始失败记录**重投：明确冲突，错误码可区分
    second = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    logger.info("second redeliver -> %s %s", second.status_code, second.json())
    assert second.status_code == 409
    body = second.json()
    assert body["error"] == "redeliver_conflict"
    assert body["reason"] == "already_redelivered"

    # 不新增投递记录、不新增回调，订阅方只成功收到一次
    after = client.get("/deliveries").json()
    assert len(after) == 2, "被拒重投不得产生新的投递记录"
    assert dispatcher.wait_idle(5)
    assert sender.call_count() == 4, "被拒重投不得产生任何重复业务副作用"


def test_concurrent_redeliver_only_one_takes_effect(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """同一条失败记录被几乎同时重投 N 次：最多一次 201，且订阅方只成功收到一次。"""
    original = _make_failed_delivery(client, sender, dispatcher, seeded)
    sender.always_fail = False  # 重投会成功

    n = 10
    results: list[tuple[int, dict]] = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(n)  # 尽量让所有请求在同一时刻发出

    def fire() -> None:
        barrier.wait()
        resp = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
        with results_lock:
            results.append((resp.status_code, resp.json()))

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(lambda _: fire(), range(n)))

    accepted = [b for code, b in results if code == 201]
    rejected = [(code, b) for code, b in results if code != 201]
    logger.info(
        "concurrent redelivers n=%d accepted=%d rejected=%d",
        n, len(accepted), len(rejected),
    )
    for code, body in rejected:
        logger.info("rejected concurrent redeliver: %s %s", code, body)

    assert len(accepted) == 1, "并发下最多只能有一次重投真正生效"
    assert len(rejected) == n - 1
    for code, body in rejected:
        assert code == 409
        assert body["error"] == "redeliver_conflict"
        assert body["reason"] in ("redelivery_in_progress", "already_redelivered")

    assert dispatcher.wait_idle(5)
    deliveries = client.get("/deliveries").json()
    redrives = [d for d in deliveries if d["redelivery_of"] == original["delivery_id"]]
    statuses = sorted(d["status"] for d in redrives)
    logger.info("redelivery records after contention: %s", statuses)
    assert len(redrives) == 1, "只登记出一条重投记录"
    assert redrives[0]["status"] == "delivered"
    assert {d["delivery_id"] for d in deliveries if d["status"] == "delivered"} == {
        redrives[0]["delivery_id"]
    }
    assert sender.call_count() == 4, "并发抢跑不得多造出一次成功投递（3 失败 + 1 成功）"

    # 收敛后串行再点一次：仍是 already_redelivered
    tail = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    logger.info("serial redeliver after contention -> %s %s", tail.status_code, tail.json())
    assert tail.status_code == 409
    assert tail.json()["reason"] == "already_redelivered"
    assert sender.call_count() == 4


def test_dispatcher_level_concurrent_redeliver_raises_distinct_conflict(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """直接对 dispatcher 并发调用：失败者抛可区分的 RedeliverConflict，不产生重复投递。"""
    original = _make_failed_delivery(client, sender, dispatcher, seeded)
    sender.always_fail = False
    sender.delay = 0.3  # 让唯一一条重投在途时间足够长，其余请求必然撞上在途冲突

    n = 6
    outcomes: list[object] = []
    outcomes_lock = threading.Lock()
    barrier = threading.Barrier(n)

    def fire() -> None:
        barrier.wait()
        try:
            d = dispatcher.redeliver(original["delivery_id"])
            with outcomes_lock:
                outcomes.append(("ok", d.delivery_id))
        except RedeliverConflict as exc:
            with outcomes_lock:
                outcomes.append(("conflict", exc.reason))

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(lambda _: fire(), range(n)))

    oks = [o for o in outcomes if o[0] == "ok"]
    conflicts = [o[1] for o in outcomes if o[0] == "conflict"]
    logger.info("dispatcher-level outcomes: oks=%d conflicts=%s", len(oks), conflicts)
    assert len(oks) == 1
    assert len(conflicts) == n - 1
    assert all(reason == "redelivery_in_progress" for reason in conflicts), (
        "并发撞在途重投时，理由必须是 redelivery_in_progress"
    )
    assert dispatcher.wait_idle(10)
    assert sender.call_count() == 4


def test_concurrent_redeliver_of_original_and_failed_child_only_one_takes_effect(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """跨链并发：首次重投失败后，并发地对原记录与失败子记录分别重投，只生效一次。"""
    original = _make_failed_delivery(client, sender, dispatcher, seeded)

    # 第一次重投仍失败 -> 原记录与子记录均为 failed（同一棵重投树的两支起点）
    r1 = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    r1_id = r1.json()["delivery_id"]
    assert dispatcher.wait_idle(5)
    state = {d["delivery_id"]: d for d in client.get("/deliveries").json()}
    assert state[original["delivery_id"]]["status"] == "failed"
    assert state[r1_id]["status"] == "failed"
    assert sender.call_count() == 6

    # 同时对"原记录"和"失败子记录"两个不同目标发起重投，二者都满足各自的
    # 本地前置条件；防护必须识别出它们属于同一棵重投树，只放一个生效
    sender.always_fail = False
    targets = [original["delivery_id"], r1_id]
    outcomes: list[tuple[int, dict]] = []
    outcomes_lock = threading.Lock()
    barrier = threading.Barrier(2)

    def fire(target: str) -> None:
        barrier.wait()
        resp = client.post(f"/deliveries/{target}/redeliver")
        with outcomes_lock:
            outcomes.append((resp.status_code, resp.json()))

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(fire, targets))

    accepted = [b for code, b in outcomes if code == 201]
    rejected = [(code, b) for code, b in outcomes if code != 201]
    logger.info(
        "cross-tree concurrent redeliver: accepted=%d rejected=%s",
        len(accepted), [(c, b) for c, b in rejected],
    )
    assert len(accepted) == 1, "跨链并发下同一事件仍只能被补救一次"
    assert len(rejected) == 1
    assert rejected[0][0] == 409
    assert rejected[0][1]["error"] == "redeliver_conflict"
    # 获胜方可能仍在途（in_progress），也可能已先一步投递成功（already_*）
    assert rejected[0][1]["reason"] in (
        "redelivery_in_progress",
        "already_redelivered",
    )

    assert dispatcher.wait_idle(5)
    final = client.get("/deliveries").json()
    logger.info("cross-tree final deliveries: %s", final)
    assert sum(1 for d in final if d["status"] == "delivered") == 1
    assert all(
        d["status"] in ("delivered", "redelivered") for d in final
    ), "整棵树收敛，不残留可再投的 failed 记录"
    assert sender.call_count() == 7, "跨链抢跑也不得多造出一次成功投递"


def test_failed_redelivery_keeps_record_retryable_until_success(
    client: TestClient, sender: FakeSender, dispatcher: EventDispatcher, seeded: dict
) -> None:
    """重投仍然失败：原记录保持 failed 且可继续重投；最终成功后整条链转为 redelivered。"""
    original = _make_failed_delivery(client, sender, dispatcher, seeded)

    # 第一次重投仍失败（再追加 3 次失败回调）
    assert sender.call_count() == 3
    r1 = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    assert r1.status_code == 201
    r1_id = r1.json()["delivery_id"]
    assert dispatcher.wait_idle(5)
    assert sender.call_count() == 6
    state1 = {d["delivery_id"]: d for d in client.get("/deliveries").json()}
    logger.info("after failed redelivery: %s", list(state1.values()))
    assert state1[original["delivery_id"]]["status"] == "failed", "重投失败不锁定原记录"
    assert state1[r1_id]["status"] == "failed"
    assert state1[r1_id]["redelivery_of"] == original["delivery_id"]

    # 原记录仍允许继续重投（不误伤正常的再次重投）
    sender.always_fail = False  # 这次让它成功
    again = client.post(f"/deliveries/{original['delivery_id']}/redeliver")
    logger.info("retry after failed redelivery -> %s %s", again.status_code, again.json())
    assert again.status_code == 201
    r2_id = again.json()["delivery_id"]
    assert dispatcher.wait_idle(5)
    assert sender.call_count() == 7

    state2 = {d["delivery_id"]: d for d in client.get("/deliveries").json()}
    logger.info("after eventual success: %s", list(state2.values()))
    assert state2[r2_id]["status"] == "delivered"
    assert state2[r2_id]["redelivery_of"] == original["delivery_id"]
    assert state2[original["delivery_id"]]["status"] == "redelivered"
    assert state2[r1_id]["status"] == "redelivered", "失败过的重投链也被成功结果补救"
    # 历史失败痕迹保留，状态可追踪
    assert state2[original["delivery_id"]]["failure_kind"] == "callback_unreachable"
    assert state2[r1_id]["attempts"] == 3

    # 此后任何一条再重投都被拒
    for target, label in (
        (original["delivery_id"], "original"),
        (r1_id, "failed-then-redelivered child"),
        (r2_id, "delivered child"),
    ):
        resp = client.post(f"/deliveries/{target}/redeliver")
        logger.info("redeliver %s -> %s %s", label, resp.status_code, resp.json())
        assert resp.status_code == 409
    assert sender.call_count() == 7, "最终收敛后再无任何回调"
