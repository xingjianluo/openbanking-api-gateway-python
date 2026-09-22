"""事件订阅与异步投递测试。

覆盖：
- 签名生成与篡改识别（改内容/改签名/错密钥/缺前缀）
- 订阅方用共享密钥正确验签（模拟回调接收端）
- 投递失败按指数退避重试，最终成功 -> DELIVERED
- 持续失败达上限 -> DEAD_LETTER，原因链包含 MAX_ATTEMPTS_EXHAUSTED
- 订阅方验签拒绝(401) -> SIGNATURE_REJECTED，立即死信不重试
- 传输异常 -> TRANSPORT_ERROR 重试
- 停用订阅 -> SUBSCRIBER_INACTIVE 死信
"""
from __future__ import annotations

import json
import time

import pytest

from obgateway.events import (
    EVENT_ID_HEADER,
    EVENT_TYPE_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    EventDispatcher,
    canonical_body,
    signing_content,
    sign_payload,
    verify_signature,
)
from obgateway.models import DeliveryFailure, DeliveryStatus
from obgateway.storage import LocalStore

from tests.conftest import FakeTransport


def _make(transport=None, *, keep_inactive: bool = False, **kwargs):
    store = LocalStore()
    if not keep_inactive:
        store.subscriptions.pop("sub-a-inactive", None)
    defaults = {"base_delay": 0.01, "jitter": 0.0, "default_max_attempts": 3}
    defaults.update(kwargs)
    return store, EventDispatcher(store, transport or FakeTransport(), **defaults)


# ---------------- 签名 ----------------

def test_signature_roundtrip_and_tamper_detection():
    body = canonical_body("evt-1", "payment.completed",
                          "2026-09-23T00:00:00+00:00", {"amount": 100})
    content = signing_content("evt-1", "payment.completed",
                              "2026-09-23T00:00:00+00:00", body)
    sig = sign_payload("secret", content)

    assert verify_signature("secret", content, sig)
    assert not verify_signature("secret", content + b"x", sig), "内容被篡改必须验签失败"
    assert not verify_signature("secret", content, sig[:-2] + "00"), "签名被篡改必须失败"
    assert not verify_signature("other-secret", content, sig), "密钥不符必须失败"
    assert not verify_signature("secret", content, "garbage"), "非法签名格式必须失败"
    print("[签名校验] 合法/改内容/改签名/错密钥/非法格式 五种输入判定均符合预期")


def test_subscriber_can_verify_delivery_signature(transport):
    """模拟订阅方：用共享密钥对实际投递的报文与头做验签。"""
    store, dispatcher = _make(transport)
    dispatcher.start()
    records = dispatcher.publish("payment.completed", {"paymentId": "p-1"})
    assert dispatcher.drain(timeout=5)
    dispatcher.stop()

    delivery = transport.deliveries[0]
    headers, raw = delivery["headers"], delivery["body"]
    doc = json.loads(raw)
    # 订阅方按同样规则重组签名内容
    content = signing_content(
        headers[EVENT_ID_HEADER], headers[EVENT_TYPE_HEADER],
        headers[TIMESTAMP_HEADER], raw)
    assert verify_signature("shh-secret-a", content, headers[SIGNATURE_HEADER])
    assert doc["data"]["paymentId"] == "p-1"
    print(f"[订阅方验签] event={headers[EVENT_ID_HEADER]} 验签通过，载荷={doc['data']}")


# ---------------- 投递结果 ----------------

def test_retry_with_backoff_then_success():
    transport = FakeTransport(script=[500, 500, 200])
    store, dispatcher = _make(transport)
    start = time.monotonic()
    dispatcher.start()
    dispatcher.publish("payment.completed", {"paymentId": "p"})
    assert dispatcher.drain(timeout=5), "应在超时前投递成功"
    dispatcher.stop()
    elapsed = time.monotonic() - start

    record = list(store.all_events())[0]
    print(f"[失败重试] 尝试次数={record.attempts} 传输调用={transport.calls} "
          f"总耗时={elapsed:.3f}s 状态={record.status.value}")
    assert record.status is DeliveryStatus.DELIVERED
    assert record.attempts == 3 and transport.calls == 3
    reasons = [h.failure_reason for h in record.history[:-1]]
    assert reasons == ["NON_SUCCESS_STATUS", "NON_SUCCESS_STATUS"]
    # base_delay=0.01：第一次重试等 0.01，第二次等 0.02
    assert elapsed >= 0.03


def test_max_attempts_dead_letter_with_distinguishable_reason():
    transport = FakeTransport(always=503)
    store, dispatcher = _make(transport, default_max_attempts=4)
    dispatcher.start()
    dispatcher.publish("payment.completed", {"paymentId": "p"})
    assert dispatcher.drain(timeout=5)
    dispatcher.stop()

    record = list(store.all_events())[0]
    tail_reasons = [h.failure_reason for h in record.history]
    print(f"[死信] 状态={record.status.value} 尝试={record.attempts} "
          f"末次原因={record.last_failure_reason} 历史={tail_reasons}")
    assert record.status is DeliveryStatus.DEAD_LETTER
    assert record.attempts == 4
    assert record.last_failure_reason == DeliveryFailure.NON_SUCCESS_STATUS.value
    assert tail_reasons[-1] == DeliveryFailure.MAX_ATTEMPTS_EXHAUSTED.value


def test_signature_rejected_no_retry():
    transport = FakeTransport(always=401)
    store, dispatcher = _make(transport)
    dispatcher.start()
    dispatcher.publish("payment.completed", {"paymentId": "p"})
    assert dispatcher.drain(timeout=5)
    dispatcher.stop()

    record = list(store.all_events())[0]
    print(f"[验签拒绝] 状态={record.status.value} 尝试={record.attempts} "
          f"原因={record.last_failure_reason} 传输调用={transport.calls}")
    assert record.status is DeliveryStatus.DEAD_LETTER
    assert record.last_failure_reason == DeliveryFailure.SIGNATURE_REJECTED.value
    assert transport.calls == 1  # 立即死信，绝不重试


def test_transport_error_is_distinguishable_and_retried():
    transport = FakeTransport(exc=ConnectionError("connection refused"))
    store, dispatcher = _make(transport, default_max_attempts=2)
    dispatcher.start()
    dispatcher.publish("payment.completed", {"paymentId": "p"})
    assert dispatcher.drain(timeout=5)
    dispatcher.stop()

    record = list(store.all_events())[0]
    reasons = [h.failure_reason for h in record.history]
    print(f"[传输异常] 状态={record.status.value} 历史={reasons}")
    assert record.status is DeliveryStatus.DEAD_LETTER
    assert DeliveryFailure.TRANSPORT_ERROR.value in reasons
    assert reasons[-1] == DeliveryFailure.MAX_ATTEMPTS_EXHAUSTED.value


def test_subscription_deactivated_after_publish_dead_letter(transport):
    """发布时订阅启用（因此生成投递记录），投递前被停用 -> SUBSCRIBER_INACTIVE 死信。"""
    store, dispatcher = _make(transport)
    # 不启动工作线程：先入队，再停用订阅，最后启动让投递线程看到停用状态
    dispatcher.publish("payment.completed", {"paymentId": "p"})
    store.subscriptions["sub-a-payments"].active = False

    dispatcher.start()
    assert dispatcher.drain(timeout=5)
    dispatcher.stop()

    record = list(store.all_events())[0]
    print(f"[投递前停用] 状态={record.status.value} 原因={record.last_failure_reason}")
    assert record.status is DeliveryStatus.DEAD_LETTER
    assert record.last_failure_reason == DeliveryFailure.SUBSCRIBER_INACTIVE.value
    assert transport.calls == 0  # 停用后不应发起 HTTP 调用


def test_failure_categories_are_distinguishable():
    """三类失败原因码集合互不相交：授权 / 幂等 / 投递。"""
    from obgateway.models import DenyReason, IdempotencyConflict
    authz = {r.value for r in DenyReason}
    idem = {r.value for r in IdempotencyConflict}
    delivery = {r.value for r in DeliveryFailure}
    print(f"[原因可区分] 授权={sorted(authz)}")
    print(f"[原因可区分] 幂等={sorted(idem)}")
    print(f"[原因可区分] 投递={sorted(delivery)}")
    assert not (authz & idem)
    assert not (authz & delivery)
    assert not (idem & delivery)
