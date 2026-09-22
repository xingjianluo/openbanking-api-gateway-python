"""HTTP 端到端集成测试：受保护入口、幂等支付、事件状态查询与错误分类。"""
from __future__ import annotations

PAYLOAD = {"accountId": "acct-1", "amountMinor": 2000, "currency": "GBP"}


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_get_account_success(client):
    r = client.get("/accounts/acct-1",
                   headers={"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok"})
    assert r.status_code == 200
    assert r.json()["balanceMinor"] == 1_000_00


def test_get_account_denied_for_each_distinct_reason(client):
    # 账户入口（accounts.read）下可区分的拒绝原因
    cases = [
        ("consent-expired", "CONSENT_EXPIRED"),
        ("consent-revoked", "CONSENT_REVOKED"),
        ("consent-partner-b", "CONSENT_PARTNER_MISMATCH"),
    ]
    for consent_id, expected in cases:
        r = client.get("/accounts/acct-1",
                       headers={"X-Partner-Id": "partner-a", "X-Consent-Id": consent_id})
        body = r.json()
        print(f"[入口拒绝] consent={consent_id} http={r.status_code} "
              f"category={body['category']} reason={body['reason']}")
        assert r.status_code == 403
        assert body["category"] == "AUTHORIZATION"
        assert body["reason"] == expected


def test_resource_mismatch_denied(client):
    r = client.get("/accounts/acct-2",
                   headers={"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok"})
    assert r.status_code == 403
    assert r.json()["reason"] == "RESOURCE_MISMATCH"


def test_missing_partner_header_is_validation_error(client):
    # 缺少 X-Partner-Id 属于请求校验问题（VALIDATION），不是授权判定
    r = client.get("/accounts/acct-1", headers={"X-Consent-Id": "consent-ok"})
    assert r.status_code == 422
    assert r.json()["category"] == "VALIDATION"


def test_payment_insufficient_funds_is_replayable(client):
    """业务失败结果（400 余额不足）同样被幂等记录重放，且不扣款。"""
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok",
               "Idempotency-Key": "insufficient-key"}
    big = {**PAYLOAD, "amountMinor": 10_000_00}
    r1 = client.post("/payments", json=big, headers=headers)
    r2 = client.post("/payments", json=big, headers=headers)
    print(f"[业务失败幂等] 首次={r1.status_code}/{r1.json()['reason']} "
          f"重放={r2.status_code}/{r2.json()['reason']}")
    assert r1.status_code == r2.status_code == 400
    assert r1.json()["reason"] == "INSUFFICIENT_FUNDS"
    assert r1.json() == r2.json()


def test_payment_validation_error(client):
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok",
               "Idempotency-Key": "bad-amount"}
    r = client.post("/payments",
                    json={"accountId": "acct-1", "amountMinor": -1, "currency": "GBP"},
                    headers=headers)
    assert r.status_code == 422
    assert r.json()["category"] == "VALIDATION"


def test_payment_publishes_delivered_event_observable_via_api(client):
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok",
               "Idempotency-Key": "event-obs-key"}
    r = client.post("/payments", json=PAYLOAD, headers=headers)
    assert r.status_code == 201

    events = client.get("/events", headers={"X-Partner-Id": "partner-a"}).json()
    assert events["count"] == 1  # 仅投递给启用订阅，停用订阅不生成记录
    event = events["events"][0]
    print(f"[事件可观察] event={event['eventId']} 状态={event['status']} "
          f"尝试={event['attempts']} 末次失败原因={event['lastFailureReason']}")
    assert event["status"] == "DELIVERED"
    assert event["attempts"] == 1
    assert event["lastFailureReason"] is None
    assert event["history"][0]["httpStatus"] == 200
    # 事件载荷关联到该支付


def test_partner_cannot_see_other_partners_events(client):
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok",
               "Idempotency-Key": "isolation-key"}
    client.post("/payments", json=PAYLOAD, headers=headers)
    view = client.get("/events", headers={"X-Partner-Id": "partner-b"}).json()
    assert view["count"] == 0


def test_consent_status_endpoint(client):
    r = client.get("/consents/consent-ok", headers={"X-Partner-Id": "partner-a"})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "AUTHORISED"
    assert "payments.create" in body["scopes"]

    # 跨合作方查看同意被拒
    r2 = client.get("/consents/consent-ok", headers={"X-Partner-Id": "partner-b"})
    assert r2.status_code == 403
    assert r2.json()["reason"] == "CONSENT_PARTNER_MISMATCH"
