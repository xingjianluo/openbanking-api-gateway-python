"""同意/授权校验测试：范围、有效期、资源归属与跨入口一致性。"""
from __future__ import annotations

import pytest

from obgateway.authz import authorize
from obgateway.errors import AuthorizationError
from obgateway.models import DenyReason
from obgateway.storage import LocalStore


def _expect_deny(store, *, scope: str, **kwargs) -> DenyReason:
    try:
        authorize(store, required_scope=scope, **kwargs)
    except AuthorizationError as exc:
        print(f"[判定依据] 输入={kwargs} -> 拒绝原因={exc.reason.value} :: {exc}")
        return exc.reason
    raise AssertionError(f"应当被拒绝: {kwargs}")


@pytest.fixture()
def store():
    return LocalStore()


def test_authorised_consent_passes(store):
    consent = authorize(
        store, partner_id="partner-a", consent_id="consent-ok",
        required_scope="payments.create", account_id="acct-1",
    )
    assert consent.consent_id == "consent-ok"


def test_partner_not_found(store):
    reason = _expect_deny(
        store, partner_id="ghost", consent_id="consent-ok",
        scope="accounts.read", account_id="acct-1")
    assert reason is DenyReason.PARTNER_NOT_FOUND


def test_consent_not_found(store):
    reason = _expect_deny(
        store, partner_id="partner-a", consent_id="no-such-consent",
        scope="accounts.read", account_id="acct-1")
    assert reason is DenyReason.CONSENT_NOT_FOUND


def test_consent_cross_partner_mismatch(store):
    # consent-partner-b 属于 partner-b，partner-a 拿不到使用权
    reason = _expect_deny(
        store, partner_id="partner-a", consent_id="consent-partner-b",
        scope="accounts.read", account_id="acct-1")
    assert reason is DenyReason.CONSENT_PARTNER_MISMATCH


def test_revoked_consent(store):
    reason = _expect_deny(
        store, partner_id="partner-a", consent_id="consent-revoked",
        scope="accounts.read", account_id="acct-1")
    assert reason is DenyReason.CONSENT_REVOKED


def test_expired_consent(store):
    reason = _expect_deny(
        store, partner_id="partner-a", consent_id="consent-expired",
        scope="accounts.read", account_id="acct-1")
    assert reason is DenyReason.CONSENT_EXPIRED


def test_scope_insufficient(store):
    # 只读同意不能发起支付
    reason = _expect_deny(
        store, partner_id="partner-a", consent_id="consent-readonly",
        scope="payments.create", account_id="acct-1")
    assert reason is DenyReason.SCOPE_INSUFFICIENT


def test_resource_mismatch(store):
    # consent-ok 的资源白名单只有 acct-1
    reason = _expect_deny(
        store, partner_id="partner-a", consent_id="consent-ok",
        scope="accounts.read", account_id="acct-2")
    assert reason is DenyReason.RESOURCE_MISMATCH


def test_revoked_takes_precedence_over_expiry(store):
    """同一同意既被撤销又已过期时，撤销优先（客户意图更强）。"""
    from obgateway.models import Consent, ConsentStatus
    from datetime import timedelta
    from obgateway.models import utcnow
    store.consents["both"] = Consent(
        consent_id="both", partner_id="partner-a", customer_id="cust-1",
        scopes=frozenset({"accounts.read"}), account_ids=frozenset({"acct-1"}),
        status=ConsentStatus.REVOKED, expires_at=utcnow() - timedelta(days=1),
        revoke_reason="both")
    reason = _expect_deny(
        store, partner_id="partner-a", consent_id="both",
        scope="accounts.read", account_id="acct-1")
    assert reason is DenyReason.CONSENT_REVOKED


# ---------- 跨入口一致性：同一请求经过账户入口与支付入口，结论必须一致 ----------

@pytest.mark.parametrize("consent_id", [
    ("consent-ok"),
    ("consent-expired"),
    ("consent-revoked"),
    ("consent-partner-b"),
])
def test_decision_consistency_across_entrypoints(client, ok_headers, consent_id):
    """对同一同意，账户查询与（同范围的）支付入口应给出同一授权结论。"""
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": consent_id}
    r_account = client.get("/accounts/acct-1", headers=headers)
    r_payment = client.post(
        "/payments",
        json={"accountId": "acct-1", "amountMinor": 50, "currency": "GBP"},
        headers={**headers, "Idempotency-Key": f"idem-{consent_id}"})

    print(f"[一致性输入] consent={consent_id} "
          f"account入口={r_account.status_code} "
          f"payment入口={r_payment.status_code}")

    if consent_id == "consent-ok":
        assert r_account.status_code == 200
        assert r_payment.status_code == 201
    else:
        # 两个入口都是 403 + 同一 reason 码
        assert r_account.status_code == 403
        assert r_payment.status_code == 403
        ra, rp = r_account.json(), r_payment.json()
        assert ra["category"] == "AUTHORIZATION" == rp["category"]
        assert ra["reason"] == rp["reason"]
        print(f"[一致性依据] 两入口 reason 均为 {ra['reason']}")


def test_readonly_scope_differs_by_entrypoint_but_is_stable(client):
    """只读同意：账户入口放行，支付入口 SCOPE_INSUFFICIENT；重复请求结论稳定。"""
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-readonly"}
    for _ in range(2):
        ra = client.get("/accounts/acct-1", headers=headers)
        rp = client.post(
            "/payments",
            json={"accountId": "acct-1", "amountMinor": 10, "currency": "GBP"},
            headers={**headers, "Idempotency-Key": "ro-key"})
        assert ra.status_code == 200
        assert rp.status_code == 403
        assert rp.json()["reason"] == "SCOPE_INSUFFICIENT"


def test_revoke_api_takes_effect_immediately(client, container):
    """撤销后，原先放行的同意立即被拒。"""
    headers = {"X-Partner-Id": "partner-a", "X-Consent-Id": "consent-ok"}
    assert client.get("/accounts/acct-1", headers=headers).status_code == 200

    r = client.post("/consents/consent-ok/revoke",
                    headers={"X-Partner-Id": "partner-a"})
    assert r.status_code == 200 and r.json()["status"] == "REVOKED"

    r2 = client.get("/accounts/acct-1", headers=headers)
    assert r2.status_code == 403
    assert r2.json()["reason"] == "CONSENT_REVOKED"

    # 重复撤销返回幂等视图
    r3 = client.post("/consents/consent-ok/revoke",
                     headers={"X-Partner-Id": "partner-a"})
    assert r3.json()["alreadyRevoked"] is True
