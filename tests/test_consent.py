"""同意授权校验：过期、撤销、范围不足、资源归属与跨入口一致性。"""
from __future__ import annotations

import logging
import time

from fastapi.testclient import TestClient

logger = logging.getLogger("openbanking.tests.consent")


def _make_consent(client: TestClient, **overrides) -> dict:
    body = {
        "partner_id": "partner-1",
        "customer_id": "cust-1",
        "scopes": ["accounts:read", "payments:write"],
        "ttl_seconds": 3600,
    }
    body.update(overrides)
    return client.post("/consents", json=body).json()


def _headers(consent_id: str, partner_id: str = "partner-1") -> dict:
    return {"X-Partner-Id": partner_id, "X-Consent-Id": consent_id}


def test_valid_consent_allows_access(client: TestClient, seeded: dict) -> None:
    resp = client.get(f"/accounts/{seeded['account_id']}", headers=seeded["headers"])
    logger.info("valid consent -> status=%s body=%s", resp.status_code, resp.json())
    assert resp.status_code == 200
    assert resp.json()["balance_cents"] == 100_000


def test_expired_consent_denied(client: TestClient, seeded: dict) -> None:
    consent = _make_consent(client, ttl_seconds=0.05)
    time.sleep(0.1)
    resp = client.get(
        f"/accounts/{seeded['account_id']}", headers=_headers(consent["consent_id"])
    )
    logger.info(
        "expired consent_id=%s -> status=%s reason=%s",
        consent["consent_id"], resp.status_code, resp.json().get("reason"),
    )
    assert resp.status_code == 403
    assert resp.json()["reason"] == "consent_expired"


def test_revoked_consent_denied(client: TestClient, seeded: dict) -> None:
    client.post(f"/consents/{seeded['consent_id']}/revoke")
    resp = client.get(f"/accounts/{seeded['account_id']}", headers=seeded["headers"])
    logger.info(
        "revoked consent_id=%s -> status=%s reason=%s",
        seeded["consent_id"], resp.status_code, resp.json().get("reason"),
    )
    assert resp.status_code == 403
    assert resp.json()["reason"] == "consent_revoked"


def test_scope_insufficient_denied(client: TestClient, seeded: dict) -> None:
    consent = _make_consent(client, scopes=["accounts:read"])
    resp = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": 100},
        headers={**_headers(consent["consent_id"]), "Idempotency-Key": "k-scope"},
    )
    logger.info(
        "scope-limited consent (accounts:read only) on /payments -> status=%s reason=%s",
        resp.status_code, resp.json().get("reason"),
    )
    assert resp.status_code == 403
    assert resp.json()["reason"] == "scope_insufficient"


def test_resource_not_owned_denied(client: TestClient, seeded: dict) -> None:
    other = client.post(
        "/accounts", json={"customer_id": "cust-2", "balance_cents": 500}
    ).json()
    resp = client.get(f"/accounts/{other['account_id']}", headers=seeded["headers"])
    logger.info(
        "consent customer=cust-1 accessing account of cust-2 -> status=%s reason=%s",
        resp.status_code, resp.json().get("reason"),
    )
    assert resp.status_code == 403
    assert resp.json()["reason"] == "resource_not_owned"


def test_denial_consistent_across_endpoints(client: TestClient, seeded: dict) -> None:
    """同一被撤销的同意，在读与写两个入口必须得到相同拒绝理由。"""
    client.post(f"/consents/{seeded['consent_id']}/revoke")
    r1 = client.get(f"/accounts/{seeded['account_id']}", headers=seeded["headers"])
    r2 = client.post(
        "/payments",
        json={"account_id": seeded["account_id"], "amount_cents": 100},
        headers={**seeded["headers"], "Idempotency-Key": "k-consistency"},
    )
    logger.info(
        "revoked consent on GET /accounts -> %s; on POST /payments -> %s",
        r1.json(), r2.json(),
    )
    assert r1.status_code == r2.status_code == 403
    assert r1.json()["reason"] == r2.json()["reason"] == "consent_revoked"
