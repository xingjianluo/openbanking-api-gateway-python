"""本地存储：合作方、同意、账户、支付、订阅、事件的内存账本（线程安全）。

所有可变状态的读写都在同一把 RLock 下完成；FastAPI 同步路由跑在线程池，
异步投递器跑在独立线程，锁保证并发下的一致视图。
"""
from __future__ import annotations

from datetime import timedelta
from threading import RLock
from typing import Iterable

from .models import (
    Account,
    Consent,
    ConsentStatus,
    EventRecord,
    Partner,
    Payment,
    Subscription,
    utcnow,
)


class LocalStore:
    def __init__(self, *, seed: bool = True) -> None:
        self._lock = RLock()
        self.partners: dict[str, Partner] = {}
        self.consents: dict[str, Consent] = {}
        self.accounts: dict[str, Account] = {}
        self.payments: dict[str, Payment] = {}
        self.subscriptions: dict[str, Subscription] = {}
        self.events: dict[str, EventRecord] = {}
        if seed:
            self._seed()

    @property
    def lock(self) -> RLock:
        return self._lock

    def reset(self) -> None:
        with self._lock:
            self.partners.clear()
            self.consents.clear()
            self.accounts.clear()
            self.payments.clear()
            self.subscriptions.clear()
            self.events.clear()

    # ---- 同意生命周期 ----
    def revoke_consent(self, consent_id: str, reason: str = "revoked by customer") -> Consent:
        with self._lock:
            consent = self.consents[consent_id]
            consent.status = ConsentStatus.REVOKED
            consent.revoked_at = utcnow()
            consent.revoke_reason = reason
            return consent

    def expire_due_consents(self) -> None:
        """将已过有效期但仍标记为 AUTHORISED 的同意转为 EXPIRED（对账用途）。"""
        now = utcnow()
        with self._lock:
            for consent in self.consents.values():
                if consent.status is ConsentStatus.AUTHORISED and now >= consent.expires_at:
                    consent.status = ConsentStatus.EXPIRED

    def subscriptions_for(self, event_type: str) -> list[Subscription]:
        with self._lock:
            return [s for s in self.subscriptions.values() if event_type in s.event_types]

    # ---- 种子数据 ----
    def _seed(self) -> None:
        now = utcnow()
        p_a = Partner(partner_id="partner-a", name="Acme TPP")
        p_b = Partner(partner_id="partner-b", name="Beta TPP")
        self.partners.update({p_a.partner_id: p_a, p_b.partner_id: p_b})

        acct_1 = Account("acct-1", customer_id="cust-1", currency="GBP", balance_minor=1_000_00)
        acct_2 = Account("acct-2", customer_id="cust-2", currency="GBP", balance_minor=5_000_00)
        self.accounts.update({a.account_id: a for a in (acct_1, acct_2)})

        self.consents["consent-ok"] = Consent(
            consent_id="consent-ok",
            partner_id="partner-a",
            customer_id="cust-1",
            scopes=frozenset({"accounts.read", "payments.create"}),
            account_ids=frozenset({"acct-1"}),
            status=ConsentStatus.AUTHORISED,
            expires_at=now + timedelta(days=1),
        )
        # 已过期：状态仍为 AUTHORISED，由有效期检查判定为 CONSENT_EXPIRED
        self.consents["consent-expired"] = Consent(
            consent_id="consent-expired",
            partner_id="partner-a",
            customer_id="cust-1",
            scopes=frozenset({"accounts.read", "payments.create"}),
            account_ids=frozenset({"acct-1"}),
            status=ConsentStatus.AUTHORISED,
            expires_at=now - timedelta(hours=1),
        )
        # 已撤销
        self.consents["consent-revoked"] = Consent(
            consent_id="consent-revoked",
            partner_id="partner-a",
            customer_id="cust-1",
            scopes=frozenset({"accounts.read", "payments.create"}),
            account_ids=frozenset({"acct-1"}),
            status=ConsentStatus.REVOKED,
            expires_at=now + timedelta(days=1),
            revoked_at=now - timedelta(minutes=10),
            revoke_reason="customer revoked",
        )
        # 范围不足：只有只读权限
        self.consents["consent-readonly"] = Consent(
            consent_id="consent-readonly",
            partner_id="partner-a",
            customer_id="cust-1",
            scopes=frozenset({"accounts.read"}),
            account_ids=frozenset({"acct-1"}),
            status=ConsentStatus.AUTHORISED,
            expires_at=now + timedelta(days=1),
        )
        # 归属不同客户的资源：白名单是 acct-2
        self.consents["consent-other"] = Consent(
            consent_id="consent-other",
            partner_id="partner-a",
            customer_id="cust-2",
            scopes=frozenset({"accounts.read", "payments.create"}),
            account_ids=frozenset({"acct-2"}),
            status=ConsentStatus.AUTHORISED,
            expires_at=now + timedelta(days=1),
        )
        # 属于另一个合作方
        self.consents["consent-partner-b"] = Consent(
            consent_id="consent-partner-b",
            partner_id="partner-b",
            customer_id="cust-1",
            scopes=frozenset({"accounts.read", "payments.create"}),
            account_ids=frozenset({"acct-1"}),
            status=ConsentStatus.AUTHORISED,
            expires_at=now + timedelta(days=1),
        )

        self.subscriptions["sub-a-payments"] = Subscription(
            subscription_id="sub-a-payments",
            partner_id="partner-a",
            event_types=frozenset({"payment.completed"}),
            callback_url="https://tpp-acme.test/callback",
            secret="shh-secret-a",
        )
        self.subscriptions["sub-a-inactive"] = Subscription(
            subscription_id="sub-a-inactive",
            partner_id="partner-a",
            event_types=frozenset({"payment.completed"}),
            callback_url="https://tpp-acme-inactive.test/callback",
            secret="shh-secret-a-inactive",
            active=False,
        )

    def all_events(self) -> Iterable[EventRecord]:
        with self._lock:
            return list(self.events.values())
