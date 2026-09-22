"""本地业务账本：账户查询与支付扣款。

所有变更在 LocalStore 的同一把锁内完成（余额扣减 + 支付落账），
保证“检查余额 -> 扣款 -> 记账”对并发是原子的，不会超额扣款。
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from .models import Account, Payment
from .storage import LocalStore

logger = logging.getLogger("obgateway.ledger")


class AccountNotFound(Exception):
    pass


class InsufficientFunds(Exception):
    pass


class Ledger:
    def __init__(self, store: LocalStore) -> None:
        self.store = store

    def get_account(self, account_id: str) -> Account:
        with self.store.lock:
            account = self.accounts.get(account_id)
            if account is None:
                raise AccountNotFound(account_id)
            # 返回快照，避免锁外被修改
            return Account(
                account_id=account.account_id,
                customer_id=account.customer_id,
                currency=account.currency,
                balance_minor=account.balance_minor,
            )

    @property
    def accounts(self) -> dict[str, Account]:
        return self.store.accounts

    def create_payment(
        self,
        *,
        consent_id: str,
        account_id: str,
        amount_minor: int,
        currency: str,
        idempotency_key: str,
    ) -> Payment:
        """原子扣款并落账。调用方必须先通过授权与幂等层。"""
        if amount_minor <= 0:
            raise ValueError("amount_minor 必须为正数")
        with self.store.lock:
            account = self.accounts.get(account_id)
            if account is None:
                raise AccountNotFound(account_id)
            if account.currency != currency:
                raise ValueError(
                    f"币种不匹配: 账户 {account.currency} vs 请求 {currency}"
                )
            if account.balance_minor < amount_minor:
                logger.warning(
                    "余额不足 account=%s balance=%s requested=%s",
                    account_id, account.balance_minor, amount_minor,
                )
                raise InsufficientFunds(
                    f"余额 {account.balance_minor} 不足以支付 {amount_minor}"
                )

            account.balance_minor -= amount_minor
            payment = Payment(
                payment_id=f"pmt-{uuid.uuid4().hex[:12]}",
                consent_id=consent_id,
                account_id=account_id,
                amount_minor=amount_minor,
                currency=currency,
                idempotency_key=idempotency_key,
            )
            self.store.payments[payment.payment_id] = payment
            logger.info(
                "支付落账 payment_id=%s account=%s amount=%s balance_after=%s idem=%s",
                payment.payment_id, account_id, amount_minor,
                account.balance_minor, idempotency_key,
            )
            return payment

    def account_view(self, account: Account) -> dict[str, Any]:
        return {
            "accountId": account.account_id,
            "customerId": account.customer_id,
            "currency": account.currency,
            "balanceMinor": account.balance_minor,
        }

    def payment_view(self, payment: Payment) -> dict[str, Any]:
        return {
            "paymentId": payment.payment_id,
            "consentId": payment.consent_id,
            "accountId": payment.account_id,
            "amountMinor": payment.amount_minor,
            "currency": payment.currency,
            "status": payment.status,
            "createdAt": payment.created_at.isoformat(),
        }
