# Open Banking Integration Gateway（开放银行接口集成层）

在 FastAPI 之上实现的一套面向合作方（TPP）的开放银行接口集成层演示：
用**本地账本 + 本地存储**模拟合作方、客户同意授权与事件订阅，使
**授权校验、幂等处理与事件投递**在并发与异常链路下都能得到一致且可解释的结论。

## 能力总览

| 能力 | 入口 | 保证 |
| --- | --- | --- |
| 同意/授权校验 | 所有受保护入口共用 `authz.authorize()` 判定链 | 范围 / 有效期 / 资源归属 / 合作方归属，拒绝原因可区分且跨入口一致 |
| 变更幂等与并发控制 | `POST /payments`（`Idempotency-Key` 头） | 同键并发只产生一次业务影响；同键异负荷明确拒绝；结果可重放 |
| 事件异步投递 | 支付成功后发布 `payment.completed` | 至少一次、指数退避重试、HMAC-SHA256 签名、死信可查 |

三类失败严格区分，互不混用：

- **AUTHORIZATION**（HTTP 403，`reason ∈ DenyReason`）：`CONSENT_NOT_FOUND` /
  `CONSENT_PARTNER_MISMATCH` / `CONSENT_REVOKED` / `CONSENT_EXPIRED` /
  `SCOPE_INSUFFICIENT` / `RESOURCE_MISMATCH` …
- **IDEMPOTENCY**（HTTP 409/422，`reason ∈ IdempotencyConflict`）：
  `IDEMPOTENCY_KEY_IN_FLIGHT` / `IDEMPOTENCY_PAYLOAD_MISMATCH`
- **DELIVERY**（异步，通过 `GET /events` 查询）：`TRANSPORT_ERROR` /
  `NON_SUCCESS_STATUS` / `SIGNATURE_REJECTED` / `SUBSCRIBER_INACTIVE` /
  `MAX_ATTEMPTS_EXHAUSTED` …

## 目录结构

```
obgateway/
  models.py       # 领域模型与原因枚举（DenyReason / IdempotencyConflict / DeliveryFailure）
  errors.py       # 三类业务异常
  storage.py      # 线程安全内存账本 + 种子数据（合作方/同意/账户/订阅）
  authz.py        # 统一授权判定链
  idempotency.py  # 幂等存储：每键条件变量 + 同负荷重放 + 异负荷拒绝
  ledger.py       # 本地业务账本：账户查询与原子扣款
  events.py       # HMAC 签名/验签 + 后台投递器（退避重试/死信）
  schemas.py      # Pydantic 请求/响应模型
  routes.py       # HTTP 路由编排
  app.py          # 应用工厂、生命周期、统一错误映射
tests/            # 41 个测试（授权/幂等并发/事件投递/HTTP 端到端）
docs/DESIGN.md    # 详细设计：同意模型、幂等语义、投递与重试规则
```

## 本地验证

需要 Python ≥ 3.14（见 `pyproject.toml`），依赖通过 `uv` 管理。

```bash
# 1) 安装依赖（pytest 为 dev 依赖，已写入 pyproject.toml）
uv sync

# 2) 运行全部测试（日志打印关键输入与判定依据）
uv run pytest -s -q

# 只跑并发幂等场景
uv run pytest tests/test_idempotency.py -s -q

# 多次运行验证并发稳定性
for i in $(seq 1 10); do uv run pytest -q 2>&1 | tail -1; done

# 3) 启动服务（生产传输层走 httpx，种子回调地址不可达时事件会重试进死信）
uv run uvicorn main:app --reload
# 或交互式 API 文档： http://127.0.0.1:8000/docs
```

### 手工调用示例

```bash
# 账户查询（受保护：accounts.read + 资源归属 + 有效期）
curl -s http://127.0.0.1:8000/accounts/acct-1 \
  -H 'X-Partner-Id: partner-a' -H 'X-Consent-Id: consent-ok'

# 过期同意 -> 403 / AUTHORIZATION / CONSENT_EXPIRED
curl -s -i http://127.0.0.1:8000/accounts/acct-1 \
  -H 'X-Partner-Id: partner-a' -H 'X-Consent-Id: consent-expired'

# 发起支付（变更类：Idempotency-Key 必填）
curl -s -X POST http://127.0.0.1:8000/payments \
  -H 'Content-Type: application/json' \
  -H 'X-Partner-Id: partner-a' -H 'X-Consent-Id: consent-ok' \
  -H 'Idempotency-Key: demo-key-001' \
  -d '{"accountId":"acct-1","amountMinor":1000,"currency":"GBP"}'

# 用同一 Idempotency-Key + 同一 body 重放 -> 与首次完全一致，余额不重复扣减
# 用同一 Key + 不同金额 -> 422 / IDEMPOTENCY / IDEMPOTENCY_PAYLOAD_MISMATCH

# 查询异步事件投递状态（成功/重试历史/死信原因）
curl -s http://127.0.0.1:8000/events -H 'X-Partner-Id: partner-a'
```

## 种子数据（LocalStore 自动初始化）

| ID | 说明 |
| --- | --- |
| `partner-a` / `partner-b` | 两个合作方 |
| `acct-1`（cust-1，余额 1000.00 GBP）/ `acct-2`（cust-2） | 账户 |
| `consent-ok` | partner-a、范围 `accounts.read,payments.create`、资源 `acct-1`、有效期 1 天 |
| `consent-expired` | 已过有效期（状态字段仍为 AUTHORISED，由有效期判定） |
| `consent-revoked` | 已撤销 |
| `consent-readonly` | 仅 `accounts.read`（支付入口必拒 SCOPE_INSUFFICIENT） |
| `consent-other` | 资源白名单为 `acct-2`（跨资源访问必拒 RESOURCE_MISMATCH） |
| `consent-partner-b` | 属于 partner-b（partner-a 使用必拒 CONSENT_PARTNER_MISMATCH） |
| `sub-a-payments` | partner-a 的 `payment.completed` 订阅（启用） |
| `sub-a-inactive` | 停用订阅（发布时跳过；投递前停用则死信） |
