# Open Banking API Gateway（Python / FastAPI）

面向合作方的开放银行接口集成层。用**本地内存账本与本地存储**模拟合作方、
同意授权与事件订阅，使授权校验、幂等处理与事件投递在并发和异常链路下
都能得到一致且可解释的结论。

## 结构

```
app/
  models.py       领域模型：Consent / Account / LedgerEntry / IdempotencyRecord / Subscription / Delivery
  store.py        线程安全的内存存储（RLock + Condition）
  consent.py      同意授权校验（所有受保护入口共用一个判定函数）
  idempotency.py  幂等执行器（内容指纹 + 并发等待）
  events.py       事件分发器（至少一次投递、重试退避、HMAC 签名）
  main.py         FastAPI 入口与路由
tests/            pytest 测试（日志打印关键输入与判定依据）
```

## 同意模型

- `Consent` 属于某个合作方（`partner_id`）与某个客户（`customer_id`），
  携带授权范围集合 `scopes` 与过期时间 `expires_at`，状态为
  `active / revoked / expired`。
- 受保护入口通过请求头 `X-Partner-Id` 与 `X-Consent-Id` 定位同意，
  所有入口共用 `app.consent.check_consent`，判定顺序固定，因此同一请求
  在不同入口得到相同结论。
- 拒绝理由可区分（HTTP 403，body 中 `reason` 字段）：

  | reason                 | 含义                             |
  |------------------------|----------------------------------|
  | `consent_not_found`    | 同意不存在或不属于该合作方       |
  | `consent_revoked`      | 同意已被撤销                     |
  | `consent_expired`      | 同意已过期                       |
  | `scope_insufficient`   | 授权范围不覆盖入口所需 scope     |
  | `resource_not_owned`   | 目标账户不属于该同意对应的客户   |

## 幂等语义

- 变更类请求（`POST /payments`）必须携带 `Idempotency-Key` 头，缺失返回
  400 `missing_idempotency_key`。
- 同一幂等键 + 相同内容（按规范化 JSON 的 SHA-256 指纹判定）：
  只执行一次业务动作，重复提交返回与首次完全一致的结果，响应头
  `X-Idempotent-Replay: true` 标识重放。
- 同一幂等键 + 不同内容：返回 409 `idempotency_conflict`，
  **绝不静默覆盖或重复扣减**。
- 并发下：首个到达的调用方登记 `IN_PROGRESS` 后执行业务动作；
  并发重复方在条件变量上等待，随后返回首次结果。业务动作抛异常时
  清除记录并唤醒等待方，允许修正后重试。

## 事件投递与重试

- 支付成功发布 `payment.completed` 事件，投递给订阅了该类型的所有订阅。
- **至少一次**：每次投递生成 `Delivery` 记录（可通过 `GET /deliveries`
  观察 `status / attempts / last_error`）。
- **重试与退避**：默认最多 3 次尝试，指数退避 `backoff_base * 2^(n-1)`
  （默认 0.05s 起）。重试耗尽后状态为 `failed` 并保留 `last_error`。
- **签名**：投递头 `X-Signature` 为对规范化正文的 HMAC-SHA256（hex），
  密钥为订阅时登记的 `secret`。订阅方用
  `app.events.verify_signature(secret, body, signature)` 校验；
  正文被篡改或密钥不符时校验失败。
- 三类失败原因互不混淆：策略拒绝（403 + `reason`）、
  幂等冲突（409 + `idempotency_conflict`）、
  投递失败（`Delivery.status=failed` + `last_error`）。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q --log-cli-level=INFO   # 运行测试并查看判定依据日志
uv run uvicorn app.main:app --reload    # 启动服务
```

手工冒烟示例：

```bash
# 预置账户与同意
curl -s -X POST localhost:8000/accounts -H 'content-type: application/json' \
  -d '{"customer_id":"cust-1","balance_cents":100000}'
curl -s -X POST localhost:8000/consents -H 'content-type: application/json' \
  -d '{"partner_id":"partner-1","customer_id":"cust-1","scopes":["accounts:read","payments:write"],"ttl_seconds":3600}'

# 幂等支付（重复执行同一命令，余额只扣一次）
curl -s -X POST localhost:8000/payments \
  -H 'X-Partner-Id: partner-1' -H 'X-Consent-Id: <consent_id>' \
  -H 'Idempotency-Key: pay-1' -H 'content-type: application/json' \
  -d '{"account_id":"<account_id>","amount_cents":500}'

# 观察事件投递状态
curl -s localhost:8000/deliveries
```
