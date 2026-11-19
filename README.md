# replica-read-routing-planner
根据复制延迟、一致性要求和负载规划数据库读路由

## 问题

业务写完立刻读时，读请求可能落到复制延迟较高的副本上，出现"刚保存的数据不存在"；
全部强制走主库又会把读流量集中到主库。本模块在不打爆主库的前提下，
按一致性要求把读请求规划到主库或合适的副本。

## 运行

要求 Node.js >= 24.16.0，无第三方依赖。

```bash
node --test
```

## 用法

```js
import { createRouter } from './src/router.js';

const router = createRouter({ maxWaitMs: 250 });

const state = {
  now: 1_000_000,            // 快照时间戳（毫秒），由调用方传入以保证确定性
  clientRegion: 'east',      // 客户端所在区域
  highWatermark: 2000,       // 主库当前已应用的复制位置
  instances: [
    { id: 'p1', role: 'primary', region: 'east', position: 2000,
      positionUpdatedAt: 1_000_000, replicationRatePerMs: Infinity,
      paused: false, healthy: true, load: 0.4, latencyMs: 20, capacity: 50 },
    { id: 'r1', role: 'replica', region: 'east', position: 1800,
      positionUpdatedAt: 1_000_000, replicationRatePerMs: 1,
      paused: false, healthy: true, load: 0.2, latencyMs: 5, capacity: 100 },
  ],
};

const { decisions, retries } = router.plan(state, [
  { key: 'w1', type: 'write', sessionId: 's1', position: 1900 },
  { key: 'r1', type: 'read', sessionId: 's1', consistency: 'monotonic' },
]);
```

每个决策包含：`target`（目标实例）、`status`（`routed` / `wait` / `retry` /
`duplicate`）、`reason`（路由原因）、`requiredPosition`、`estimatedWaitMs`
（预计等待复制追平的毫秒数）；需要重试的请求带 `retryAfterMs`，并汇总在
返回值的 `retries` 中。

## 复制位置的含义

- **复制位置（position）**：单调递增的日志位点（如 LSN / binlog 位点 /
  Raft index），表示该实例已经应用到的写日志位置。位置越大数据越新。
- **高水位（highWatermark）**：主库当前已应用的位置，即"最新数据"的基准。
- **会话下限（session floor）**：路由器为每个 `sessionId` 维护的、该会话
  已经见过的最大位置。它只增不减，是单调读的保证来源：同一会话后续的读
  只会被路由到 `position >= 会话下限` 的实例。写请求成功路由后用其
  `position` 推进下限；读请求按实际满足的位置推进下限。
- **复制速率（replicationRatePerMs）**：副本应用日志的速度（位置/毫秒），
  用于估算等待时间：`wait = (所需位置 - 副本位置) / 速率`。速率为 0 表示
  追不上，等待时间视为无穷大。

## 路由假设

- **写永远走主库**；主库不健康时写请求进入重试，绝不发往副本。
- **一致性级别**：
  - `strong`：所需位置 = 高水位，只有已追平的实例（通常只有主库）可用；
  - `monotonic`（默认）：所需位置 = max(请求自带 `lastWritePosition`,
    会话下限)；
  - `eventual`：仍需满足会话下限（单调读对同一会话始终成立），
    除此之外任何健康副本都可用。
- **不静默降级**：所需位置无法满足时，若某实例能在 `maxWaitMs` 内追平，
  决策为 `wait` 并给出 `estimatedWaitMs`；否则明确返回 `retry`，
  绝不把请求发给数据过旧的实例。
- **副本可用性**：不健康、复制暂停（`paused`）、位置心跳超过
  `maxStalenessMs`、负载达到 `maxLoad` 的副本一律摘除。
- **区域亲和**：同区域实例优先；跨区域有固定惩罚 `regionPenaltyMs`，
  本区域无可用实例时自动故障切换到跨区域实例，并在 `reason` 中标注
  `cross-region-failover`。
- **评分**：`score = 延迟 + 跨区域惩罚 + 负载 × loadWeightMs + 等待时间`，
  取最低分；平分按实例 ID 字典序打破，保证确定性。
- **容量额度**：每个实例的 `capacity` 是单次 `plan` 调用内可承接的请求数，
  按请求顺序消耗；耗尽后溢出到下一候选，全部耗尽则该请求进入重试
  （`capacity-exhausted`）。
- **批量与重复键**：请求按输入顺序处理；相同 `key` 的重复请求复用首次
  决策，标记为 `duplicate`，不重复消耗容量。
- **确定性**：不读系统时钟、不用随机数；相同输入与相同请求顺序必然产生
  相同结果。`now` 由调用方随集群快照传入。
