import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRouter, Status } from '../src/router.js';

const NOW = 1_000_000;

function instance(overrides) {
  return {
    id: 'r1',
    role: 'replica',
    region: 'east',
    position: 1000,
    positionUpdatedAt: NOW,
    replicationRatePerMs: 1,
    paused: false,
    healthy: true,
    load: 0.1,
    latencyMs: 5,
    capacity: 100,
    ...overrides,
  };
}

function state(instances, overrides = {}) {
  return {
    now: NOW,
    clientRegion: 'east',
    highWatermark: 2000,
    instances,
    ...overrides,
  };
}

function readReq(overrides) {
  return {
    key: 'k1',
    type: 'read',
    sessionId: 's1',
    lastWritePosition: 0,
    consistency: 'monotonic',
    ...overrides,
  };
}

const primary = (overrides = {}) =>
  instance({ id: 'p1', role: 'primary', position: 2000, latencyMs: 20, ...overrides });

test('写后读：写入推进会话下限，后续读不被路由到滞后副本', () => {
  const router = createRouter({ maxWaitMs: 100 });
  const replica = instance({ id: 'r1', position: 1000, replicationRatePerMs: 0 }); // 永远追不上
  const cluster = state([primary(), replica]);

  const { decisions } = router.plan(cluster, [
    { key: 'w1', type: 'write', sessionId: 's1', position: 1500 },
    readReq({ key: 'r1', sessionId: 's1' }),
  ]);

  assert.equal(decisions[0].target, 'p1');
  assert.equal(decisions[0].reason, 'writes-always-go-to-primary');
  // 副本位置 1000 < 会话下限 1500，且追不上，只能去主库
  assert.equal(decisions[1].target, 'p1');
  assert.equal(decisions[1].requiredPosition, 1500);
  assert.equal(router.getSessionFloor('s1'), 1500);
});

test('写后读：副本能在预算内追上时选择等待而非静默降级', () => {
  const router = createRouter({ maxWaitMs: 250 });
  // 差距 100，速率 1/ms，预计等待 100ms
  const replica = instance({ id: 'r1', position: 1400, latencyMs: 2, load: 0 });
  const cluster = state([primary({ latencyMs: 200 }), replica]);

  const { decisions } = router.plan(cluster, [
    readReq({ key: 'r1', lastWritePosition: 1500, consistency: 'monotonic' }),
  ]);

  assert.equal(decisions[0].status, Status.WAIT);
  assert.equal(decisions[0].target, 'r1');
  assert.equal(decisions[0].estimatedWaitMs, 100);
  assert.match(decisions[0].reason, /wait-for-replication/);
});

test('写后读：等待超出预算且主库不可用时明确失败，不降级', () => {
  const router = createRouter({ maxWaitMs: 50 });
  const replica = instance({ id: 'r1', position: 1000, replicationRatePerMs: 1 });
  const cluster = state([primary({ healthy: false }), replica]);

  const { decisions, retries } = router.plan(cluster, [
    readReq({ key: 'r1', lastWritePosition: 1500, consistency: 'strong' }),
  ]);

  assert.equal(decisions[0].status, Status.RETRY);
  assert.equal(decisions[0].target, null);
  assert.equal(retries.length, 1);
  assert.ok(decisions[0].retryAfterMs > 0);
});

test('跨区域副本：本区域副本不健康时故障切换到跨区域副本', () => {
  const router = createRouter();
  const local = instance({ id: 'r-east', region: 'east', healthy: false });
  const remote = instance({ id: 'r-west', region: 'west', position: 2000, latencyMs: 80 });
  const cluster = state([primary({ capacity: 0 }), local, remote]);

  const { decisions } = router.plan(cluster, [
    readReq({ key: 'r1', consistency: 'eventual' }),
  ]);

  assert.equal(decisions[0].target, 'r-west');
  assert.match(decisions[0].reason, /cross-region-failover/);
});

test('跨区域副本：本区域副本可用时优先本区域', () => {
  const router = createRouter();
  const local = instance({ id: 'r-east', region: 'east', position: 2000, latencyMs: 5 });
  const remote = instance({ id: 'r-west', region: 'west', position: 2000, latencyMs: 1 });
  const cluster = state([primary({ capacity: 0 }), local, remote]);

  const { decisions } = router.plan(cluster, [
    readReq({ key: 'r1', consistency: 'eventual' }),
  ]);

  // 远端延迟虽低，但区域惩罚使本区域胜出
  assert.equal(decisions[0].target, 'r-east');
});

test('复制暂停：暂停的副本被摘除，强一致性读回退到主库', () => {
  const router = createRouter();
  const paused = instance({ id: 'r1', paused: true, position: 2000, latencyMs: 1 });
  const cluster = state([primary(), paused]);

  const { decisions } = router.plan(cluster, [
    readReq({ key: 'r1', consistency: 'strong' }),
    readReq({ key: 'r2', consistency: 'eventual', sessionId: 's2' }),
  ]);

  assert.equal(decisions[0].target, 'p1');
  assert.match(decisions[0].reason, /strong-consistency/);
  // 最终一致性也不使用暂停副本
  assert.equal(decisions[1].target, 'p1');
});

test('复制暂停：位置心跳过期视为不可信副本', () => {
  const router = createRouter({ maxStalenessMs: 1000 });
  const stale = instance({ id: 'r1', positionUpdatedAt: NOW - 60_000 });
  const cluster = state([primary(), stale]);

  const { decisions } = router.plan(cluster, [
    readReq({ key: 'r1', consistency: 'eventual' }),
  ]);

  assert.equal(decisions[0].target, 'p1');
});

test('故障切换：主库不健康时写请求进入重试而不是发往副本', () => {
  const router = createRouter();
  const cluster = state([primary({ healthy: false }), instance({ id: 'r1' })]);

  const { decisions, retries } = router.plan(cluster, [
    { key: 'w1', type: 'write', sessionId: 's1', position: 2100 },
  ]);

  assert.equal(decisions[0].status, Status.RETRY);
  assert.equal(decisions[0].reason, 'primary-unavailable');
  assert.equal(retries.length, 1);
  // 写未成功，会话下限不得推进
  assert.equal(router.getSessionFloor('s1'), 0);
});

test('批量乱序：读先于写到达时仍按请求自带位置路由，重复键去重', () => {
  const router = createRouter({ maxWaitMs: 100 });
  const replica = instance({ id: 'r1', position: 1000, replicationRatePerMs: 0 });
  const cluster = state([primary(), replica]);

  const requests = [
    // 读排在写之前，但携带了 lastWritePosition
    readReq({ key: 'r1', sessionId: 's1', lastWritePosition: 1500 }),
    { key: 'w1', type: 'write', sessionId: 's1', position: 1500 },
    readReq({ key: 'r1', sessionId: 's1', lastWritePosition: 1500 }), // 重复键
  ];

  const { decisions } = router.plan(cluster, requests);

  assert.equal(decisions[0].target, 'p1'); // 不依赖批内写请求也满足单调读
  assert.equal(decisions[1].target, 'p1');
  assert.equal(decisions[2].status, Status.DUPLICATE);
  assert.equal(decisions[2].reason, 'duplicate-of:r1');
  assert.equal(decisions[2].target, decisions[0].target);
});

test('确定性：相同输入与顺序产生完全相同的结果', () => {
  const build = () => {
    const router = createRouter();
    const cluster = state([
      primary(),
      instance({ id: 'r1', position: 2000, load: 0.3 }),
      instance({ id: 'r2', position: 2000, load: 0.3 }),
    ]);
    const requests = [
      readReq({ key: 'a', consistency: 'eventual' }),
      readReq({ key: 'b', consistency: 'eventual' }),
      readReq({ key: 'c', consistency: 'strong' }),
    ];
    return router.plan(cluster, requests);
  };

  assert.deepEqual(build(), build());
});

test('确定性：评分相同时按实例 ID 稳定打破平局', () => {
  const router = createRouter();
  const cluster = state([
    primary({ capacity: 0 }),
    instance({ id: 'r-b', position: 2000, load: 0.2, latencyMs: 5 }),
    instance({ id: 'r-a', position: 2000, load: 0.2, latencyMs: 5 }),
  ]);

  const { decisions } = router.plan(cluster, [
    readReq({ key: 'k1', consistency: 'eventual' }),
  ]);

  assert.equal(decisions[0].target, 'r-a');
});

test('额度耗尽：实例容量用完后溢出到下一候选，再耗尽则重试', () => {
  const router = createRouter();
  const cluster = state([
    primary({ capacity: 0 }),
    instance({ id: 'r1', capacity: 1, position: 2000, latencyMs: 5 }),
    instance({ id: 'r2', capacity: 1, position: 2000, latencyMs: 6 }),
  ]);

  const { decisions, retries } = router.plan(cluster, [
    readReq({ key: 'k1', consistency: 'eventual' }),
    readReq({ key: 'k2', consistency: 'eventual' }),
    readReq({ key: 'k3', consistency: 'eventual' }),
  ]);

  assert.equal(decisions[0].target, 'r1');
  assert.equal(decisions[1].target, 'r2'); // r1 额度耗尽后溢出
  assert.equal(decisions[2].status, Status.RETRY);
  assert.equal(decisions[2].reason, 'capacity-exhausted');
  assert.equal(retries.length, 1);
  assert.ok(decisions[2].retryAfterMs > 0);
});

test('负载摘除：过载副本不参与路由', () => {
  const router = createRouter({ maxLoad: 0.9 });
  const cluster = state([
    primary({ capacity: 0 }),
    instance({ id: 'r1', load: 0.99, position: 2000 }),
    instance({ id: 'r2', load: 0.2, position: 2000 }),
  ]);

  const { decisions } = router.plan(cluster, [
    readReq({ key: 'k1', consistency: 'eventual' }),
  ]);

  assert.equal(decisions[0].target, 'r2');
});
