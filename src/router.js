/**
 * 数据库读路由规划器。
 *
 * 输入一批读/写请求与集群状态，输出每个请求的路由决策。
 * 保证：
 *  - 同一会话单调读（会话下限只增不减）；
 *  - 需要最新数据的请求只在复制位置满足后放行，否则明确失败，绝不静默降级；
 *  - 相同输入与相同请求顺序产生完全相同的结果（无随机、无隐式时钟）。
 */

export const Consistency = Object.freeze({
  EVENTUAL: 'eventual',
  MONOTONIC: 'monotonic',
  STRONG: 'strong',
});

export const Status = Object.freeze({
  ROUTED: 'routed',
  WAIT: 'wait',
  DUPLICATE: 'duplicate',
  RETRY: 'retry',
});

const DEFAULTS = Object.freeze({
  maxWaitMs: 250,        // 为满足一致性允许等待复制的上限
  maxStalenessMs: 5000,  // 复制位置心跳超过该时长视为不可信
  loadWeightMs: 100,     // 每单位负载折算的延迟惩罚
  regionPenaltyMs: 50,   // 跨区域读取的惩罚
  maxLoad: 0.95,         // 负载达到该值即摘除
});

function compareStrings(a, b) {
  return a < b ? -1 : a > b ? 1 : 0;
}

function normalizeConsistency(value) {
  if (value === undefined || value === null) return Consistency.MONOTONIC;
  if (Object.values(Consistency).includes(value)) return value;
  throw new TypeError(`unknown consistency level: ${value}`);
}

function estimatedWaitMs(instance, requiredPosition) {
  if (instance.role === 'primary') return 0;
  const gap = Math.max(0, requiredPosition - instance.position);
  if (gap === 0) return 0;
  const rate = instance.replicationRatePerMs;
  if (!(rate > 0)) return Number.POSITIVE_INFINITY;
  return Math.ceil(gap / rate);
}

export function createRouter(options = {}) {
  const config = { ...DEFAULTS, ...options };
  // 会话下限：sessionId -> 该会话已经见过的最大复制位置
  const sessionFloors = new Map();

  function getSessionFloor(sessionId) {
    if (sessionId === undefined || sessionId === null) return 0;
    return sessionFloors.get(sessionId) ?? 0;
  }

  function raiseSessionFloor(sessionId, position) {
    if (sessionId === undefined || sessionId === null) return;
    const current = sessionFloors.get(sessionId) ?? 0;
    if (position > current) sessionFloors.set(sessionId, position);
  }

  /**
   * @param state 集群快照：
   *   { now, clientRegion, highWatermark, instances: [...] }
   *   instance: { id, role: 'primary'|'replica', region, position,
   *               positionUpdatedAt, replicationRatePerMs, paused,
   *               healthy, load, latencyMs, capacity }
   * @param requests 有序请求数组：
   *   { key, type: 'read'|'write', sessionId, lastWritePosition,
   *     consistency, position (写请求产生的位置) }
   */
  function plan(state, requests) {
    const { now, clientRegion, highWatermark } = state;
    const remainingCapacity = new Map(
      state.instances.map((inst) => [inst.id, inst.capacity ?? Number.POSITIVE_INFINITY]),
    );
    const seenKeys = new Map();
    const decisions = [];

    for (const request of requests) {
      if (seenKeys.has(request.key)) {
        const first = seenKeys.get(request.key);
        decisions.push({
          ...first,
          key: request.key,
          status: Status.DUPLICATE,
          reason: `duplicate-of:${first.key}`,
        });
        continue;
      }

      const decision =
        request.type === 'write'
          ? planWrite(request, state)
          : planRead(request, state, remainingCapacity, clientRegion, now, highWatermark);

      seenKeys.set(request.key, decision);
      decisions.push(decision);
    }

    return {
      decisions,
      retries: decisions.filter((d) => d.status === Status.RETRY),
    };
  }

  function planWrite(request, state) {
    const primary = state.instances.find((inst) => inst.role === 'primary');
    if (!primary || !primary.healthy) {
      return {
        key: request.key,
        type: 'write',
        status: Status.RETRY,
        target: null,
        reason: 'primary-unavailable',
        estimatedWaitMs: 0,
        retryAfterMs: config.maxWaitMs,
      };
    }
    if (typeof request.position === 'number') {
      raiseSessionFloor(request.sessionId, request.position);
    }
    return {
      key: request.key,
      type: 'write',
      status: Status.ROUTED,
      target: primary.id,
      reason: 'writes-always-go-to-primary',
      estimatedWaitMs: 0,
    };
  }

  function planRead(request, state, remainingCapacity, clientRegion, now, highWatermark) {
    const consistency = normalizeConsistency(request.consistency);
    const requiredPosition = Math.max(
      request.lastWritePosition ?? 0,
      getSessionFloor(request.sessionId),
      consistency === Consistency.STRONG ? highWatermark : 0,
    );

    const rejected = [];
    const candidates = [];

    for (const inst of state.instances) {
      if (!inst.healthy) {
        rejected.push(`${inst.id}:unhealthy`);
        continue;
      }
      if (inst.paused) {
        rejected.push(`${inst.id}:replication-paused`);
        continue;
      }
      if (inst.role !== 'primary' && now - inst.positionUpdatedAt > config.maxStalenessMs) {
        rejected.push(`${inst.id}:stale-position`);
        continue;
      }
      if (inst.load >= config.maxLoad) {
        rejected.push(`${inst.id}:overloaded`);
        continue;
      }
      const waitMs = estimatedWaitMs(inst, requiredPosition);
      if (waitMs > config.maxWaitMs) {
        rejected.push(`${inst.id}:lag-${waitMs}ms-exceeds-budget`);
        continue;
      }
      const crossRegion = inst.region !== clientRegion;
      const score =
        inst.latencyMs +
        (crossRegion ? config.regionPenaltyMs : 0) +
        inst.load * config.loadWeightMs +
        waitMs;
      candidates.push({ inst, waitMs, crossRegion, score });
    }

    candidates.sort((a, b) => a.score - b.score || compareStrings(a.inst.id, b.inst.id));

    let chosen = null;
    for (const candidate of candidates) {
      const left = remainingCapacity.get(candidate.inst.id) ?? 0;
      if (left <= 0) {
        rejected.push(`${candidate.inst.id}:capacity-exhausted`);
        continue;
      }
      remainingCapacity.set(candidate.inst.id, left - 1);
      chosen = candidate;
      break;
    }

    if (!chosen) {
      const capacityExhausted = candidates.length > 0;
      return {
        key: request.key,
        type: 'read',
        status: Status.RETRY,
        target: null,
        reason: capacityExhausted ? 'capacity-exhausted' : 'no-eligible-instance',
        detail: rejected,
        requiredPosition,
        estimatedWaitMs: 0,
        retryAfterMs: config.maxWaitMs,
      };
    }

    // 单调读：会话下限推进到本次实际满足的位置
    raiseSessionFloor(request.sessionId, requiredPosition);

    const reasons = [];
    reasons.push(chosen.inst.role === 'primary' ? 'primary' : 'replica');
    if (chosen.crossRegion) reasons.push('cross-region-failover');
    if (chosen.waitMs > 0) reasons.push('wait-for-replication');
    if (consistency === Consistency.STRONG) reasons.push('strong-consistency');
    else if (requiredPosition > 0) reasons.push('monotonic-read');

    return {
      key: request.key,
      type: 'read',
      status: chosen.waitMs > 0 ? Status.WAIT : Status.ROUTED,
      target: chosen.inst.id,
      reason: reasons.join('+'),
      requiredPosition,
      estimatedWaitMs: chosen.waitMs,
    };
  }

  return { plan, getSessionFloor };
}
