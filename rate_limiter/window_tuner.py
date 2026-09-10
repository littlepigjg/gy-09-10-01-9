"""滑动窗口参数自适应重估

依据 rate_limit_events 中该规则下请求的到达间隔分布，
反推滑动窗口的最优槽数与槽宽：

- 槽宽锚定到达间隔中位数：槽宽 ≈ 典型间隔时，请求在槽间分布最均匀，
  既不会因槽过宽把突发压进同一槽导致误杀，也不会因槽过细浪费内存；
- 槽数 = 窗口大小 / 槽宽，并夹在 [MIN_SLOTS, MAX_SLOTS] 之间，
  槽宽最终回算为 window_size / 槽数，保证整除窗口不留残槽；
- 置信度 = 样本充足度 × 分布稳定度（泊松到达 CV≈1 时稳定度最高）。

边界处理：
- 空窗口（无样本/样本不足）：保持现状，置信度 0，status=empty；
- 突发单点（所有到达集中在同一时刻，间隔全为 0）：建议最小槽宽，
  置信度低，status=burst。

性能：单次全规则重估有 EVAL_BUDGET_SECONDS 时间预算保护，
事件流一次查询拉取、内存分组计算，20s 内完成。
"""
import fnmatch
import logging
import statistics
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# 槽数/槽宽约束
MIN_SLOTS = 8
MAX_SLOTS = 600
MIN_GRANULARITY = 0.1    # 最小槽宽(秒)
MAX_GRANULARITY = 300.0  # 最大槽宽(秒)

# 重估采样与预算
EVENT_LOOKBACK_SECONDS = 3600   # 事件回溯窗口
MAX_EVENTS_PER_RUN = 50000      # 单次重估最多读入的事件数
EVAL_BUDGET_SECONDS = 19.0      # 全规则重估时间预算(20s 上限内)
SAMPLE_CONFIDENCE_FULL = 200    # 样本数达到该值时样本充足度为 1
BURST_CONFIDENCE = 0.2          # 突发单点场景的固定低置信度


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _clamp_slots(window_size: int, granularity: float) -> int:
    """槽数夹在约束区间内"""
    return int(_clamp(round(window_size / granularity), MIN_SLOTS, MAX_SLOTS))


def compute_suggestion(intervals: list[float], window_size: int,
                       current_granularity: float) -> dict:
    """根据到达间隔分布反推最优槽宽与槽数。

    Args:
        intervals: 相邻请求到达间隔(秒)，按到达顺序排列
        window_size: 当前窗口大小(秒)
        current_granularity: 当前槽宽(秒)

    Returns:
        含建议槽宽/槽数/窗口、置信度、状态与偏差的字典
    """
    window_size = max(1, int(window_size))
    current_granularity = max(MIN_GRANULARITY, float(current_granularity))
    n = len(intervals)

    base = {
        "sample_count": n,
        "suggested_window_size": window_size,
        "suggested_slot_granularity": current_granularity,
        "suggested_slot_count": _clamp_slots(window_size, current_granularity),
    }

    # 边界一：空窗口（无事件或样本不足以构成间隔序列）
    if n < 2:
        return {**base, "status": "empty", "confidence": 0.0,
                "detail": "样本不足，保持现有槽参数"}

    positive = [i for i in intervals if i > 1e-9]

    # 边界二：突发单点（所有到达集中在同一时刻，间隔全为 0）
    if not positive:
        slots = _clamp_slots(window_size, MIN_GRANULARITY)
        g = window_size / slots
        return {**base,
                "status": "burst",
                "confidence": BURST_CONFIDENCE,
                "suggested_slot_granularity": g,
                "suggested_slot_count": slots,
                "detail": "到达集中于单点，建议最小槽宽以分辨突发"}

    mean = statistics.fmean(positive)
    median = statistics.median(positive)
    stdev = statistics.pstdev(positive) if len(positive) > 1 else 0.0
    cv = stdev / mean if mean > 0 else 0.0

    # 槽宽锚定中位间隔，并约束在合法范围
    g_ideal = _clamp(median, MIN_GRANULARITY, min(MAX_GRANULARITY, float(window_size)))
    slots = _clamp_slots(window_size, g_ideal)
    suggested_window = max(1, int(round(slots * g_ideal)))
    g = suggested_window / slots  # 槽宽整除窗口，避免末端残槽

    # 置信度：样本充足度 × 分布稳定度（泊松到达 CV≈1 时最高）
    sample_factor = min(1.0, n / SAMPLE_CONFIDENCE_FULL)
    stability = 1.0 / (1.0 + abs(cv - 1.0))
    confidence = round(sample_factor * stability, 4)

    return {**base,
            "status": "ok",
            "confidence": confidence,
            "suggested_window_size": suggested_window,
            "suggested_slot_granularity": g,
            "suggested_slot_count": slots,
            "detail": f"间隔中位数{median:.4f}s, 均值{mean:.4f}s, CV={cv:.2f}"}


def deviation_pct(current: float, suggested: float) -> float:
    """现槽宽与建议槽宽的相对偏差(%)"""
    if not current:
        return 0.0
    return round((suggested - current) / current * 100, 2)


def group_events_by_rule(events: list, rules: list[dict]) -> dict[int, list]:
    """把事件流按规则分组。

    优先按事件携带的 rule_id 精确归属；历史数据无 rule_id 时
    回退为按规则 path 通配符匹配。与规则热更新并发安全：
    分组仅依赖调用时刻的规则快照。
    """
    groups: dict[int, list] = {r["id"]: [] for r in rules}
    patterns = [(r["id"], r["path"]) for r in rules]
    for ev in events:
        rid = getattr(ev, "rule_id", None)
        if rid is not None and rid in groups:
            groups[rid].append(ev.created_at)
            continue
        path = ev.path
        for rule_id, pattern in patterns:
            if fnmatch.fnmatch(path, pattern):
                groups[rule_id].append(ev.created_at)
                break
    return groups


async def run_full_evaluation(session, manager) -> list[dict]:
    """对所有 sliding_window 规则执行一次重估并落库。

    事件流一次查询拉取、内存分组，带时间预算保护，
    单次全规则重估在 20s 内完成。与 PUT /api/rules/{id} 热更新并发：
    规则快照在入口取一次，事件流为只读查询，互不加锁。
    """
    from sqlalchemy import select
    from models import RateLimitEvent, WindowTuneResult

    started = time.monotonic()
    rules = [r for r in manager.get_rules() if r["algorithm"] == "sliding_window"]
    if not rules:
        return []

    since = datetime.now() - timedelta(seconds=EVENT_LOOKBACK_SECONDS)
    stmt = (select(RateLimitEvent)
            .where(RateLimitEvent.created_at >= since)
            .order_by(RateLimitEvent.id.desc())
            .limit(MAX_EVENTS_PER_RUN))
    rows = (await session.execute(stmt)).scalars().all()
    events = list(reversed(rows))  # 恢复时间正序

    groups = group_events_by_rule(events, rules)
    evaluated_at = datetime.now()
    results = []

    for rule in rules:
        # 时间预算保护：超预算则停止后续规则，保证整体 20s 内返回
        if time.monotonic() - started > EVAL_BUDGET_SECONDS:
            logger.warning("重估时间预算耗尽，剩余规则跳过本次评估")
            break

        timestamps = sorted(t for t in groups[rule["id"]] if t is not None)
        intervals = [(b - a).total_seconds()
                     for a, b in zip(timestamps, timestamps[1:])]

        current_g = rule.get("slot_granularity") or 1.0
        sug = compute_suggestion(intervals, rule["window_size"], current_g)
        dev = deviation_pct(current_g, sug["suggested_slot_granularity"])

        row = WindowTuneResult(
            rule_id=rule["id"],
            path=rule["path"],
            window_size=rule["window_size"],
            current_slot_granularity=current_g,
            suggested_window_size=sug["suggested_window_size"],
            suggested_slot_granularity=sug["suggested_slot_granularity"],
            suggested_slot_count=sug["suggested_slot_count"],
            sample_count=sug["sample_count"],
            confidence=sug["confidence"],
            status=sug["status"],
            deviation_pct=dev,
            detail=sug["detail"],
            evaluated_at=evaluated_at,
        )
        session.add(row)
        results.append({
            "rule_id": rule["id"],
            "path": rule["path"],
            "window_size": rule["window_size"],
            "current_slot_granularity": current_g,
            "suggested_window_size": sug["suggested_window_size"],
            "suggested_slot_granularity": sug["suggested_slot_granularity"],
            "suggested_slot_count": sug["suggested_slot_count"],
            "sample_count": sug["sample_count"],
            "confidence": sug["confidence"],
            "status": sug["status"],
            "deviation_pct": dev,
            "detail": sug["detail"],
            "evaluated_at": evaluated_at.isoformat(),
        })

    await session.commit()
    elapsed = time.monotonic() - started
    logger.info(f"窗口参数重估完成: {len(results)}/{len(rules)} 条规则, 耗时 {elapsed:.2f}s")
    return results
