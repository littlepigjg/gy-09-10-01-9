"""滑动窗口算法 - 内存优化版本

核心思路：
- 使用固定数量的时间槽(TimeSlot)模拟滑动窗口
- 每个槽记录该时间段内的请求数
- 当前窗口请求数 = 各槽请求数之和（含当前部分槽）
- 内存优化：用 OrderedDict 按时间顺序管理槽，自动淘汰旧槽
- 高并发：使用 RLock 保护槽状态，读操作无锁快路径
- 自适应：槽粒度可按实例配置，支持运行时 retune 热变更；
  槽宽变更瞬间已计数请求按时间映射重新分桶，不清零也不重复扣减
"""
import threading
import time
from collections import OrderedDict
from .base import BaseRateLimiter, RateLimitResult

DEFAULT_SLOT_GRANULARITY = 1.0  # 默认槽粒度(秒)
MIN_SLOT_GRANULARITY = 0.1      # 最小槽粒度(秒)，防止槽数爆炸
MAX_SLOT_COUNT = 1000           # 单窗口最大槽数保护


class _SlidingWindow:
    """单个滑动窗口"""

    def __init__(self, rate: float, window_size: int,
                 slot_granularity: float = DEFAULT_SLOT_GRANULARITY):
        self.rate = rate
        self.window_size = window_size
        self.slot_granularity = max(MIN_SLOT_GRANULARITY, float(slot_granularity))
        self.slots: OrderedDict[int, int] = OrderedDict()  # slot_key -> count
        self.lock = threading.RLock()
        self._current_slot = self._slot_key(time.monotonic())

    def _slot_key(self, ts: float) -> int:
        return int(ts / self.slot_granularity)

    def _slot_span(self) -> int:
        """窗口覆盖的槽数"""
        return max(1, int(self.window_size / self.slot_granularity))

    def _evict_old_slots(self, current_slot: int):
        """淘汰窗口外的旧槽"""
        cutoff = current_slot - self._slot_span()
        while self.slots:
            oldest_key = next(iter(self.slots))
            if oldest_key <= cutoff:
                self.slots.popitem(last=False)
            else:
                break

    def _total_count(self, current_slot: int) -> int:
        """计算当前窗口总请求数（窗口内各时间槽完整计数之和）。

        注意：已发生的请求不能按当前槽的时间流逝比例折算，否则高并发突发
        落在某一槽的极短时间片内时计数会被压成 0，导致限流失效。
        """
        cutoff = current_slot - self._slot_span()
        return sum(v for k, v in self.slots.items() if k > cutoff)

    def retune(self, rate: float = None, window_size: int = None,
               slot_granularity: float = None):
        """热更新窗口参数。

        槽宽变更时对已计数请求重新分桶(re-bucket)：旧槽 k 覆盖时间区间
        [k*g_old, (k+1)*g_old)，整体映射到新槽 int(k*g_old / g_new)，
        计数按目标槽累加合并且总量守恒 —— 不清零、不重复扣减。
        旧槽 key 单调递增 => 新槽 key 单调非降，OrderedDict 有序性保持。
        """
        with self.lock:
            new_g = max(MIN_SLOT_GRANULARITY, float(slot_granularity)) \
                if slot_granularity else self.slot_granularity
            if rate:
                self.rate = rate
            if window_size:
                self.window_size = window_size

            if new_g != self.slot_granularity:
                old_g = self.slot_granularity
                new_slots: OrderedDict[int, int] = OrderedDict()
                for k, v in self.slots.items():
                    new_key = int((k * old_g) / new_g)
                    new_slots[new_key] = new_slots.get(new_key, 0) + v
                self.slots = new_slots
                self.slot_granularity = new_g

            self._current_slot = self._slot_key(time.monotonic())
            # 窗口/槽宽变化后立即按新参数淘汰出界槽
            self._evict_old_slots(self._current_slot)

    def check_and_consume(self) -> tuple[bool, float, int, int]:
        """检查并消费请求，返回 (allowed, current_rate, total, limit)"""
        with self.lock:
            # 槽 key 必须在锁内计算：与 retune 串行化，
            # 避免并发热更新时用旧槽宽算出的 key 插入新槽表
            current_slot = self._slot_key(time.monotonic())
            if current_slot != self._current_slot:
                self._evict_old_slots(current_slot)
                self._current_slot = current_slot

            total = self._total_count(current_slot)
            limit = int(self.rate * self.window_size)

            if total < limit:
                self.slots[current_slot] = self.slots.get(current_slot, 0) + 1
                total += 1
                current_rate = total / self.window_size
                return True, current_rate, total, limit
            else:
                current_rate = total / self.window_size
                return False, current_rate, total, limit


class SlidingWindowLimiter(BaseRateLimiter):
    """滑动窗口限流器"""

    def __init__(self, rate: float, burst: int = 0, window_size: int = 60,
                 slot_granularity: float = DEFAULT_SLOT_GRANULARITY, **kwargs):
        super().__init__(rate, burst)
        self.window_size = window_size
        self.slot_granularity = max(MIN_SLOT_GRANULARITY, float(slot_granularity))
        self._windows: dict[str, _SlidingWindow] = {}
        self._window_locks: dict[str, threading.RLock] = {}
        self._lock_lock = threading.RLock()

    def _get_window(self, key: str) -> _SlidingWindow:
        """获取或创建滑动窗口"""
        if key in self._windows:
            return self._windows[key]

        with self._lock_lock:
            if key not in self._windows:
                self._windows[key] = _SlidingWindow(
                    self.rate, self.window_size, self.slot_granularity)
            return self._windows[key]

    def retune(self, rate: float = None, window_size: int = None,
               slot_granularity: float = None):
        """热更新限流器参数，所有已存在窗口原地重调，计数不丢失"""
        with self._lock_lock:
            if rate:
                self.rate = rate
            if window_size:
                self.window_size = window_size
            if slot_granularity:
                self.slot_granularity = max(MIN_SLOT_GRANULARITY, float(slot_granularity))
            for w in self._windows.values():
                w.retune(rate=self.rate, window_size=self.window_size,
                         slot_granularity=self.slot_granularity)

    def allow(self, key: str = "default") -> RateLimitResult:
        window = self._get_window(key)
        allowed, current_rate, total, limit = window.check_and_consume()

        remaining = max(0, limit - total)

        return RateLimitResult(
            allowed=allowed,
            current_rate=round(current_rate, 2),
            limit_rate=self.rate,
            remaining=remaining,
            retry_after=window.slot_granularity if not allowed else 0.0,
            reason="" if allowed else f"滑动窗口已达上限({total}/{limit})",
        )

    def get_window_info(self, key: str) -> dict:
        """获取窗口信息"""
        if key in self._windows:
            w = self._windows[key]
            now = time.monotonic()
            current_slot = w._slot_key(now)
            total = w._total_count(current_slot)
            limit = int(self.rate * self.window_size)
            return {"total": total, "limit": limit, "window_size": self.window_size,
                    "slot_granularity": w.slot_granularity}
        return {"total": 0, "limit": int(self.rate * self.window_size),
                "window_size": self.window_size,
                "slot_granularity": self.slot_granularity}
