"""时钟抽象。

生产环境使用 :class:`RealClock`，基于 :func:`time.monotonic_ns`，不受墙上
时间回拨影响；测试使用 :class:`MockClock`，可模拟节点间时钟漂移。

注意：跨节点只比较「时长」，不比较单调时钟的绝对值，因为不同机器的
单调时钟零点没有任何关系。
"""

import time
from typing import Callable, Optional


class Clock:
    """时钟接口：单调时钟 + 墙上时钟。"""

    def mono(self) -> float:
        """返回单调时间（秒），只保证在本进程内单调递增。"""
        raise NotImplementedError

    def wall(self) -> float:
        """返回 Unix 墙上时间（秒），仅用于日志展示，不参与限流判断。"""
        raise NotImplementedError


class RealClock(Clock):
    """生产时钟：单调时钟基于 ``monotonic_ns``，不受 NTP 调表影响。"""

    def mono(self) -> float:
        return time.monotonic_ns() / 1_000_000_000.0

    def wall(self) -> float:
        return time.time()


class MockClock(Clock):
    """测试时钟。

    - 所有 :class:`MockClock` 共享同一个仿真基准时间，由 :meth:`advance`
      推进；
    - ``rate`` 模拟时钟频率偏差（如 1.05 表示该节点时钟走快 5%）；
    - ``mono_offset`` 模拟单调时钟零点差；
    - ``wall_offset`` 模拟墙上时间差（对限流判断无影响）。
    """

    def __init__(
        self,
        rate: float = 1.0,
        mono_offset: float = 0.0,
        wall_offset: float = 0.0,
    ) -> None:
        self._base = 0.0
        self.rate = rate
        self.mono_offset = mono_offset
        self.wall_offset = wall_offset

    def advance(self, delta: float) -> None:
        if delta < 0:
            raise ValueError("仿真时间只能向前推进")
        self._base += delta

    def mono(self) -> float:
        return self._base * self.rate + self.mono_offset

    def wall(self) -> float:
        return self._base + self.wall_offset


class Simulation:
    """测试驱动器：持有一个权威时钟，按固定步长推进仿真。"""

    def __init__(self) -> None:
        self.clock = MockClock()
        self.elapsed = 0.0

    def run(
        self,
        seconds: float,
        step: float,
        callback: Optional[Callable[[float], None]] = None,
    ) -> None:
        end = self.elapsed + seconds
        while self.elapsed < end - 1e-12:
            if callback is not None:
                callback(self.elapsed)
            self.clock.advance(step)
            self.elapsed += step
