"""分布式令牌桶限流库。

核心组件：

- :class:`~ratelimit.clock.Clock`：单调时钟抽象，测试中可注入模拟时钟。
- :class:`~ratelimit.store.SharedTokenBucket`：共享桶权威状态，负责配额预留、
  带 TTL 的配额租约、续租结算、fencing epoch 与过期回收。
- :class:`~ratelimit.node.RateLimiterNode`：网关节点，申请/续租租约，
  续租失败时退回本地安全配额，绝不按全速放行。
"""

from .clock import Clock, MockClock, RealClock, Simulation
from .errors import (
    LeaseError,
    LeaseExpired,
    LeaseFenced,
    NodePartitioned,
    QuotaExceeded,
    QuotaUnavailable,
)
from .node import Decision, RateLimiterNode, SafeBucket
from .store import Grant, LeaseView, SharedTokenBucket

__all__ = [
    "Clock",
    "MockClock",
    "RealClock",
    "Simulation",
    "LeaseError",
    "LeaseExpired",
    "LeaseFenced",
    "NodePartitioned",
    "QuotaExceeded",
    "QuotaUnavailable",
    "Decision",
    "RateLimiterNode",
    "SafeBucket",
    "Grant",
    "LeaseView",
    "SharedTokenBucket",
]

__version__ = "1.0.0"
