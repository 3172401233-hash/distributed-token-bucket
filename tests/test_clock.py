"""时钟漂移与单调时钟语义测试。"""

import unittest

from ratelimit import MockClock, SharedTokenBucket
from ratelimit.node import MODE_SAFE
from tests.helpers import make_cluster


class ClockDriftTest(unittest.TestCase):
    def test_node_local_deadline_precedes_store_expiry(self):
        # 节点时钟走快 5%，漂移边距 10%
        cluster = make_cluster(
            n=1,
            capacity=100.0,
            refill_rate=50.0,
            ttl=2.0,
            clock_rates=[1.05],
            drift_margin=0.1,
        )
        node = cluster.nodes[0]
        node.allow()
        grant = node._grant
        self.assertIsNotNone(grant)

        store_expiry = grant.granted_at + grant.ttl
        # 推进到节点本地有效期刚过（节点时钟走快，基准时间按比例折算）。
        # 不发请求，避免续租窗口内的请求触发提前续租。
        drift_elapsed = grant.ttl * (1.0 - 0.1) / 1.05 + 0.005
        cluster.advance(drift_elapsed)
        self.assertLess(cluster.store_clock.mono(), store_expiry)
        # 懒失效：本地视角已到期，模式判定为降级
        self.assertEqual(node.mode, MODE_SAFE)
        # 不发新请求：控制面仍可达时请求会触发重新申请（属正确自愈），
        # 这里只验证「节点先于存储端放弃租约」的时序契约。
        view = cluster.store.get_lease(node.node_id)
        self.assertIsNotNone(view)
        self.assertFalse(view.expired)
        # 本地有效期严格短于存储端 TTL
        local_lifetime = node._local_deadline - node._grant_local_mono
        self.assertLess(local_lifetime, grant.ttl)

    def test_drift_within_margin_keeps_lease_valid_long_term(self):
        cluster = make_cluster(
            n=2,
            capacity=100.0,
            refill_rate=50.0,
            ttl=2.0,
            clock_rates=[1.05, 0.95],
            drift_margin=0.1,
        )
        # 需求低于限额，跑 10s：漂移在边距内，续租链路必须全程稳定
        cluster.run(duration=10.0, step=0.05, per_step_requests=1)
        for node in cluster.nodes:
            self.assertEqual(node.renew_failures_total, 0)
        self.assertLess(cluster.false_reject_rate(), 0.01)
        cluster.assert_within_global_cap(window_seconds=10.0, slack=2.0)

    def test_mono_zero_point_difference_is_ignored(self):
        # 单调时钟零点差巨大也不影响：跨节点只比较时长
        cluster = make_cluster(
            n=2,
            capacity=100.0,
            refill_rate=50.0,
            ttl=2.0,
        )
        # helpers 已注入不同 mono_offset；正常跑 6s 不应出现异常拒绝
        cluster.run(duration=6.0, step=0.1, per_step_requests=1)
        self.assertLess(cluster.false_reject_rate(), 0.01)

    def test_wall_time_never_drives_decisions(self):
        cluster = make_cluster(n=1, capacity=10.0, refill_rate=10.0)
        node = cluster.nodes[0]
        node.allow()
        # 墙上时间剧烈跳变（模拟 NTP 步进），限流行为不受影响
        node.clock.wall_offset = -999999.0  # type: ignore[attr-defined]
        cluster.store_clock.wall_offset = 999999.0
        d = node.allow()
        self.assertTrue(d.allowed)

    def test_mock_clock_refuses_backward_jump(self):
        clock = MockClock()
        clock.advance(5.0)
        with self.assertRaises(ValueError):
            clock.advance(-1.0)


class IsolatedSlowClockTest(unittest.TestCase):
    def test_slow_node_still_renews_before_store_expiry(self):
        # 节点时钟走慢 5%：续租判断基于节点时长，0.5 因子 + 10% 边距
        # 给出充足余量，存储端 TTL 到达前必然完成过续租
        store_clock = MockClock()
        store = SharedTokenBucket(100.0, 50.0, clock=store_clock)
        node_clock = MockClock(rate=0.95, mono_offset=500.0)
        from ratelimit import RateLimiterNode

        node = RateLimiterNode(
            node_id="slow",
            store=store,
            lease_quota=100.0,
            lease_ttl=2.0,
            safe_rate=5.0,
            safe_capacity=10.0,
            clock=node_clock,
            drift_margin=0.1,
        )
        node.allow()
        first_epoch = node._grant.epoch
        # 走到续租窗口（本地有效 1.8s 的一半 = 0.9s 节点时长，
        # 对应基准 0.9/0.95 ≈ 0.947s），此时存储端 TTL 远未到期
        store_clock.advance(1.0)
        node_clock.advance(1.0)
        node.allow()
        # 存储端 TTL=2s 未到，节点已提前续租，未发生降级
        self.assertGreaterEqual(node.renewals_total, 1)
        self.assertEqual(node.renew_failures_total, 0)
        self.assertGreaterEqual(node._grant.epoch, first_epoch)


if __name__ == "__main__":
    unittest.main()
