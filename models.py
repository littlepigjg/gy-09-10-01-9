"""数据库模型"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, JSON, Index
from database import Base


class RateLimitRule(Base):
    """限流规则表"""
    __tablename__ = "rate_limit_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    path = Column(String(255), nullable=False, index=True, comment="请求路径(支持通配符)")
    method = Column(String(10), default="ALL", comment="HTTP方法: GET/POST/ALL")
    algorithm = Column(String(50), nullable=False, comment="限流算法: token_bucket/sliding_window/fixed_window")
    rate = Column(Float, nullable=False, comment="速率(每秒请求数)")
    burst = Column(Integer, default=0, comment="突发容量")
    window_size = Column(Integer, default=60, comment="窗口大小(秒)")
    slot_granularity = Column(Float, default=1.0, comment="槽粒度(秒，仅滑动窗口)")
    enabled = Column(Boolean, default=True, comment="是否启用")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class CircuitBreakerState(Base):
    """熔断器状态表"""
    __tablename__ = "circuit_breaker_states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_name = Column(String(255), nullable=False, unique=True, comment="后端服务名称")
    backend_url = Column(String(500), nullable=False, comment="后端服务地址")
    state = Column(String(20), nullable=False, default="closed", comment="状态: closed/open/half_open")
    failure_count = Column(Integer, default=0, comment="连续失败次数")
    success_count = Column(Integer, default=0, comment="半开状态成功次数")
    failure_threshold = Column(Integer, default=5, comment="失败阈值")
    recovery_timeout = Column(Integer, default=30, comment="恢复超时(秒)")
    half_open_max_calls = Column(Integer, default=3, comment="半开最大试探数")
    success_threshold = Column(Integer, default=2, comment="半开恢复成功阈值")
    last_failure_time = Column(DateTime, nullable=True, comment="最近失败时间")
    last_state_change = Column(DateTime, default=datetime.now, comment="最近状态变更时间")
    total_requests = Column(Integer, default=0, comment="总请求数")
    total_failures = Column(Integer, default=0, comment="总失败数")
    enabled = Column(Boolean, default=True, comment="是否启用")
    created_at = Column(DateTime, default=datetime.now)


class RateLimitEvent(Base):
    """限流事件日志"""
    __tablename__ = "rate_limit_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, nullable=True, index=True, comment="关联规则ID")
    path = Column(String(255), nullable=False)
    client_ip = Column(String(50))
    algorithm = Column(String(50))
    allowed = Column(Boolean, nullable=False, comment="是否允许通过")
    current_rate = Column(Float, comment="当前速率")
    limit_rate = Column(Float, comment="限制速率")
    reason = Column(String(200), comment="拒绝原因")
    created_at = Column(DateTime(6), default=datetime.now, comment="事件时间(微秒级)")


class WindowTuneResult(Base):
    """滑动窗口参数重估结果"""
    __tablename__ = "window_tune_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, nullable=False, comment="规则ID")
    path = Column(String(255), nullable=False, comment="规则路径")
    window_size = Column(Integer, comment="评估时窗口大小(秒)")
    current_slot_granularity = Column(Float, comment="现槽宽(秒)")
    suggested_window_size = Column(Integer, comment="建议窗口大小(秒)")
    suggested_slot_granularity = Column(Float, comment="建议槽宽(秒)")
    suggested_slot_count = Column(Integer, comment="建议槽数")
    sample_count = Column(Integer, default=0, comment="样本数(到达间隔数)")
    confidence = Column(Float, default=0, comment="置信度 0~1")
    status = Column(String(20), default="ok", comment="评估状态: ok/empty/burst")
    deviation_pct = Column(Float, default=0, comment="现槽宽与建议槽宽偏差(%)")
    detail = Column(String(500), default="", comment="评估说明")
    evaluated_at = Column(DateTime, default=datetime.now, comment="评估时间")

    __table_args__ = (
        Index("idx_rule_evaluated", "rule_id", "evaluated_at"),
    )


class TrafficStat(Base):
    """流量统计表"""
    __tablename__ = "traffic_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.now, index=True)
    total_requests = Column(Integer, default=0)
    allowed_requests = Column(Integer, default=0)
    rejected_requests = Column(Integer, default=0)
    avg_latency_ms = Column(Float, default=0)
    circuit_breaker_trips = Column(Integer, default=0)
