"""API网关限流熔断服务 - 主应用

功能：
1. 反向代理：将请求转发到后端服务
2. 限流：令牌桶/滑动窗口/固定窗口多种算法
3. 熔断：后端异常时自动熔断，支持半开试探
4. Dashboard：前端页面展示实时状态
5. 规则管理：动态调整限流规则和熔断配置
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from config import APP_HOST, APP_PORT
from database import init_db, async_session, engine
from models import RateLimitRule, CircuitBreakerState, RateLimitEvent, TrafficStat, WindowTuneResult
from rate_limiter import RateLimitManager
from rate_limiter import window_tuner
from circuit_breaker import CircuitBreakerManager

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# 全局管理器
rate_limit_manager = RateLimitManager()
circuit_breaker_manager = CircuitBreakerManager()
http_client: httpx.AsyncClient = None

# 事件缓冲队列（批量写入数据库，线程安全）
from collections import deque
import threading

event_buffer: deque = deque()
event_buffer_lock = threading.Lock()


async def persist_events():
    """定时批量持久化事件到数据库"""
    while True:
        await asyncio.sleep(5)  # 每5秒刷一次
        with event_buffer_lock:
            if not event_buffer:
                continue
            events_to_write = list(event_buffer)
            event_buffer.clear()

        try:
            async with async_session() as session:
                for ev in events_to_write:
                    session.add(RateLimitEvent(
                        rule_id=ev.get("rule_id"),
                        path=ev["path"],
                        client_ip=ev["client_ip"],
                        algorithm=ev["algorithm"],
                        allowed=ev["allowed"],
                        current_rate=ev["current_rate"],
                        limit_rate=ev["limit_rate"],
                        reason=ev["reason"],
                    ))
                await session.commit()
        except Exception as e:
            logger.error(f"持久化事件失败: {e}")


async def persist_traffic_stats():
    """定时持久化流量统计"""
    while True:
        await asyncio.sleep(10)
        stats = rate_limit_manager.get_stats()
        try:
            async with async_session() as session:
                session.add(TrafficStat(
                    total_requests=stats["total_requests"],
                    allowed_requests=stats["allowed_requests"],
                    rejected_requests=stats["rejected_requests"],
                ))
                await session.commit()
        except Exception as e:
            logger.error(f"持久化流量统计失败: {e}")


async def persist_circuit_breaker_states():
    """定时持久化熔断器状态"""
    while True:
        await asyncio.sleep(10)
        states = circuit_breaker_manager.get_all_states()
        try:
            async with async_session() as session:
                for s in states:
                    result = await session.execute(
                        select(CircuitBreakerState).where(
                            CircuitBreakerState.service_name == s["service_name"]
                        )
                    )
                    db_state = result.scalar_one_or_none()
                    if db_state:
                        db_state.state = s["state"]
                        db_state.failure_count = s["failure_count"]
                        db_state.success_count = s["success_count"]
                        db_state.total_requests = s["total_requests"]
                        db_state.total_failures = s["total_failures"]
                        db_state.last_state_change = datetime.fromisoformat(s["last_state_change"])
                        if s["last_failure_time"]:
                            db_state.last_failure_time = datetime.fromisoformat(s["last_failure_time"])
                await session.commit()
        except Exception as e:
            logger.error(f"持久化熔断器状态失败: {e}")


async def cleanup_window_tune_results():
    """定时清理重估结果表：每个 rule_id 仅保留最近 50 条，防止无限膨胀"""
    while True:
        await asyncio.sleep(600)  # 每10分钟清理一次
        try:
            async with async_session() as session:
                await window_tuner.cleanup_old_results(session)
        except Exception as e:
            logger.error(f"清理重估结果失败: {e}")


def on_rate_limit_event(path, client_ip, rule, result):
    """限流事件回调（限流检查可能在线程池中执行，使用线程安全方式缓冲）"""
    event = {
        "rule_id": rule.get("id"),
        "path": path,
        "client_ip": client_ip,
        "algorithm": rule.get("algorithm", "unknown"),
        "allowed": result.allowed,
        "current_rate": result.current_rate,
        "limit_rate": result.limit_rate,
        "reason": result.reason,
    }
    with event_buffer_lock:
        # 缓冲上限保护，防止事件洪峰撑爆内存
        if len(event_buffer) < 50000:
            event_buffer.append(event)


def on_circuit_breaker_change(service_name, old_state, new_state):
    """熔断器状态变更回调"""
    logger.info(f"熔断器状态变更: {service_name} {old_state} -> {new_state}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global http_client

    # 初始化数据库
    await init_db()

    # 初始化HTTP客户端
    http_client = httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=200, max_keepalive_connections=50))

    # 加载规则和状态
    async with async_session() as session:
        await rate_limit_manager.load_rules(session)
        await circuit_breaker_manager.load_states(session)

    rate_limit_manager.set_event_callback(on_rate_limit_event)
    circuit_breaker_manager.set_state_change_callback(on_circuit_breaker_change)

    # 启动后台持久化任务
    tasks = [
        asyncio.create_task(persist_events()),
        asyncio.create_task(persist_traffic_stats()),
        asyncio.create_task(persist_circuit_breaker_states()),
        asyncio.create_task(cleanup_window_tune_results()),
    ]

    logger.info(f"限流熔断网关启动: http://{APP_HOST}:{APP_PORT}")
    yield

    # 清理
    for t in tasks:
        t.cancel()
    await http_client.aclose()
    await engine.dispose()


app = FastAPI(title="API网关限流熔断服务", lifespan=lifespan)


# ==================== 反向代理 ====================

@app.api_route("/proxy/{service_name}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def reverse_proxy(service_name: str, path: str, request: Request):
    """反向代理入口 - 执行限流检查和熔断检查后转发请求"""
    start_time = time.time()
    client_ip = request.client.host if request.client else "unknown"
    full_path = f"/{path}"

    # 1. 限流检查
    limit_result = rate_limit_manager.check(full_path, client_ip, request.method)
    if not limit_result.allowed:
        return JSONResponse(
            status_code=429,
            content={
                "error": "Too Many Requests",
                "message": limit_result.reason,
                "limit_rate": limit_result.limit_rate,
                "retry_after": round(limit_result.retry_after, 2),
            },
            headers={"Retry-After": str(int(limit_result.retry_after) + 1)},
        )

    # 2. 熔断检查
    if not circuit_breaker_manager.allow_request(service_name):
        return JSONResponse(
            status_code=503,
            content={
                "error": "Service Unavailable",
                "message": f"服务 {service_name} 已熔断",
            },
            headers={"Retry-After": "30"},
        )

    # 3. 获取后端地址
    cb_state = circuit_breaker_manager.get_state(service_name)
    if not cb_state:
        return JSONResponse(status_code=503, content={"error": f"未知服务: {service_name}"})

    backend_url = cb_state["backend_url"]
    target_url = f"{backend_url}/{path}"

    # 4. 转发请求
    try:
        headers = dict(request.headers)
        headers.pop("host", None)

        body = await request.body()

        response = await http_client.request(
            method=request.method,
            url=target_url,
            headers=headers,
            content=body,
            params=dict(request.query_params),
        )

        # 记录成功
        circuit_breaker_manager.record_success(service_name)

        latency_ms = (time.time() - start_time) * 1000
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
        )

    except httpx.ConnectError:
        circuit_breaker_manager.record_failure(service_name)
        return JSONResponse(
            status_code=502,
            content={"error": "Bad Gateway", "message": f"无法连接到后端服务 {backend_url}"},
        )
    except httpx.TimeoutException:
        circuit_breaker_manager.record_failure(service_name)
        return JSONResponse(
            status_code=504,
            content={"error": "Gateway Timeout", "message": f"后端服务 {backend_url} 响应超时"},
        )
    except Exception as e:
        circuit_breaker_manager.record_failure(service_name)
        logger.error(f"代理请求异常: {e}")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error", "message": str(e)})


# ==================== 管理API ====================

@app.get("/api/dashboard")
async def get_dashboard_data():
    """获取Dashboard数据"""
    stats = rate_limit_manager.get_stats()
    cb_states = circuit_breaker_manager.get_all_states()
    rules = rate_limit_manager.get_rules()

    # 获取最近的限流事件
    async with async_session() as session:
        result = await session.execute(
            select(RateLimitEvent)
            .order_by(RateLimitEvent.created_at.desc())
            .limit(50)
        )
        events = result.scalars().all()

        # 获取最近的流量统计
        result = await session.execute(
            select(TrafficStat)
            .order_by(TrafficStat.timestamp.desc())
            .limit(30)
        )
        traffic_stats = result.scalars().all()

    return {
        "stats": stats,
        "circuit_breakers": cb_states,
        "rules": rules,
        "recent_events": [
            {
                "id": e.id,
                "path": e.path,
                "client_ip": e.client_ip,
                "algorithm": e.algorithm,
                "allowed": e.allowed,
                "current_rate": e.current_rate,
                "limit_rate": e.limit_rate,
                "reason": e.reason,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ],
        "traffic_history": [
            {
                "timestamp": t.timestamp.isoformat() if t.timestamp else None,
                "total": t.total_requests,
                "allowed": t.allowed_requests,
                "rejected": t.rejected_requests,
            }
            for t in reversed(list(traffic_stats))
        ],
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/api/stats")
async def get_stats():
    """获取实时统计"""
    return rate_limit_manager.get_stats()


@app.get("/api/stats/realtime")
async def get_realtime_stats():
    """获取实时统计（含当前速率信息）"""
    stats = rate_limit_manager.get_stats()
    total = stats["total_requests"]
    allowed = stats["allowed_requests"]
    rejected = stats["rejected_requests"]
    return {
        "total": total,
        "allowed": allowed,
        "rejected": rejected,
        "rejection_rate": round(rejected / total * 100, 2) if total > 0 else 0,
        "timestamp": datetime.now().isoformat(),
    }


# ==================== 限流规则API ====================

@app.get("/api/rules")
async def list_rules():
    """列出所有限流规则"""
    return rate_limit_manager.get_rules()


@app.post("/api/rules")
async def create_rule(request: Request):
    """创建限流规则"""
    data = await request.json()
    required = ["path", "algorithm", "rate"]
    for field in required:
        if field not in data:
            raise HTTPException(400, f"缺少必填字段: {field}")

    if data["algorithm"] not in ("token_bucket", "sliding_window", "fixed_window"):
        raise HTTPException(400, "无效的算法类型")

    async with async_session() as session:
        rule = RateLimitRule(
            path=data["path"],
            method=data.get("method", "ALL"),
            algorithm=data["algorithm"],
            rate=data["rate"],
            burst=data.get("burst", int(data["rate"])),
            window_size=data.get("window_size", 60),
            slot_granularity=data.get("slot_granularity", 1.0),
            enabled=data.get("enabled", True),
        )
        session.add(rule)
        await session.commit()
        await session.refresh(rule)

        # 重新加载规则
        await rate_limit_manager.load_rules(session)

    return {"message": "规则创建成功", "id": rule.id}


@app.put("/api/rules/{rule_id}")
async def update_rule(rule_id: int, request: Request):
    """更新限流规则"""
    data = await request.json()

    async with async_session() as session:
        result = await session.execute(select(RateLimitRule).where(RateLimitRule.id == rule_id))
        rule = result.scalar_one_or_none()
        if not rule:
            raise HTTPException(404, "规则不存在")

        for field in ["path", "method", "algorithm", "rate", "burst", "window_size", "slot_granularity", "enabled"]:
            if field in data:
                setattr(rule, field, data[field])

        await session.commit()
        await rate_limit_manager.load_rules(session)

    return {"message": "规则更新成功"}


@app.delete("/api/rules/{rule_id}")
async def delete_rule(rule_id: int):
    """删除限流规则"""
    async with async_session() as session:
        result = await session.execute(select(RateLimitRule).where(RateLimitRule.id == rule_id))
        rule = result.scalar_one_or_none()
        if not rule:
            raise HTTPException(404, "规则不存在")

        path = rule.path
        await session.delete(rule)
        await session.commit()
        rate_limit_manager.remove_rule(path)

    return {"message": "规则删除成功"}


# ==================== 滑动窗口自适应重估API ====================

@app.post("/api/window-tuner/run")
async def run_window_tuner():
    """对所有 sliding_window 规则执行一次参数重估（20s 内完成）"""
    try:
        async with async_session() as session:
            results = await asyncio.wait_for(
                window_tuner.run_full_evaluation(session, rate_limit_manager),
                timeout=window_tuner.EVAL_BUDGET_SECONDS + 1.0,
            )
        return {"message": "重估完成", "count": len(results), "results": results}
    except asyncio.TimeoutError:
        raise HTTPException(504, "重估超时，已返回部分结果")


@app.get("/api/window-tuner/results")
async def list_window_tune_results():
    """获取每条规则最近一次重估结果（含现槽宽与建议槽宽偏差）"""
    async with async_session() as session:
        result = await session.execute(
            select(WindowTuneResult)
            .order_by(WindowTuneResult.evaluated_at.desc(), WindowTuneResult.id.desc())
            .limit(500)
        )
        rows = result.scalars().all()

    # 每条规则仅保留最近一次评估
    latest: dict[int, dict] = {}
    for r in rows:
        if r.rule_id in latest:
            continue
        latest[r.rule_id] = {
            "rule_id": r.rule_id,
            "path": r.path,
            "window_size": r.window_size,
            "current_slot_granularity": r.current_slot_granularity,
            "suggested_window_size": r.suggested_window_size,
            "suggested_slot_granularity": r.suggested_slot_granularity,
            "suggested_slot_count": r.suggested_slot_count,
            "sample_count": r.sample_count,
            "confidence": r.confidence,
            "status": r.status,
            "deviation_pct": r.deviation_pct,
            "detail": r.detail,
            "evaluated_at": r.evaluated_at.isoformat() if r.evaluated_at else None,
        }
    return list(latest.values())


@app.post("/api/window-tuner/apply/{rule_id}")
async def apply_window_tune(rule_id: int):
    """把最近一次重估建议应用到规则（热更新，已计数请求不清零）"""
    async with async_session() as session:
        result = await session.execute(
            select(WindowTuneResult)
            .where(WindowTuneResult.rule_id == rule_id)
            .order_by(WindowTuneResult.evaluated_at.desc(), WindowTuneResult.id.desc())
            .limit(1)
        )
        tune = result.scalar_one_or_none()
        if not tune:
            raise HTTPException(404, "该规则暂无重估结果")

        result = await session.execute(
            select(RateLimitRule).where(RateLimitRule.id == rule_id))
        rule = result.scalar_one_or_none()
        if not rule:
            raise HTTPException(404, "规则不存在")

        rule.window_size = tune.suggested_window_size
        rule.slot_granularity = tune.suggested_slot_granularity
        await session.commit()
        # 增量热更新：滑动窗口走 retune 路径，窗口内计数保留
        await rate_limit_manager.load_rules(session)

    return {
        "message": "已应用重估建议",
        "rule_id": rule_id,
        "window_size": tune.suggested_window_size,
        "slot_granularity": tune.suggested_slot_granularity,
    }


# ==================== 熔断器API ====================

@app.get("/api/circuit-breakers")
async def list_circuit_breakers():
    """列出所有熔断器状态"""
    return circuit_breaker_manager.get_all_states()


@app.put("/api/circuit-breakers/{service_name}")
async def update_circuit_breaker(service_name: str, request: Request):
    """更新熔断器配置"""
    data = await request.json()
    circuit_breaker_manager.update_breaker(service_name, data)

    # 持久化到数据库
    async with async_session() as session:
        result = await session.execute(
            select(CircuitBreakerState).where(CircuitBreakerState.service_name == service_name)
        )
        cb = result.scalar_one_or_none()
        if cb:
            for field in ["failure_threshold", "recovery_timeout", "half_open_max_calls", "success_threshold", "enabled"]:
                if field in data:
                    setattr(cb, field, data[field])
            await session.commit()

    return {"message": "熔断器配置更新成功"}


@app.post("/api/circuit-breakers/{service_name}/reset")
async def reset_circuit_breaker(service_name: str):
    """手动重置熔断器为关闭状态"""
    circuit_breaker_manager.reset_breaker(service_name)

    async with async_session() as session:
        result = await session.execute(
            select(CircuitBreakerState).where(CircuitBreakerState.service_name == service_name)
        )
        cb = result.scalar_one_or_none()
        if cb:
            cb.state = "closed"
            cb.failure_count = 0
            cb.success_count = 0
            await session.commit()

    return {"message": f"熔断器 {service_name} 已重置为关闭状态"}


@app.post("/api/circuit-breakers")
async def add_circuit_breaker(request: Request):
    """添加新的熔断器"""
    data = await request.json()
    required = ["service_name", "backend_url"]
    for field in required:
        if field not in data:
            raise HTTPException(400, f"缺少必填字段: {field}")

    circuit_breaker_manager.add_breaker(data["service_name"], data["backend_url"], data)

    async with async_session() as session:
        cb = CircuitBreakerState(
            service_name=data["service_name"],
            backend_url=data["backend_url"],
            failure_threshold=data.get("failure_threshold", 5),
            recovery_timeout=data.get("recovery_timeout", 30),
            half_open_max_calls=data.get("half_open_max_calls", 3),
            success_threshold=data.get("success_threshold", 2),
            enabled=data.get("enabled", True),
        )
        session.add(cb)
        await session.commit()

    return {"message": "熔断器添加成功"}


@app.delete("/api/circuit-breakers/{service_name}")
async def delete_circuit_breaker(service_name: str):
    """删除熔断器"""
    circuit_breaker_manager.remove_breaker(service_name)

    async with async_session() as session:
        result = await session.execute(
            select(CircuitBreakerState).where(CircuitBreakerState.service_name == service_name)
        )
        cb = result.scalar_one_or_none()
        if cb:
            await session.delete(cb)
            await session.commit()

    return {"message": f"熔断器 {service_name} 已删除"}


# ==================== 前端页面 ====================

@app.get("/", response_class=HTMLResponse)
async def index():
    """Dashboard首页"""
    with open("frontend/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


# ==================== 启动入口 ====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=APP_HOST, port=APP_PORT, reload=True, log_level="info")
