"""数据库连接管理"""
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

logger = logging.getLogger(__name__)

DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=20, max_overflow=10, pool_recycle=3600)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def _column_exists(conn, table: str, column: str) -> bool:
    result = await conn.execute(text(
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND COLUMN_NAME = :c"
    ), {"t": table, "c": column})
    return result.scalar() > 0


async def _ensure_schema_upgrades(conn):
    """轻量列级迁移：create_all 只建表不修改已有表，这里补齐新增列"""
    upgrades = [
        ("rate_limit_rules", "slot_granularity",
         "ALTER TABLE rate_limit_rules ADD COLUMN slot_granularity FLOAT DEFAULT 1.0"),
        ("rate_limit_events", "rule_id",
         "ALTER TABLE rate_limit_events ADD COLUMN rule_id INT NULL, ADD INDEX idx_rule_id (rule_id)"),
    ]
    for table, column, ddl in upgrades:
        try:
            if not await _column_exists(conn, table, column):
                await conn.execute(text(ddl))
                logger.info(f"迁移: {table}.{column} 已添加")
        except Exception as e:
            logger.warning(f"迁移 {table}.{column} 跳过: {e}")

    # 事件时间升级为微秒精度，支撑到达间隔分布计算（已有表 best-effort）
    try:
        await conn.execute(text(
            "ALTER TABLE rate_limit_events MODIFY created_at DATETIME(6) NULL"
        ))
    except Exception as e:
        logger.warning(f"迁移 rate_limit_events.created_at 精度跳过: {e}")


async def init_db():
    """创建所有表"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _ensure_schema_upgrades(conn)
    logger.info("数据库表初始化完成")


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
