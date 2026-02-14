"""
PostgreSQL Service for production data storage.

전형적인 공통코드 + 도메인 테이블 패턴.

Architecture:
- MongoDB: staging, review workflow, logs, lineage (유연한 스키마)
- PostgreSQL: 승인된 프로덕션 데이터 (구조화, SQL 조회, JOIN)

Tables:
- tb_common_code_group / tb_common_code: 공통코드
- tb_source_master / tb_source_field: 소스 마스터
- tb_data_news / tb_data_finance / tb_data_announcement / tb_data_generic: 도메인 데이터
- tb_promotion_log: 프로모션 이력
"""

import os
import json
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False
    logger.warning("asyncpg not installed. PostgreSQL features disabled.")

from .pg_schema import ALL_DDL, SEED_COMMON_CODES, DOMAIN_TABLE_MAP, DOMAIN_COLUMNS


class PostgresService:
    """
    Async PostgreSQL service for production data.

    Lifecycle:
        pg = PostgresService()
        await pg.connect()    # create pool + init schema
        ...                   # use service
        await pg.disconnect() # cleanup
    """

    def __init__(self):
        self.pool: Optional[Any] = None
        self.dsn = os.getenv(
            "POSTGRES_DSN",
            os.getenv("DATABASE_URL", "postgresql://crawler:crawler@localhost:5432/crawler_production")
        )

    async def connect(self):
        """Create connection pool and initialize schema."""
        if not HAS_ASYNCPG:
            logger.warning("asyncpg not available, PostgreSQL disabled")
            return

        if self.pool:
            return

        try:
            self.pool = await asyncpg.create_pool(
                self.dsn,
                min_size=2,
                max_size=10,
                command_timeout=30,
            )
            logger.info(f"PostgreSQL pool created: {self.dsn.split('@')[-1]}")
            await self._init_schema()
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            self.pool = None

    async def disconnect(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("PostgreSQL pool closed")

    @property
    def is_available(self) -> bool:
        return HAS_ASYNCPG and self.pool is not None

    async def _init_schema(self):
        """Run DDL and seed data on startup."""
        if not self.pool:
            return

        async with self.pool.acquire() as conn:
            # Create all tables
            for ddl in ALL_DDL:
                await conn.execute(ddl)

            # Seed common codes (idempotent)
            await conn.execute(SEED_COMMON_CODES)

            logger.info("PostgreSQL schema initialized (tables + common codes)")

    # ============================================================
    # 공통코드 조회
    # ============================================================

    async def get_common_codes(self, group_code: str) -> List[Dict]:
        """Get all codes for a group."""
        if not self.is_available:
            return []

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT code, code_name, code_name_en, description,
                       extra_value1, extra_value2, sort_order
                FROM tb_common_code
                WHERE group_code = $1 AND is_active = TRUE
                ORDER BY sort_order, code
                """,
                group_code
            )
            return [dict(r) for r in rows]

    async def get_all_code_groups(self) -> List[Dict]:
        """Get all code groups with their codes."""
        if not self.is_available:
            return []

        async with self.pool.acquire() as conn:
            groups = await conn.fetch(
                """
                SELECT g.group_code, g.group_name, g.description,
                       COUNT(c.id) as code_count
                FROM tb_common_code_group g
                LEFT JOIN tb_common_code c ON g.group_code = c.group_code AND c.is_active = TRUE
                WHERE g.is_active = TRUE
                GROUP BY g.group_code, g.group_name, g.description, g.sort_order
                ORDER BY g.sort_order
                """
            )
            return [dict(r) for r in groups]

    # ============================================================
    # 소스 마스터 관리
    # ============================================================

    async def upsert_source(
        self,
        mongo_id: str,
        name: str,
        url: str,
        source_type: str,
        data_category: str = "GENERIC",
        schedule: Optional[str] = None,
        fields: Optional[List[Dict]] = None
    ) -> Optional[int]:
        """
        Upsert source master + field definitions.

        Called when a source is registered or updated in MongoDB.
        Returns the PostgreSQL source_id (tb_source_master.id).
        """
        if not self.is_available:
            return None

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Upsert source master
                source_id = await conn.fetchval(
                    """
                    INSERT INTO tb_source_master (mongo_id, name, url, source_type, data_category, schedule)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (mongo_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        url = EXCLUDED.url,
                        source_type = EXCLUDED.source_type,
                        data_category = EXCLUDED.data_category,
                        schedule = EXCLUDED.schedule,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    mongo_id, name, url, source_type.upper(), data_category.upper(), schedule
                )

                # Upsert field definitions
                if fields:
                    for i, field in enumerate(fields):
                        field_name = field.get("name", "")
                        field_type = field.get("data_type", "string").upper()
                        selector = field.get("selector")
                        is_list = field.get("is_list", False)

                        await conn.execute(
                            """
                            INSERT INTO tb_source_field (source_id, field_name, field_type, selector, is_list, sort_order)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT (source_id, field_name) DO UPDATE SET
                                field_type = EXCLUDED.field_type,
                                selector = EXCLUDED.selector,
                                is_list = EXCLUDED.is_list,
                                sort_order = EXCLUDED.sort_order
                            """,
                            source_id, field_name, field_type, selector, is_list, i
                        )

                return source_id

    async def get_source_pg_id(self, mongo_id: str) -> Optional[int]:
        """Get PostgreSQL source_id from MongoDB ObjectId."""
        if not self.is_available:
            return None

        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT id FROM tb_source_master WHERE mongo_id = $1",
                mongo_id
            )

    async def get_source_category(self, mongo_id: str) -> str:
        """Get data category for a source."""
        if not self.is_available:
            return "GENERIC"

        async with self.pool.acquire() as conn:
            cat = await conn.fetchval(
                "SELECT data_category FROM tb_source_master WHERE mongo_id = $1",
                mongo_id
            )
            return cat or "GENERIC"

    # ============================================================
    # 프로덕션 데이터 저장 (프로모션)
    # ============================================================

    async def promote_to_production(
        self,
        source_mongo_id: str,
        data: Dict[str, Any],
        meta: Dict[str, Any],
        category: Optional[str] = None
    ) -> Optional[Tuple[str, int]]:
        """
        Insert verified data into the appropriate domain table.

        Args:
            source_mongo_id: Source's MongoDB ObjectId string
            data: The actual data fields
            meta: Metadata (review_id, staging_id, verified_by, etc.)
            category: Override data category

        Returns:
            Tuple of (table_name, row_id) or None if failed
        """
        if not self.is_available:
            return None

        try:
            async with self.pool.acquire() as conn:
                # Get source info
                source = await conn.fetchrow(
                    "SELECT id, data_category FROM tb_source_master WHERE mongo_id = $1",
                    source_mongo_id
                )

                if not source:
                    # Auto-register source if not in PG yet
                    logger.warning(f"Source {source_mongo_id} not in PG, using GENERIC")
                    source_pg_id = None
                    cat = category or "GENERIC"
                else:
                    source_pg_id = source["id"]
                    cat = category or source["data_category"] or "GENERIC"

                table_name = DOMAIN_TABLE_MAP.get(cat.upper(), "tb_data_generic")
                domain_cols = DOMAIN_COLUMNS.get(cat.upper(), DOMAIN_COLUMNS["GENERIC"])

                # Build INSERT
                columns = ["source_id", "mongo_review_id", "mongo_prod_id",
                           "data_date", "crawled_at", "verified_at", "verified_by",
                           "has_corrections", "promoted_at"]
                values = [
                    source_pg_id,
                    meta.get("review_id"),
                    meta.get("production_id"),
                    _parse_date(meta.get("data_date")),
                    _parse_ts(meta.get("crawled_at")),
                    _parse_ts(meta.get("verified_at")) or datetime.utcnow(),
                    meta.get("verified_by"),
                    meta.get("has_corrections", False),
                    datetime.utcnow(),
                ]

                # Add domain-specific columns
                if cat.upper() == "GENERIC":
                    # For generic: store entire data as JSONB
                    columns.append("data_json")
                    values.append(json.dumps(data, ensure_ascii=False, default=str))
                    # Also map title/url if present
                    if "title" in data:
                        columns.append("title")
                        values.append(str(data["title"]))
                    if "url" in data:
                        columns.append("url")
                        values.append(str(data["url"]))
                    columns.append("category_code")
                    values.append(cat.upper())
                else:
                    # Map data fields to domain columns
                    for col in domain_cols:
                        val = data.get(col)
                        if val is not None:
                            columns.append(col)
                            values.append(_coerce_value(col, val, cat))

                placeholders = [f"${i+1}" for i in range(len(values))]
                sql = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    RETURNING id
                """

                row_id = await conn.fetchval(sql, *values)

                # Log promotion
                await conn.execute(
                    """
                    INSERT INTO tb_promotion_log
                        (source_id, mongo_review_id, mongo_staging_id, target_table,
                         target_id, reviewer_id, has_corrections, corrections)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    source_pg_id,
                    meta.get("review_id"),
                    meta.get("staging_id"),
                    table_name,
                    row_id,
                    meta.get("verified_by"),
                    meta.get("has_corrections", False),
                    json.dumps(meta.get("corrections", []), ensure_ascii=False, default=str) if meta.get("corrections") else None
                )

                logger.info(f"PG promoted: {table_name}/{row_id} (source={source_mongo_id})")
                return table_name, row_id

        except Exception as e:
            logger.error(f"PG promotion failed: {e}")
            return None

    async def rollback_production(
        self,
        mongo_prod_id: str,
        reason: str
    ) -> bool:
        """Rollback a production record (delete from PG)."""
        if not self.is_available:
            return False

        try:
            async with self.pool.acquire() as conn:
                # Find in promotion log
                log = await conn.fetchrow(
                    "SELECT target_table, target_id FROM tb_promotion_log WHERE mongo_review_id = $1 AND rolled_back_at IS NULL",
                    mongo_prod_id
                )

                if not log:
                    return False

                table_name = log["target_table"]
                target_id = log["target_id"]

                async with conn.transaction():
                    await conn.execute(
                        f"DELETE FROM {table_name} WHERE id = $1",
                        target_id
                    )
                    await conn.execute(
                        """
                        UPDATE tb_promotion_log
                        SET rolled_back_at = NOW(), rollback_reason = $1
                        WHERE target_table = $2 AND target_id = $3
                        """,
                        reason, table_name, target_id
                    )

                logger.info(f"PG rollback: {table_name}/{target_id}")
                return True

        except Exception as e:
            logger.error(f"PG rollback failed: {e}")
            return False

    # ============================================================
    # 프로덕션 데이터 조회
    # ============================================================

    async def query_domain_data(
        self,
        table_name: str,
        filters: Optional[Dict[str, Any]] = None,
        search: Optional[str] = None,
        order_by: str = "promoted_at DESC",
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[Dict], int]:
        """
        Query production data from a domain table.

        Returns (rows, total_count).
        """
        if not self.is_available:
            return [], 0

        where_parts = []
        values = []
        idx = 1

        if filters:
            for key, val in filters.items():
                where_parts.append(f"{key} = ${idx}")
                values.append(val)
                idx += 1

        if search:
            where_parts.append(f"(title ILIKE ${idx} OR CAST(data_json AS TEXT) ILIKE ${idx})")
            values.append(f"%{search}%")
            idx += 1

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        try:
            async with self.pool.acquire() as conn:
                total = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table_name} {where_sql}",
                    *values
                )

                rows = await conn.fetch(
                    f"SELECT * FROM {table_name} {where_sql} ORDER BY {order_by} LIMIT ${idx} OFFSET ${idx + 1}",
                    *values, limit, offset
                )

                return [dict(r) for r in rows], total

        except Exception as e:
            logger.error(f"PG query failed on {table_name}: {e}")
            return [], 0

    async def get_domain_stats(self) -> List[Dict]:
        """Get row counts per domain table."""
        if not self.is_available:
            return []

        stats = []
        try:
            async with self.pool.acquire() as conn:
                for cat, table in DOMAIN_TABLE_MAP.items():
                    try:
                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                        latest = await conn.fetchval(f"SELECT MAX(promoted_at) FROM {table}")
                        stats.append({
                            "category": cat,
                            "table_name": table,
                            "row_count": count,
                            "latest_promotion": latest.isoformat() if latest else None
                        })
                    except Exception:
                        stats.append({
                            "category": cat,
                            "table_name": table,
                            "row_count": 0,
                            "latest_promotion": None
                        })
        except Exception as e:
            logger.error(f"PG stats failed: {e}")

        return stats

    async def get_promotion_history(
        self,
        source_id: Optional[int] = None,
        limit: int = 50
    ) -> List[Dict]:
        """Get recent promotion log entries."""
        if not self.is_available:
            return []

        try:
            async with self.pool.acquire() as conn:
                if source_id:
                    rows = await conn.fetch(
                        """
                        SELECT l.*, s.name as source_name
                        FROM tb_promotion_log l
                        LEFT JOIN tb_source_master s ON l.source_id = s.id
                        WHERE l.source_id = $1
                        ORDER BY l.promoted_at DESC LIMIT $2
                        """,
                        source_id, limit
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT l.*, s.name as source_name
                        FROM tb_promotion_log l
                        LEFT JOIN tb_source_master s ON l.source_id = s.id
                        ORDER BY l.promoted_at DESC LIMIT $1
                        """,
                        limit
                    )
                return [dict(r) for r in rows]

        except Exception as e:
            logger.error(f"PG promotion history failed: {e}")
            return []


# ============================================================
# Value coercion helpers
# ============================================================

def _parse_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        return None


def _parse_ts(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _coerce_value(col_name: str, val: Any, category: str) -> Any:
    """Coerce a value to the expected PostgreSQL type."""
    if val is None:
        return None

    # JSONB columns
    if col_name in ("tags", "attachments", "extra_data"):
        if isinstance(val, (list, dict)):
            return json.dumps(val, ensure_ascii=False, default=str)
        return val

    # Numeric columns
    numeric_cols = {"price", "open_price", "high_price", "low_price", "close_price",
                    "change_amount", "change_rate", "market_cap", "volume"}
    if col_name in numeric_cols:
        try:
            import re
            cleaned = re.sub(r'[^\d.\-]', '', str(val))
            if col_name == "volume":
                return int(float(cleaned)) if cleaned else None
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    # Date columns
    if col_name in ("published_at", "filing_date", "trading_date"):
        return _parse_date(val) or _parse_ts(val)

    # Default: string
    return str(val) if val is not None else None


# ============================================================
# Singleton + FastAPI dependency
# ============================================================

_pg_service: Optional[PostgresService] = None


async def get_pg() -> PostgresService:
    """FastAPI dependency for PostgreSQL service."""
    global _pg_service
    if _pg_service is None:
        _pg_service = PostgresService()
        await _pg_service.connect()
    return _pg_service


async def close_pg():
    """Shutdown hook."""
    global _pg_service
    if _pg_service:
        await _pg_service.disconnect()
        _pg_service = None
