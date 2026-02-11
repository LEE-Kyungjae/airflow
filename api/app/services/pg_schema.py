"""
PostgreSQL Schema Definitions for Production Data.

Architecture:
- tb_common_code_group / tb_common_code: 공통코드 마스터/상세
- tb_source_master / tb_source_field: 소스 마스터/필드 정의
- tb_data_news / tb_data_finance / tb_data_announcement: 도메인별 프로덕션 데이터
- tb_data_generic: 범용 데이터 (JSONB)
- tb_promotion_log: 프로모션 이력
"""

# ============================================================
# 1. 공통코드 테이블
# ============================================================

DDL_COMMON_CODE_GROUP = """
CREATE TABLE IF NOT EXISTS tb_common_code_group (
    group_code      VARCHAR(30) PRIMARY KEY,
    group_name      VARCHAR(100) NOT NULL,
    description     TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    sort_order      INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE tb_common_code_group IS '공통코드 그룹 마스터';
"""

DDL_COMMON_CODE = """
CREATE TABLE IF NOT EXISTS tb_common_code (
    id              SERIAL PRIMARY KEY,
    group_code      VARCHAR(30) NOT NULL REFERENCES tb_common_code_group(group_code),
    code            VARCHAR(50) NOT NULL,
    code_name       VARCHAR(200) NOT NULL,
    code_name_en    VARCHAR(200),
    description     TEXT,
    extra_value1    VARCHAR(200),
    extra_value2    VARCHAR(200),
    is_active       BOOLEAN DEFAULT TRUE,
    sort_order      INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(group_code, code)
);

CREATE INDEX IF NOT EXISTS idx_common_code_group ON tb_common_code(group_code);

COMMENT ON TABLE tb_common_code IS '공통코드 상세';
"""

# ============================================================
# 2. 소스 마스터 테이블
# ============================================================

DDL_SOURCE_MASTER = """
CREATE TABLE IF NOT EXISTS tb_source_master (
    id              SERIAL PRIMARY KEY,
    mongo_id        VARCHAR(24) UNIQUE NOT NULL,
    name            VARCHAR(200) NOT NULL,
    url             TEXT NOT NULL,
    source_type     VARCHAR(30) NOT NULL,
    data_category   VARCHAR(30) NOT NULL DEFAULT 'GENERIC',
    schedule        VARCHAR(50),
    status          VARCHAR(20) DEFAULT 'active',
    description     TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_source_master_category ON tb_source_master(data_category);
CREATE INDEX IF NOT EXISTS idx_source_master_status ON tb_source_master(status);

COMMENT ON TABLE tb_source_master IS '크롤링 소스 마스터';
"""

DDL_SOURCE_FIELD = """
CREATE TABLE IF NOT EXISTS tb_source_field (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER NOT NULL REFERENCES tb_source_master(id),
    field_name      VARCHAR(100) NOT NULL,
    field_type      VARCHAR(30) NOT NULL DEFAULT 'STRING',
    pg_column_name  VARCHAR(100),
    selector        TEXT,
    is_list         BOOLEAN DEFAULT FALSE,
    is_required     BOOLEAN DEFAULT FALSE,
    sort_order      INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(source_id, field_name)
);

CREATE INDEX IF NOT EXISTS idx_source_field_source ON tb_source_field(source_id);

COMMENT ON TABLE tb_source_field IS '소스별 필드 정의 (스키마 매핑)';
"""

# ============================================================
# 3. 도메인 데이터 테이블 - 공통 메타 컬럼
# ============================================================

# 모든 도메인 테이블이 공유하는 공통 컬럼
_COMMON_META_COLUMNS = """
    id              BIGSERIAL PRIMARY KEY,
    source_id       INTEGER REFERENCES tb_source_master(id),
    mongo_review_id VARCHAR(24),
    mongo_prod_id   VARCHAR(24) UNIQUE,
    data_date       DATE,
    crawled_at      TIMESTAMP,
    verified_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    verified_by     VARCHAR(100),
    has_corrections BOOLEAN DEFAULT FALSE,
    promoted_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMP DEFAULT NOW()
"""

# ============================================================
# 3-1. 뉴스 도메인
# ============================================================

DDL_DATA_NEWS = f"""
CREATE TABLE IF NOT EXISTS tb_data_news (
{_COMMON_META_COLUMNS},
    title           TEXT,
    content         TEXT,
    summary         TEXT,
    author          VARCHAR(200),
    press           VARCHAR(200),
    published_at    TIMESTAMP,
    category        VARCHAR(100),
    url             TEXT,
    image_url       TEXT,
    tags            JSONB DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_data_news_source ON tb_data_news(source_id);
CREATE INDEX IF NOT EXISTS idx_data_news_date ON tb_data_news(data_date);
CREATE INDEX IF NOT EXISTS idx_data_news_published ON tb_data_news(published_at);
CREATE INDEX IF NOT EXISTS idx_data_news_category ON tb_data_news(category);

COMMENT ON TABLE tb_data_news IS '뉴스/기사 프로덕션 데이터';
"""

# ============================================================
# 3-2. 금융 도메인
# ============================================================

DDL_DATA_FINANCE = f"""
CREATE TABLE IF NOT EXISTS tb_data_finance (
{_COMMON_META_COLUMNS},
    ticker          VARCHAR(20),
    name            VARCHAR(200),
    price           NUMERIC(18, 4),
    open_price      NUMERIC(18, 4),
    high_price      NUMERIC(18, 4),
    low_price       NUMERIC(18, 4),
    close_price     NUMERIC(18, 4),
    volume          BIGINT,
    change_amount   NUMERIC(18, 4),
    change_rate     NUMERIC(10, 4),
    market_cap      NUMERIC(24, 0),
    market          VARCHAR(50),
    currency        VARCHAR(10) DEFAULT 'KRW',
    trading_date    DATE,
    extra_data      JSONB DEFAULT '{{}}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_data_finance_source ON tb_data_finance(source_id);
CREATE INDEX IF NOT EXISTS idx_data_finance_ticker ON tb_data_finance(ticker);
CREATE INDEX IF NOT EXISTS idx_data_finance_date ON tb_data_finance(trading_date);
CREATE INDEX IF NOT EXISTS idx_data_finance_market ON tb_data_finance(market);

COMMENT ON TABLE tb_data_finance IS '금융(주식/환율/지수) 프로덕션 데이터';
"""

# ============================================================
# 3-3. 공시 도메인
# ============================================================

DDL_DATA_ANNOUNCEMENT = f"""
CREATE TABLE IF NOT EXISTS tb_data_announcement (
{_COMMON_META_COLUMNS},
    company         VARCHAR(200),
    company_code    VARCHAR(20),
    title           TEXT,
    content         TEXT,
    filing_date     DATE,
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    market          VARCHAR(50),
    url             TEXT,
    attachments     JSONB DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_data_announce_source ON tb_data_announcement(source_id);
CREATE INDEX IF NOT EXISTS idx_data_announce_company ON tb_data_announcement(company_code);
CREATE INDEX IF NOT EXISTS idx_data_announce_filing ON tb_data_announcement(filing_date);
CREATE INDEX IF NOT EXISTS idx_data_announce_category ON tb_data_announcement(category);

COMMENT ON TABLE tb_data_announcement IS '기업공시 프로덕션 데이터';
"""

# ============================================================
# 3-4. 범용 도메인 (다양한 도메인 대응)
# ============================================================

DDL_DATA_GENERIC = f"""
CREATE TABLE IF NOT EXISTS tb_data_generic (
{_COMMON_META_COLUMNS},
    category_code   VARCHAR(30),
    data_json       JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    title           TEXT,
    url             TEXT
);

CREATE INDEX IF NOT EXISTS idx_data_generic_source ON tb_data_generic(source_id);
CREATE INDEX IF NOT EXISTS idx_data_generic_date ON tb_data_generic(data_date);
CREATE INDEX IF NOT EXISTS idx_data_generic_category ON tb_data_generic(category_code);
CREATE INDEX IF NOT EXISTS idx_data_generic_json ON tb_data_generic USING GIN(data_json);

COMMENT ON TABLE tb_data_generic IS '범용 프로덕션 데이터 (JSONB 기반)';
"""

# ============================================================
# 4. 프로모션 이력 테이블
# ============================================================

DDL_PROMOTION_LOG = """
CREATE TABLE IF NOT EXISTS tb_promotion_log (
    id              BIGSERIAL PRIMARY KEY,
    source_id       INTEGER REFERENCES tb_source_master(id),
    mongo_review_id VARCHAR(24),
    mongo_staging_id VARCHAR(24),
    target_table    VARCHAR(63) NOT NULL,
    target_id       BIGINT,
    action          VARCHAR(20) NOT NULL DEFAULT 'promote',
    reviewer_id     VARCHAR(100),
    has_corrections BOOLEAN DEFAULT FALSE,
    corrections     JSONB,
    promoted_at     TIMESTAMP DEFAULT NOW(),
    rolled_back_at  TIMESTAMP,
    rollback_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_promotion_log_source ON tb_promotion_log(source_id);
CREATE INDEX IF NOT EXISTS idx_promotion_log_table ON tb_promotion_log(target_table);
CREATE INDEX IF NOT EXISTS idx_promotion_log_date ON tb_promotion_log(promoted_at);

COMMENT ON TABLE tb_promotion_log IS '데이터 프로모션(승인→프로덕션) 이력';
"""

# ============================================================
# 초기 공통코드 데이터
# ============================================================

SEED_COMMON_CODES = """
-- 소스 유형
INSERT INTO tb_common_code_group (group_code, group_name, description, sort_order)
VALUES ('SOURCE_TYPE', '소스 유형', '크롤링 소스의 데이터 형식', 1)
ON CONFLICT (group_code) DO NOTHING;

INSERT INTO tb_common_code (group_code, code, code_name, code_name_en, sort_order) VALUES
('SOURCE_TYPE', 'HTML', 'HTML 웹페이지', 'HTML Web Page', 1),
('SOURCE_TYPE', 'PDF', 'PDF 문서', 'PDF Document', 2),
('SOURCE_TYPE', 'EXCEL', '엑셀 파일', 'Excel File', 3),
('SOURCE_TYPE', 'CSV', 'CSV 파일', 'CSV File', 4),
('SOURCE_TYPE', 'API', 'REST API', 'REST API', 5),
('SOURCE_TYPE', 'JSON', 'JSON 데이터', 'JSON Data', 6)
ON CONFLICT (group_code, code) DO NOTHING;

-- 필드 타입
INSERT INTO tb_common_code_group (group_code, group_name, description, sort_order)
VALUES ('FIELD_TYPE', '필드 타입', '소스 필드의 데이터 타입', 2)
ON CONFLICT (group_code) DO NOTHING;

INSERT INTO tb_common_code (group_code, code, code_name, code_name_en, sort_order) VALUES
('FIELD_TYPE', 'STRING', '문자열', 'String', 1),
('FIELD_TYPE', 'NUMBER', '숫자', 'Number', 2),
('FIELD_TYPE', 'DATE', '날짜', 'Date', 3),
('FIELD_TYPE', 'BOOLEAN', '논리값', 'Boolean', 4),
('FIELD_TYPE', 'CURRENCY', '통화', 'Currency', 5),
('FIELD_TYPE', 'URL', 'URL', 'URL', 6),
('FIELD_TYPE', 'LIST', '목록', 'List', 7),
('FIELD_TYPE', 'JSON', 'JSON', 'JSON', 8)
ON CONFLICT (group_code, code) DO NOTHING;

-- 리뷰 상태
INSERT INTO tb_common_code_group (group_code, group_name, description, sort_order)
VALUES ('REVIEW_STATUS', '리뷰 상태', '데이터 검증 워크플로우 상태', 3)
ON CONFLICT (group_code) DO NOTHING;

INSERT INTO tb_common_code (group_code, code, code_name, code_name_en, sort_order) VALUES
('REVIEW_STATUS', 'PENDING', '대기', 'Pending', 1),
('REVIEW_STATUS', 'APPROVED', '승인', 'Approved', 2),
('REVIEW_STATUS', 'CORRECTED', '수정승인', 'Corrected', 3),
('REVIEW_STATUS', 'REJECTED', '반려', 'Rejected', 4),
('REVIEW_STATUS', 'ON_HOLD', '보류', 'On Hold', 5)
ON CONFLICT (group_code, code) DO NOTHING;

-- 데이터 카테고리
INSERT INTO tb_common_code_group (group_code, group_name, description, sort_order)
VALUES ('DATA_CATEGORY', '데이터 분류', '크롤링 데이터의 도메인 분류', 4)
ON CONFLICT (group_code) DO NOTHING;

INSERT INTO tb_common_code (group_code, code, code_name, code_name_en, description, sort_order) VALUES
('DATA_CATEGORY', 'NEWS', '뉴스/기사', 'News', 'tb_data_news 테이블에 저장', 1),
('DATA_CATEGORY', 'FINANCE', '금융', 'Finance', 'tb_data_finance 테이블에 저장 (주식/환율/지수)', 2),
('DATA_CATEGORY', 'ANNOUNCEMENT', '공시', 'Announcement', 'tb_data_announcement 테이블에 저장', 3),
('DATA_CATEGORY', 'GENERIC', '범용', 'Generic', 'tb_data_generic 테이블에 JSONB로 저장', 99)
ON CONFLICT (group_code, code) DO NOTHING;

-- 스케줄 유형
INSERT INTO tb_common_code_group (group_code, group_name, description, sort_order)
VALUES ('SCHEDULE_TYPE', '스케줄 유형', '크롤링 실행 주기', 5)
ON CONFLICT (group_code) DO NOTHING;

INSERT INTO tb_common_code (group_code, code, code_name, code_name_en, extra_value1, sort_order) VALUES
('SCHEDULE_TYPE', 'HOURLY', '매시간', 'Hourly', '0 * * * *', 1),
('SCHEDULE_TYPE', 'DAILY', '매일', 'Daily', '0 9 * * *', 2),
('SCHEDULE_TYPE', 'WEEKLY', '매주', 'Weekly', '0 9 * * 1', 3),
('SCHEDULE_TYPE', 'MONTHLY', '매월', 'Monthly', '0 9 1 * *', 4),
('SCHEDULE_TYPE', 'CUSTOM', '커스텀', 'Custom Cron', NULL, 99)
ON CONFLICT (group_code, code) DO NOTHING;

-- 오류 유형
INSERT INTO tb_common_code_group (group_code, group_name, description, sort_order)
VALUES ('ERROR_TYPE', '오류 유형', '크롤링 중 발생하는 오류 분류', 6)
ON CONFLICT (group_code) DO NOTHING;

INSERT INTO tb_common_code (group_code, code, code_name, code_name_en, sort_order) VALUES
('ERROR_TYPE', 'CONNECTION', '연결 오류', 'Connection Error', 1),
('ERROR_TYPE', 'TIMEOUT', '타임아웃', 'Timeout', 2),
('ERROR_TYPE', 'PARSING', '파싱 오류', 'Parsing Error', 3),
('ERROR_TYPE', 'VALIDATION', '검증 오류', 'Validation Error', 4),
('ERROR_TYPE', 'AUTH', '인증 오류', 'Authentication Error', 5),
('ERROR_TYPE', 'RATE_LIMIT', '속도 제한', 'Rate Limited', 6),
('ERROR_TYPE', 'UNKNOWN', '알 수 없음', 'Unknown', 99)
ON CONFLICT (group_code, code) DO NOTHING;
"""

# ============================================================
# 도메인 → 테이블 매핑
# ============================================================

DOMAIN_TABLE_MAP = {
    "NEWS": "tb_data_news",
    "FINANCE": "tb_data_finance",
    "ANNOUNCEMENT": "tb_data_announcement",
    "GENERIC": "tb_data_generic",
}

# 도메인별 특화 컬럼 (공통 메타 제외)
DOMAIN_COLUMNS = {
    "NEWS": [
        "title", "content", "summary", "author", "press",
        "published_at", "category", "url", "image_url", "tags"
    ],
    "FINANCE": [
        "ticker", "name", "price", "open_price", "high_price",
        "low_price", "close_price", "volume", "change_amount",
        "change_rate", "market_cap", "market", "currency",
        "trading_date", "extra_data"
    ],
    "ANNOUNCEMENT": [
        "company", "company_code", "title", "content", "filing_date",
        "category", "sub_category", "market", "url", "attachments"
    ],
    "GENERIC": [
        "category_code", "data_json", "title", "url"
    ],
}

# 전체 DDL 실행 순서
ALL_DDL = [
    DDL_COMMON_CODE_GROUP,
    DDL_COMMON_CODE,
    DDL_SOURCE_MASTER,
    DDL_SOURCE_FIELD,
    DDL_DATA_NEWS,
    DDL_DATA_FINANCE,
    DDL_DATA_ANNOUNCEMENT,
    DDL_DATA_GENERIC,
    DDL_PROMOTION_LOG,
]
