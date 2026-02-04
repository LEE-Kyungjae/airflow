"""
ETL Pipeline - Extract, Transform, Load 완전 자동화

데이터 타입에 맞는 자동 변환 및 DB 적재:
1. 뉴스/기사 텍스트
2. 금융/시황 데이터
3. 테이블 데이터
4. 일반 구조화 데이터
"""

import os
import re
import json
import hashlib
import logging
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List, Union, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from decimal import Decimal
import unicodedata

logger = logging.getLogger(__name__)


class DataCategory(str, Enum):
    """데이터 카테고리"""
    NEWS_ARTICLE = "news_article"
    FINANCIAL_DATA = "financial_data"
    MARKET_INDEX = "market_index"
    EXCHANGE_RATE = "exchange_rate"
    STOCK_PRICE = "stock_price"
    ANNOUNCEMENT = "announcement"
    TABLE_DATA = "table_data"
    GENERIC = "generic"


class QualityLevel(str, Enum):
    """데이터 품질 레벨"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INVALID = "invalid"


@dataclass
class TransformConfig:
    """변환 설정"""
    category: DataCategory
    date_format: str = "%Y-%m-%d %H:%M:%S"
    timezone: str = "Asia/Seoul"
    deduplicate: bool = True
    dedup_fields: List[str] = field(default_factory=list)
    required_fields: List[str] = field(default_factory=list)
    field_mappings: Dict[str, str] = field(default_factory=dict)
    custom_transforms: Dict[str, str] = field(default_factory=dict)
    quality_threshold: float = 0.7


@dataclass
class LoadConfig:
    """적재 설정"""
    collection_name: str
    create_index: bool = True
    index_fields: List[str] = field(default_factory=list)
    ttl_days: Optional[int] = None
    partition_by: Optional[str] = None  # date, month, source
    upsert: bool = True
    upsert_key: List[str] = field(default_factory=list)


@dataclass
class ETLResult:
    """ETL 결과"""
    success: bool
    source_id: str
    category: DataCategory
    extracted_count: int
    transformed_count: int
    loaded_count: int
    duplicate_count: int
    invalid_count: int
    quality_score: float
    errors: List[str]
    warnings: List[str]
    sample_data: List[Dict]
    execution_time_ms: int
    metadata: Dict[str, Any]
    # 변경 감지 통계
    skipped_unchanged: int = 0
    new_records: int = 0
    modified_records: int = 0


class DataTransformer:
    """데이터 변환기"""

    # 한국 날짜 패턴
    KO_DATE_PATTERNS = [
        (r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*(\d{1,2}):(\d{2})', '%Y-%m-%d %H:%M'),
        (r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', '%Y-%m-%d'),
        (r'(\d{4})\.(\d{1,2})\.(\d{1,2})\s*(\d{1,2}):(\d{2})', '%Y-%m-%d %H:%M'),
        (r'(\d{4})\.(\d{1,2})\.(\d{1,2})', '%Y-%m-%d'),
        (r'(\d{4})-(\d{1,2})-(\d{1,2})\s*(\d{1,2}):(\d{2}):(\d{2})', '%Y-%m-%d %H:%M:%S'),
        (r'(\d{4})-(\d{1,2})-(\d{1,2})\s*(\d{1,2}):(\d{2})', '%Y-%m-%d %H:%M'),
        (r'(\d{4})-(\d{1,2})-(\d{1,2})', '%Y-%m-%d'),
        (r'(\d{1,2})/(\d{1,2})/(\d{4})', '%m/%d/%Y'),
        (r'(\d{2}):(\d{2})', '%H:%M'),  # 오늘 시간만
    ]

    # 숫자 정제 패턴
    NUMBER_PATTERNS = {
        'korean': {'억': 100000000, '만': 10000, '천': 1000},
        'symbols': {',': '', '%': '', '원': '', '$': '', '₩': ''},
    }

    def __init__(self, config: TransformConfig):
        self.config = config

    def transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """데이터 변환"""
        transformed = []

        for idx, record in enumerate(raw_data):
            try:
                # 1. 필드 매핑
                record = self._apply_field_mappings(record)

                # 2. 카테고리별 변환
                record = self._category_transform(record)

                # 3. 공통 변환
                record = self._common_transform(record, idx)

                # 4. 메타데이터 추가
                record = self._add_metadata(record)

                # 5. 품질 검증
                quality = self._validate_quality(record)
                record['_quality_score'] = quality
                record['_quality_level'] = self._get_quality_level(quality).value

                if quality >= self.config.quality_threshold:
                    transformed.append(record)
                else:
                    logger.warning(f"Record {idx} failed quality check: {quality}")

            except Exception as e:
                logger.error(f"Transform error at record {idx}: {e}")
                continue

        return transformed

    def _apply_field_mappings(self, record: Dict) -> Dict:
        """필드 매핑 적용"""
        if not self.config.field_mappings:
            return record

        new_record = {}
        for key, value in record.items():
            new_key = self.config.field_mappings.get(key, key)
            new_record[new_key] = value

        return new_record

    def _category_transform(self, record: Dict) -> Dict:
        """카테고리별 변환"""
        category = self.config.category

        if category == DataCategory.NEWS_ARTICLE:
            return self._transform_news(record)
        elif category in [DataCategory.FINANCIAL_DATA, DataCategory.STOCK_PRICE]:
            return self._transform_financial(record)
        elif category == DataCategory.EXCHANGE_RATE:
            return self._transform_exchange_rate(record)
        elif category == DataCategory.MARKET_INDEX:
            return self._transform_market_index(record)
        elif category == DataCategory.ANNOUNCEMENT:
            return self._transform_announcement(record)
        else:
            return record

    def _transform_news(self, record: Dict) -> Dict:
        """뉴스 데이터 변환"""
        # 제목 정제
        if 'title' in record:
            record['title'] = self._clean_text(record['title'])
            record['title_length'] = len(record['title'])

        # 본문 정제
        if 'content' in record:
            record['content'] = self._clean_text(record['content'])
            record['content_length'] = len(record['content'])

            # 요약이 없으면 생성
            if 'summary' not in record:
                record['summary'] = record['content'][:200] + '...' if len(record['content']) > 200 else record['content']

        # 날짜 파싱
        for date_field in ['date', 'published_at', 'created_at', 'datetime']:
            if date_field in record:
                record['published_at'] = self._parse_date(record[date_field])
                break

        # URL 정규화
        if 'link' in record:
            record['url'] = self._normalize_url(record['link'])
        elif 'url' not in record:
            record['url'] = None

        # 고유 ID 생성
        record['content_hash'] = self._generate_content_hash(
            record.get('title', ''),
            record.get('url', '')
        )

        return record

    def _transform_financial(self, record: Dict) -> Dict:
        """금융 데이터 변환"""
        # 가격/수치 변환
        numeric_fields = ['price', 'value', 'open', 'high', 'low', 'close', 'volume',
                        'change', 'change_rate', 'market_cap', 'per', 'pbr', 'eps']

        for field in numeric_fields:
            if field in record:
                record[field] = self._parse_number(record[field])

        # 변동률 계산
        if 'change' in record and 'price' in record:
            if record['price'] and record['change']:
                prev_price = record['price'] - record['change']
                if prev_price != 0:
                    record['change_rate'] = round((record['change'] / prev_price) * 100, 2)

        # 거래일 파싱
        if 'date' in record:
            record['trade_date'] = self._parse_date(record['date'])

        # 종목 코드 정규화
        if 'code' in record:
            record['stock_code'] = self._normalize_stock_code(record['code'])

        return record

    def _transform_exchange_rate(self, record: Dict) -> Dict:
        """환율 데이터 변환"""
        record = self._transform_financial(record)

        # 통화 코드 정규화
        if 'currency' in record:
            record['currency_code'] = record['currency'].upper()[:3]

        # 매매기준율, 송금, 현찰 등 파싱
        rate_fields = ['base_rate', 'buy_rate', 'sell_rate', 'send_rate', 'receive_rate']
        for field in rate_fields:
            if field in record:
                record[field] = self._parse_number(record[field])

        return record

    def _transform_market_index(self, record: Dict) -> Dict:
        """시장 지수 데이터 변환"""
        record = self._transform_financial(record)

        # 지수명 정규화
        index_mappings = {
            '코스피': 'KOSPI',
            '코스닥': 'KOSDAQ',
            '다우존스': 'DJI',
            '나스닥': 'NASDAQ',
            'S&P500': 'SPX',
        }

        if 'name' in record:
            for ko, en in index_mappings.items():
                if ko in record['name']:
                    record['index_code'] = en
                    break

        return record

    def _transform_announcement(self, record: Dict) -> Dict:
        """공시 데이터 변환"""
        record = self._transform_news(record)

        # 공시 유형 분류
        if 'title' in record:
            title = record['title']
            if '실적' in title or '영업' in title:
                record['announcement_type'] = 'earnings'
            elif '배당' in title:
                record['announcement_type'] = 'dividend'
            elif '증자' in title or '감자' in title:
                record['announcement_type'] = 'capital'
            elif '합병' in title or '인수' in title:
                record['announcement_type'] = 'ma'
            else:
                record['announcement_type'] = 'other'

        return record

    def _common_transform(self, record: Dict, idx: int) -> Dict:
        """공통 변환"""
        # 빈 문자열 None 처리
        for key, value in record.items():
            if isinstance(value, str) and value.strip() == '':
                record[key] = None

        # 순서 인덱스
        record['_order_index'] = idx

        return record

    def _add_metadata(self, record: Dict) -> Dict:
        """메타데이터 추가"""
        now = datetime.utcnow()

        record['_crawled_at'] = now
        record['_data_category'] = self.config.category.value
        record['_transform_version'] = '1.0'

        # 데이터 날짜 (published_at 또는 trade_date 기준)
        data_date = record.get('published_at') or record.get('trade_date')
        if data_date:
            if isinstance(data_date, datetime):
                record['_data_date'] = data_date.date().isoformat()
            elif isinstance(data_date, date):
                record['_data_date'] = data_date.isoformat()
        else:
            record['_data_date'] = now.date().isoformat()

        return record

    def _validate_quality(self, record: Dict) -> float:
        """품질 점수 계산 (0.0 ~ 1.0)"""
        score = 1.0
        penalties = []

        # 필수 필드 체크
        for field in self.config.required_fields:
            if field not in record or record[field] is None:
                score -= 0.2
                penalties.append(f"missing_{field}")

        # 데이터 타입 체크
        if self.config.category == DataCategory.NEWS_ARTICLE:
            if not record.get('title'):
                score -= 0.3
            if not record.get('published_at'):
                score -= 0.2
        elif self.config.category in [DataCategory.FINANCIAL_DATA, DataCategory.STOCK_PRICE]:
            if record.get('price') is None:
                score -= 0.3

        # 날짜 유효성 (미래 날짜 체크)
        data_date = record.get('published_at') or record.get('trade_date')
        if data_date and isinstance(data_date, datetime):
            if data_date > datetime.utcnow() + timedelta(days=1):
                score -= 0.3

        return max(0.0, score)

    def _get_quality_level(self, score: float) -> QualityLevel:
        """품질 레벨 반환"""
        if score >= 0.8:
            return QualityLevel.HIGH
        elif score >= 0.6:
            return QualityLevel.MEDIUM
        elif score >= 0.4:
            return QualityLevel.LOW
        else:
            return QualityLevel.INVALID

    def _clean_text(self, text: Any) -> str:
        """텍스트 정제"""
        if not text:
            return ""

        text = str(text)

        # Unicode 정규화
        text = unicodedata.normalize('NFC', text)

        # 불필요한 공백 제거
        text = re.sub(r'\s+', ' ', text)

        # 특수 문자 정리
        text = text.replace('\xa0', ' ')
        text = text.replace('\u200b', '')

        return text.strip()

    def _parse_date(self, value: Any) -> Optional[datetime]:
        """날짜 파싱"""
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if not value:
            return None

        text = str(value).strip()

        # 상대적 시간 처리
        relative_patterns = {
            r'(\d+)분\s*전': lambda m: datetime.now() - timedelta(minutes=int(m.group(1))),
            r'(\d+)시간\s*전': lambda m: datetime.now() - timedelta(hours=int(m.group(1))),
            r'(\d+)일\s*전': lambda m: datetime.now() - timedelta(days=int(m.group(1))),
            r'방금': lambda m: datetime.now(),
            r'오늘': lambda m: datetime.now().replace(hour=0, minute=0, second=0),
            r'어제': lambda m: (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0),
        }

        for pattern, handler in relative_patterns.items():
            match = re.search(pattern, text)
            if match:
                return handler(match)

        # 절대 날짜 패턴 매칭
        for pattern, fmt in self.KO_DATE_PATTERNS:
            match = re.search(pattern, text)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 2:  # 시:분만
                        today = datetime.now()
                        return today.replace(hour=int(groups[0]), minute=int(groups[1]), second=0)
                    elif len(groups) == 3:
                        return datetime(int(groups[0]), int(groups[1]), int(groups[2]))
                    elif len(groups) == 5:
                        return datetime(int(groups[0]), int(groups[1]), int(groups[2]),
                                      int(groups[3]), int(groups[4]))
                    elif len(groups) == 6:
                        return datetime(int(groups[0]), int(groups[1]), int(groups[2]),
                                      int(groups[3]), int(groups[4]), int(groups[5]))
                except Exception:
                    continue

        # ISO 형식 시도
        try:
            return datetime.fromisoformat(text.replace('Z', '+00:00'))
        except Exception:
            pass

        return None

    def _parse_number(self, value: Any) -> Optional[float]:
        """숫자 파싱"""
        if value is None:
            return None
        if isinstance(value, (int, float, Decimal)):
            return float(value)

        text = str(value).strip()
        if not text:
            return None

        # 부호 처리
        negative = False
        if text.startswith('-') or text.startswith('▼') or text.startswith('↓'):
            negative = True
            text = text[1:]
        elif text.startswith('+') or text.startswith('▲') or text.startswith('↑'):
            text = text[1:]

        # 기호 제거
        for symbol, replacement in self.NUMBER_PATTERNS['symbols'].items():
            text = text.replace(symbol, replacement)

        # 한국어 단위 처리
        multiplier = 1
        for unit, mult in self.NUMBER_PATTERNS['korean'].items():
            if unit in text:
                text = text.replace(unit, '')
                multiplier = mult
                break

        # 숫자 추출
        try:
            number = float(text.strip())
            number *= multiplier
            return -number if negative else number
        except ValueError:
            # 숫자만 추출 시도
            numbers = re.findall(r'[\d.]+', text)
            if numbers:
                try:
                    number = float(numbers[0])
                    number *= multiplier
                    return -number if negative else number
                except ValueError:
                    pass

        return None

    def _normalize_url(self, url: str) -> str:
        """URL 정규화"""
        if not url:
            return ""

        url = url.strip()

        # 상대 URL 처리는 상위에서
        if not url.startswith(('http://', 'https://')):
            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                pass  # 상대 URL, 상위에서 처리 필요

        return url

    def _normalize_stock_code(self, code: str) -> str:
        """종목 코드 정규화"""
        if not code:
            return ""

        code = str(code).strip()

        # 숫자만 추출
        code = re.sub(r'[^\d]', '', code)

        # 6자리 패딩
        return code.zfill(6)

    def _generate_content_hash(self, *fields) -> str:
        """콘텐츠 해시 생성 (중복 검사용)"""
        combined = '|'.join(str(f) for f in fields if f)
        return hashlib.md5(combined.encode()).hexdigest()


class DataLoader:
    """데이터 적재기 - Staging 컬렉션에 저장"""

    # Staging 컬렉션 매핑 (production → staging)
    STAGING_COLLECTION_MAP = {
        'news_articles': 'staging_news',
        'financial_data': 'staging_financial',
        'stock_prices': 'staging_financial',
        'exchange_rates': 'staging_financial',
        'market_indices': 'staging_financial',
        'announcements': 'staging_news',
        'crawl_data': 'staging_data',
    }

    def __init__(self, mongo_service, config: LoadConfig, use_staging: bool = True):
        self.mongo = mongo_service
        self.config = config
        self.use_staging = use_staging

    async def load(
        self,
        data: List[Dict[str, Any]],
        source_id: str,
        crawl_result_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """데이터를 Staging 컬렉션에 적재"""
        if not data:
            return {'loaded': 0, 'duplicates': 0, 'errors': [], 'staging_ids': []}

        result = {
            'loaded': 0,
            'duplicates': 0,
            'errors': [],
            'upserted_ids': [],
            'staging_ids': []
        }

        # Staging 컬렉션 이름 결정
        base_collection = self._get_collection_name(data[0] if data else {})
        if self.use_staging:
            collection_name = self.STAGING_COLLECTION_MAP.get(base_collection, 'staging_data')
        else:
            collection_name = base_collection

        collection = self.mongo.db[collection_name]

        # 인덱스 생성
        if self.config.create_index:
            await self._ensure_indexes(collection)

        # 배치 처리
        for idx, record in enumerate(data):
            try:
                # 소스 ID 추가
                record['_source_id'] = source_id

                # Staging 메타데이터 추가
                if self.use_staging:
                    record['_review_status'] = 'pending'
                    record['_record_index'] = idx
                    if crawl_result_id:
                        record['_crawl_result_id'] = crawl_result_id

                if self.config.upsert and self.config.upsert_key:
                    # Upsert 모드
                    filter_query = {k: record.get(k) for k in self.config.upsert_key if k in record}

                    if filter_query:
                        update_result = collection.update_one(
                            filter_query,
                            {'$set': record, '$setOnInsert': {'_first_seen': datetime.utcnow()}},
                            upsert=True
                        )

                        if update_result.upserted_id:
                            result['loaded'] += 1
                            result['upserted_ids'].append(str(update_result.upserted_id))
                            result['staging_ids'].append(str(update_result.upserted_id))
                        elif update_result.modified_count > 0:
                            result['loaded'] += 1
                        else:
                            result['duplicates'] += 1
                    else:
                        # 키가 없으면 직접 삽입
                        insert_result = collection.insert_one(record)
                        result['loaded'] += 1
                        result['upserted_ids'].append(str(insert_result.inserted_id))
                        result['staging_ids'].append(str(insert_result.inserted_id))
                else:
                    # 직접 삽입
                    insert_result = collection.insert_one(record)
                    result['loaded'] += 1
                    result['upserted_ids'].append(str(insert_result.inserted_id))
                    result['staging_ids'].append(str(insert_result.inserted_id))

            except Exception as e:
                if 'duplicate key' in str(e).lower():
                    result['duplicates'] += 1
                else:
                    result['errors'].append(str(e)[:200])
                    logger.error(f"Load error: {e}")

        # Staging 컬렉션 정보 추가
        result['collection'] = collection_name
        result['is_staging'] = self.use_staging

        return result

    def _get_collection_name(self, sample_record: Dict) -> str:
        """컬렉션 이름 결정"""
        base_name = self.config.collection_name

        if self.config.partition_by:
            # 날짜 기반 파티셔닝
            if self.config.partition_by == 'date':
                data_date = sample_record.get('_data_date', datetime.utcnow().date().isoformat())
                return f"{base_name}_{data_date.replace('-', '')}"
            elif self.config.partition_by == 'month':
                data_date = sample_record.get('_data_date', datetime.utcnow().date().isoformat())
                return f"{base_name}_{data_date[:7].replace('-', '')}"
            elif self.config.partition_by == 'source':
                source_id = sample_record.get('_source_id', 'unknown')
                return f"{base_name}_{source_id[:8]}"

        return base_name

    async def _ensure_indexes(self, collection):
        """인덱스 생성"""
        try:
            # 기본 인덱스
            collection.create_index('_crawled_at')
            collection.create_index('_source_id')
            collection.create_index('_data_date')

            # 설정된 인덱스
            for field in self.config.index_fields:
                collection.create_index(field)

            # 복합 인덱스 (upsert 키)
            if self.config.upsert_key:
                collection.create_index(
                    [(k, 1) for k in self.config.upsert_key],
                    unique=True,
                    sparse=True
                )

            # TTL 인덱스
            if self.config.ttl_days:
                collection.create_index(
                    '_crawled_at',
                    expireAfterSeconds=self.config.ttl_days * 24 * 60 * 60
                )

        except Exception as e:
            logger.warning(f"Index creation warning: {e}")


class ETLPipeline:
    """완전 자동화 ETL 파이프라인"""

    # 카테고리별 기본 설정
    DEFAULT_CONFIGS = {
        DataCategory.NEWS_ARTICLE: {
            'transform': TransformConfig(
                category=DataCategory.NEWS_ARTICLE,
                required_fields=['title'],
                dedup_fields=['content_hash'],
            ),
            'load': LoadConfig(
                collection_name='news_articles',
                index_fields=['published_at', 'title'],
                upsert=True,
                upsert_key=['content_hash'],
                ttl_days=90,
            )
        },
        DataCategory.FINANCIAL_DATA: {
            'transform': TransformConfig(
                category=DataCategory.FINANCIAL_DATA,
                required_fields=['name'],
            ),
            'load': LoadConfig(
                collection_name='financial_data',
                index_fields=['trade_date', 'stock_code'],
                upsert=True,
                upsert_key=['stock_code', '_data_date'],
                partition_by='date',
            )
        },
        DataCategory.STOCK_PRICE: {
            'transform': TransformConfig(
                category=DataCategory.STOCK_PRICE,
                required_fields=['stock_code', 'price'],
            ),
            'load': LoadConfig(
                collection_name='stock_prices',
                index_fields=['trade_date', 'stock_code'],
                upsert=True,
                upsert_key=['stock_code', '_data_date'],
                partition_by='date',
            )
        },
        DataCategory.EXCHANGE_RATE: {
            'transform': TransformConfig(
                category=DataCategory.EXCHANGE_RATE,
                required_fields=['currency_code'],
            ),
            'load': LoadConfig(
                collection_name='exchange_rates',
                index_fields=['currency_code', '_data_date'],
                upsert=True,
                upsert_key=['currency_code', '_data_date'],
            )
        },
        DataCategory.MARKET_INDEX: {
            'transform': TransformConfig(
                category=DataCategory.MARKET_INDEX,
                required_fields=['index_code'],
            ),
            'load': LoadConfig(
                collection_name='market_indices',
                index_fields=['index_code', '_data_date'],
                upsert=True,
                upsert_key=['index_code', '_data_date'],
            )
        },
        DataCategory.ANNOUNCEMENT: {
            'transform': TransformConfig(
                category=DataCategory.ANNOUNCEMENT,
                required_fields=['title'],
            ),
            'load': LoadConfig(
                collection_name='announcements',
                index_fields=['published_at', 'announcement_type'],
                upsert=True,
                upsert_key=['content_hash'],
                ttl_days=365,
            )
        },
    }

    def __init__(self, mongo_service):
        self.mongo = mongo_service

    async def run(
        self,
        raw_data: List[Dict[str, Any]],
        source_id: str,
        category: Optional[DataCategory] = None,
        transform_config: Optional[TransformConfig] = None,
        load_config: Optional[LoadConfig] = None,
        skip_unchanged: bool = True,
        hash_fields: Optional[List[str]] = None,
        use_staging: bool = True,
        crawl_result_id: Optional[str] = None
    ) -> ETLResult:
        """
        ETL 파이프라인 실행 - 데이터를 Staging에 저장

        Args:
            raw_data: 크롤링된 원본 데이터
            source_id: 소스 ID
            category: 데이터 카테고리
            transform_config: 변환 설정
            load_config: 적재 설정
            skip_unchanged: 변경되지 않은 데이터 스킵 (트래픽 최소화)
            hash_fields: 변경 감지에 사용할 필드 목록
            use_staging: True면 staging 컬렉션에 저장 (기본값), False면 바로 production
            crawl_result_id: 크롤 결과 ID (리뷰 연동용)
        """
        start_time = datetime.utcnow()
        errors = []
        warnings = []
        skipped_unchanged = 0
        new_count = 0
        modified_count = 0

        # 1. 카테고리 자동 감지
        if not category:
            category = self._detect_category(raw_data)
            warnings.append(f"Auto-detected category: {category.value}")

        # 2. 설정 로드
        default_config = self.DEFAULT_CONFIGS.get(category, self.DEFAULT_CONFIGS[DataCategory.NEWS_ARTICLE])

        t_config = transform_config or default_config['transform']
        l_config = load_config or default_config['load']

        # 3. 변경 감지 (skip_unchanged=True일 때)
        data_to_process = raw_data
        if skip_unchanged and self.mongo:
            try:
                from api.app.services.change_detection import ChangeDetectionService

                change_service = ChangeDetectionService(self.mongo)
                change_result = await change_service.check_batch(
                    source_id=source_id,
                    records=raw_data,
                    hash_fields=hash_fields
                )

                # 변경된 레코드만 처리
                data_to_process = change_result.new_records + change_result.modified_records
                skipped_unchanged = change_result.unchanged_count
                new_count = change_result.new_count
                modified_count = change_result.modified_count

                if skipped_unchanged > 0:
                    skip_ratio = round(skipped_unchanged / len(raw_data) * 100, 1)
                    warnings.append(f"Skipped {skipped_unchanged} unchanged records ({skip_ratio}%)")
                    logger.info(f"Change detection: {new_count} new, {modified_count} modified, {skipped_unchanged} skipped")

            except ImportError:
                warnings.append("Change detection service not available, processing all records")
            except Exception as e:
                warnings.append(f"Change detection failed: {str(e)}, processing all records")
                logger.warning(f"Change detection error: {e}")

        # 데이터가 없으면 조기 종료
        if not data_to_process:
            execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            return ETLResult(
                success=True,
                source_id=source_id,
                category=category,
                extracted_count=len(raw_data),
                transformed_count=0,
                loaded_count=0,
                duplicate_count=0,
                invalid_count=0,
                quality_score=1.0,
                errors=[],
                warnings=warnings + ["No changed data to process"],
                sample_data=[],
                execution_time_ms=execution_time,
                metadata={'collection': l_config.collection_name, 'category': category.value, 'transform_version': '1.0'},
                skipped_unchanged=skipped_unchanged,
                new_records=new_count,
                modified_records=modified_count
            )

        # 4. Transform
        transformer = DataTransformer(t_config)
        transformed_data = transformer.transform(data_to_process)

        invalid_count = len(data_to_process) - len(transformed_data)
        if invalid_count > 0:
            warnings.append(f"{invalid_count} records failed quality check")

        # 5. Deduplicate (배치 내 중복 제거)
        if t_config.deduplicate and t_config.dedup_fields:
            before_dedup = len(transformed_data)
            transformed_data = self._deduplicate(transformed_data, t_config.dedup_fields)
            dup_count = before_dedup - len(transformed_data)
        else:
            dup_count = 0

        # 6. Load to Staging (or Production if use_staging=False)
        loader = DataLoader(self.mongo, l_config, use_staging=use_staging)
        load_result = await loader.load(transformed_data, source_id, crawl_result_id)

        errors.extend(load_result.get('errors', []))

        # 7. 해시 업데이트 (성공적으로 저장된 경우)
        if skip_unchanged and self.mongo and load_result['loaded'] > 0:
            try:
                from api.app.services.change_detection import ChangeDetectionService

                change_service = ChangeDetectionService(self.mongo)
                await change_service.update_hashes(
                    source_id=source_id,
                    records=transformed_data,
                    hash_fields=hash_fields
                )
            except Exception as e:
                warnings.append(f"Hash update failed: {str(e)}")

        # 8. Staging 데이터에 대한 Review 레코드 생성
        if use_staging and load_result['loaded'] > 0 and load_result.get('staging_ids'):
            try:
                review_count = await self._create_review_records(
                    source_id=source_id,
                    staging_ids=load_result['staging_ids'],
                    transformed_data=transformed_data,
                    crawl_result_id=crawl_result_id
                )
                if review_count > 0:
                    warnings.append(f"Created {review_count} review records for staging data")
            except Exception as e:
                warnings.append(f"Review record creation failed: {str(e)}")
                logger.error(f"Failed to create review records: {e}")

        # 9. 결과 생성
        execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

        # 품질 점수 계산
        quality_scores = [r.get('_quality_score', 0) for r in transformed_data]
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0

        return ETLResult(
            success=len(errors) == 0 and (load_result['loaded'] > 0 or skipped_unchanged > 0),
            source_id=source_id,
            category=category,
            extracted_count=len(raw_data),
            transformed_count=len(transformed_data),
            loaded_count=load_result['loaded'],
            duplicate_count=dup_count + load_result.get('duplicates', 0),
            invalid_count=invalid_count,
            quality_score=round(avg_quality, 3),
            errors=errors,
            warnings=warnings,
            sample_data=transformed_data[:3] if transformed_data else [],
            execution_time_ms=execution_time,
            metadata={
                'collection': load_result.get('collection', l_config.collection_name),
                'category': category.value,
                'transform_version': '1.0',
                'change_detection_enabled': skip_unchanged,
                'is_staging': load_result.get('is_staging', use_staging),
                'staging_ids': load_result.get('staging_ids', []),
                'crawl_result_id': crawl_result_id
            },
            skipped_unchanged=skipped_unchanged,
            new_records=new_count,
            modified_records=modified_count
        )

    async def _create_review_records(
        self,
        source_id: str,
        staging_ids: List[str],
        transformed_data: List[Dict],
        crawl_result_id: Optional[str] = None
    ) -> int:
        """
        Staging 데이터에 대한 Review 레코드 생성

        검토 대기열에 자동으로 등록하여 사람이 검토할 수 있게 함
        """
        from bson import ObjectId

        created_count = 0
        reviews_collection = self.mongo.db['data_reviews']

        for idx, (staging_id, record) in enumerate(zip(staging_ids, transformed_data)):
            try:
                # 신뢰도 정보 추출
                confidence = record.get('confidence', record.get('_confidence'))
                ocr_conf = record.get('ocr_confidence', record.get('_ocr_confidence'))
                ai_conf = record.get('ai_confidence', record.get('_ai_confidence'))
                needs_review = record.get('needs_number_review', False)
                uncertain = record.get('uncertain_numbers', [])

                # Review 레코드 생성
                review_doc = {
                    'staging_id': ObjectId(staging_id),
                    'source_id': ObjectId(source_id) if not isinstance(source_id, ObjectId) else source_id,
                    'data_record_index': idx,
                    'review_status': 'pending',
                    'original_data': {k: v for k, v in record.items() if not k.startswith('_')},
                    'confidence_score': confidence,
                    'ocr_confidence': ocr_conf,
                    'ai_confidence': ai_conf,
                    'needs_number_review': needs_review,
                    'uncertain_numbers': uncertain,
                    'source_highlights': record.get('_highlights', []),
                    'created_at': datetime.utcnow()
                }

                if crawl_result_id:
                    review_doc['crawl_result_id'] = ObjectId(crawl_result_id) if not isinstance(crawl_result_id, ObjectId) else crawl_result_id

                reviews_collection.insert_one(review_doc)
                created_count += 1

            except Exception as e:
                logger.warning(f"Failed to create review for staging_id {staging_id}: {e}")
                continue

        return created_count

    def _detect_category(self, data: List[Dict]) -> DataCategory:
        """데이터 카테고리 자동 감지"""
        if not data:
            return DataCategory.GENERIC

        sample = data[0]
        fields = set(sample.keys())

        # 필드 기반 판별
        if {'title', 'content', 'summary'} & fields:
            if 'announcement_type' in fields or '공시' in str(sample):
                return DataCategory.ANNOUNCEMENT
            return DataCategory.NEWS_ARTICLE

        if {'price', 'stock_code', 'code'} & fields:
            return DataCategory.STOCK_PRICE

        if {'currency', 'currency_code', 'exchange_rate'} & fields:
            return DataCategory.EXCHANGE_RATE

        if {'index', 'index_code', '지수'} & fields:
            return DataCategory.MARKET_INDEX

        if {'value', 'change', 'change_rate'} & fields:
            return DataCategory.FINANCIAL_DATA

        return DataCategory.GENERIC

    def _deduplicate(self, data: List[Dict], dedup_fields: List[str]) -> List[Dict]:
        """중복 제거"""
        seen = set()
        unique = []

        for record in data:
            key_parts = [str(record.get(f, '')) for f in dedup_fields]
            key = '|'.join(key_parts)

            if key not in seen:
                seen.add(key)
                unique.append(record)

        return unique
