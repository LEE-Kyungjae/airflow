"""
Auto Discovery Service - Zero-Config Source Registration

URL만 입력하면 AI가 자동으로:
1. 페이지 타입 분석 (뉴스, 테이블, API, 파일)
2. 추출 필드 자동 추천
3. 최적 스케줄 추천
4. 크롤링 전략 결정
"""

import os
import re
import json
import logging
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup
from openai import OpenAI

logger = logging.getLogger(__name__)


class PageType(str, Enum):
    """페이지 타입 분류"""
    NEWS_LIST = "news_list"           # 뉴스 목록 페이지
    NEWS_ARTICLE = "news_article"     # 뉴스 상세 페이지
    DATA_TABLE = "data_table"         # 데이터 테이블
    FINANCIAL_DATA = "financial_data" # 금융/시황 데이터
    API_ENDPOINT = "api_endpoint"     # REST API
    FILE_DOWNLOAD = "file_download"   # 파일 다운로드
    DYNAMIC_SPA = "dynamic_spa"       # SPA/동적 페이지
    UNKNOWN = "unknown"


class CrawlStrategy(str, Enum):
    """크롤링 전략"""
    STATIC_HTML = "static_html"       # 정적 HTML (requests + bs4)
    DYNAMIC_JS = "dynamic_js"         # 동적 페이지 (Selenium)
    API_CALL = "api_call"             # API 직접 호출
    FILE_EXTRACT = "file_extract"     # 파일 다운로드 후 추출
    HYBRID = "hybrid"                 # 혼합 전략


@dataclass
class DiscoveredField:
    """자동 발견된 필드"""
    name: str
    selector: str
    data_type: str  # string, number, date, url
    sample_value: str
    confidence: float  # 0.0 ~ 1.0
    is_list: bool = False
    extraction_method: str = "text"  # text, attribute, href


@dataclass
class SourceDiscoveryResult:
    """소스 자동 발견 결과"""
    url: str
    page_type: PageType
    strategy: CrawlStrategy
    requires_js: bool
    has_pagination: bool
    pagination_type: Optional[str]  # next_button, page_param, infinite_scroll
    recommended_fields: List[DiscoveredField]
    recommended_schedule: str
    schedule_reason: str
    data_freshness: str  # realtime, hourly, daily, weekly
    estimated_records: int
    sample_data: List[Dict[str, Any]]
    confidence_score: float
    warnings: List[str]
    metadata: Dict[str, Any]


class AutoDiscoveryService:
    """자동 소스 발견 서비스"""

    # 뉴스 사이트 패턴
    NEWS_PATTERNS = [
        r'news', r'article', r'press', r'media', r'report',
        r'뉴스', r'기사', r'보도', r'시황', r'속보'
    ]

    # 금융 데이터 패턴
    FINANCIAL_PATTERNS = [
        r'stock', r'market', r'finance', r'price', r'index',
        r'주식', r'시장', r'금융', r'시세', r'지수', r'환율', r'코스피', r'코스닥'
    ]

    # 스케줄 추천 규칙
    SCHEDULE_RULES = {
        'realtime': '*/5 * * * *',      # 5분마다
        'frequent': '*/15 * * * *',     # 15분마다
        'hourly': '0 * * * *',          # 매시간
        'business_hours': '*/30 9-18 * * 1-5',  # 평일 9-18시 30분마다
        'daily_morning': '0 8 * * *',   # 매일 아침 8시
        'daily_evening': '0 18 * * *',  # 매일 저녁 6시
        'weekly': '0 9 * * 1',          # 매주 월요일 9시
    }

    def __init__(self):
        self.api_key = os.getenv('OPENAI_API_KEY')
        self.model = os.getenv('AI_MODEL', 'gpt-4o-mini')
        if self.api_key:
            client_kwargs = {"api_key": self.api_key}
            ai_base_url = os.getenv('AI_BASE_URL')
            if ai_base_url:
                client_kwargs["base_url"] = ai_base_url
            self.client = OpenAI(**client_kwargs)
        else:
            self.client = None
            logger.warning("OpenAI API key not set")

    async def discover(
        self,
        url: str,
        hint: Optional[str] = None,
        deep_analysis: bool = True
    ) -> SourceDiscoveryResult:
        """
        URL 자동 분석 및 소스 발견

        Args:
            url: 분석할 URL
            hint: 사용자 힌트 (예: "뉴스", "주가", "환율")
            deep_analysis: GPT 심층 분석 여부
        """
        logger.info(f"Starting auto-discovery for: {url}")

        # 1. 기본 페이지 fetch
        html_content, response_headers, fetch_time = await self._fetch_page(url)

        # 2. 페이지 타입 판별
        page_type = self._detect_page_type(url, html_content, hint)

        # 3. 동적 페이지 여부 확인
        requires_js = self._check_requires_javascript(html_content)

        # 4. 페이지네이션 분석
        has_pagination, pagination_type = self._detect_pagination(html_content)

        # 5. 크롤링 전략 결정
        strategy = self._determine_strategy(page_type, requires_js, response_headers)

        # 6. GPT 기반 필드 자동 추출
        if deep_analysis and self.client:
            discovered_fields, sample_data = await self._gpt_analyze_structure(
                url, html_content, page_type, hint
            )
        else:
            discovered_fields, sample_data = self._basic_field_extraction(
                html_content, page_type
            )

        # 7. 스케줄 추천
        schedule, schedule_reason, freshness = self._recommend_schedule(
            url, page_type, hint
        )

        # 8. 신뢰도 계산
        confidence = self._calculate_confidence(
            discovered_fields, sample_data, page_type
        )

        # 9. 경고 생성
        warnings = self._generate_warnings(
            requires_js, has_pagination, discovered_fields, page_type
        )

        return SourceDiscoveryResult(
            url=url,
            page_type=page_type,
            strategy=strategy,
            requires_js=requires_js,
            has_pagination=has_pagination,
            pagination_type=pagination_type,
            recommended_fields=discovered_fields,
            recommended_schedule=schedule,
            schedule_reason=schedule_reason,
            data_freshness=freshness,
            estimated_records=len(sample_data) * 10,  # 예상치
            sample_data=sample_data[:5],
            confidence_score=confidence,
            warnings=warnings,
            metadata={
                'fetch_time_ms': fetch_time,
                'content_length': len(html_content),
                'analyzed_at': datetime.utcnow().isoformat()
            }
        )

    async def _fetch_page(self, url: str) -> Tuple[str, Dict, int]:
        """페이지 fetch"""
        start = datetime.utcnow()

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        }

        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()

            # 인코딩 처리
            content = response.text

        elapsed = int((datetime.utcnow() - start).total_seconds() * 1000)
        return content, dict(response.headers), elapsed

    def _detect_page_type(
        self,
        url: str,
        html: str,
        hint: Optional[str]
    ) -> PageType:
        """페이지 타입 자동 감지"""
        url_lower = url.lower()
        html_lower = html.lower()

        # 힌트 기반 우선 판별
        if hint:
            hint_lower = hint.lower()
            if any(p in hint_lower for p in ['뉴스', 'news', '기사', 'article']):
                return PageType.NEWS_LIST
            if any(p in hint_lower for p in ['시황', '금융', '주가', 'stock', 'market']):
                return PageType.FINANCIAL_DATA

        # URL 패턴 기반
        if any(re.search(p, url_lower) for p in self.FINANCIAL_PATTERNS):
            return PageType.FINANCIAL_DATA
        if any(re.search(p, url_lower) for p in self.NEWS_PATTERNS):
            return PageType.NEWS_LIST

        # Content-Type이 JSON이면 API
        if 'application/json' in html[:100].lower():
            return PageType.API_ENDPOINT

        # HTML 구조 분석
        soup = BeautifulSoup(html, 'lxml')

        # 테이블이 많으면 데이터 테이블
        tables = soup.find_all('table')
        if len(tables) >= 2 or (tables and len(tables[0].find_all('tr')) > 5):
            return PageType.DATA_TABLE

        # 뉴스 관련 클래스/ID 체크
        news_indicators = soup.find_all(class_=re.compile(r'news|article|headline', re.I))
        if len(news_indicators) > 3:
            return PageType.NEWS_LIST

        # 금융 데이터 지표
        financial_keywords = ['price', 'index', '지수', '시세', '환율', '등락']
        if sum(1 for kw in financial_keywords if kw in html_lower) >= 2:
            return PageType.FINANCIAL_DATA

        return PageType.UNKNOWN

    def _check_requires_javascript(self, html: str) -> bool:
        """JavaScript 필요 여부 확인"""
        indicators = [
            'react', 'vue', 'angular', 'next',
            '__NEXT_DATA__', '__NUXT__', 'window.__INITIAL_STATE__',
            'data-reactroot', 'ng-app', 'v-app',
            'Loading...', '로딩중', 'Please wait'
        ]

        html_lower = html.lower()

        # 콘텐츠가 너무 적으면 JS 렌더링 필요
        soup = BeautifulSoup(html, 'lxml')
        text_content = soup.get_text(strip=True)
        if len(text_content) < 500:
            return True

        return any(ind.lower() in html_lower for ind in indicators)

    def _detect_pagination(self, html: str) -> Tuple[bool, Optional[str]]:
        """페이지네이션 감지"""
        soup = BeautifulSoup(html, 'lxml')

        # 1. 페이지 번호 링크
        page_links = soup.find_all('a', href=re.compile(r'page=\d+|p=\d+|/page/\d+'))
        if page_links:
            return True, 'page_param'

        # 2. Next/이전 버튼
        next_patterns = ['next', 'more', '다음', '더보기', '>>', '›']
        for pattern in next_patterns:
            next_btn = soup.find(['a', 'button'], string=re.compile(pattern, re.I))
            if next_btn:
                return True, 'next_button'

        # 3. 페이지네이션 클래스
        pagination = soup.find(class_=re.compile(r'pagination|pager|page-nav', re.I))
        if pagination:
            return True, 'page_param'

        # 4. 무한 스크롤 힌트
        infinite_indicators = ['infinite', 'scroll-load', 'lazy-load']
        if any(ind in html.lower() for ind in infinite_indicators):
            return True, 'infinite_scroll'

        return False, None

    def _determine_strategy(
        self,
        page_type: PageType,
        requires_js: bool,
        headers: Dict
    ) -> CrawlStrategy:
        """크롤링 전략 결정"""
        content_type = headers.get('content-type', '').lower()

        if 'application/json' in content_type:
            return CrawlStrategy.API_CALL

        if page_type == PageType.FILE_DOWNLOAD:
            return CrawlStrategy.FILE_EXTRACT

        if page_type == PageType.API_ENDPOINT:
            return CrawlStrategy.API_CALL

        if requires_js or page_type == PageType.DYNAMIC_SPA:
            return CrawlStrategy.DYNAMIC_JS

        return CrawlStrategy.STATIC_HTML

    async def _gpt_analyze_structure(
        self,
        url: str,
        html: str,
        page_type: PageType,
        hint: Optional[str]
    ) -> Tuple[List[DiscoveredField], List[Dict]]:
        """GPT 기반 구조 분석"""

        # HTML 정제 (토큰 절약)
        soup = BeautifulSoup(html, 'lxml')

        # 스크립트, 스타일 제거
        for tag in soup(['script', 'style', 'noscript', 'iframe']):
            tag.decompose()

        # 주요 콘텐츠 영역만 추출
        main_content = soup.find(['main', 'article', 'div[class*="content"]', 'div[class*="list"]'])
        if main_content:
            clean_html = str(main_content)[:8000]
        else:
            clean_html = str(soup.body)[:8000] if soup.body else str(soup)[:8000]

        prompt = f"""다음 웹페이지를 분석하여 데이터 추출 구조를 JSON으로 반환하세요.

URL: {url}
페이지 타입: {page_type.value}
사용자 힌트: {hint or '없음'}

[HTML 구조]
{clean_html}

다음 JSON 형식으로 응답하세요:
{{
    "fields": [
        {{
            "name": "필드명 (영문, snake_case)",
            "name_ko": "한글명",
            "selector": "CSS 선택자",
            "data_type": "string|number|date|url",
            "sample_value": "샘플값",
            "confidence": 0.0-1.0,
            "is_list": true|false,
            "extraction_method": "text|attribute|href"
        }}
    ],
    "sample_data": [
        {{"field1": "value1", "field2": "value2"}}
    ],
    "list_container_selector": "반복되는 아이템의 부모 선택자",
    "item_selector": "개별 아이템 선택자"
}}

중요:
1. 뉴스/시황이면 title, date, summary, link, source 필드 필수
2. 금융 데이터면 name, value, change, change_rate 필드 필수
3. 테이블이면 모든 컬럼을 필드로 변환
4. confidence는 해당 선택자의 정확도 추정치
5. sample_data는 실제 추출된 데이터 3-5개

JSON만 출력하세요."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "웹 스크래핑 전문가입니다. 정확한 CSS 선택자와 데이터 구조를 분석합니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=3000,
                temperature=0.1
            )

            result_text = response.choices[0].message.content.strip()

            # JSON 추출
            if '```json' in result_text:
                result_text = result_text.split('```json')[1].split('```')[0]
            elif '```' in result_text:
                result_text = result_text.split('```')[1].split('```')[0]

            result = json.loads(result_text)

            fields = []
            for f in result.get('fields', []):
                fields.append(DiscoveredField(
                    name=f.get('name', 'unknown'),
                    selector=f.get('selector', ''),
                    data_type=f.get('data_type', 'string'),
                    sample_value=str(f.get('sample_value', ''))[:100],
                    confidence=float(f.get('confidence', 0.5)),
                    is_list=f.get('is_list', False),
                    extraction_method=f.get('extraction_method', 'text')
                ))

            sample_data = result.get('sample_data', [])

            return fields, sample_data

        except Exception as e:
            logger.error(f"GPT analysis failed: {e}")
            return self._basic_field_extraction(html, page_type)

    def _basic_field_extraction(
        self,
        html: str,
        page_type: PageType
    ) -> Tuple[List[DiscoveredField], List[Dict]]:
        """기본 필드 추출 (GPT 없이)"""
        soup = BeautifulSoup(html, 'lxml')
        fields = []

        if page_type in [PageType.NEWS_LIST, PageType.NEWS_ARTICLE]:
            # 뉴스 기본 필드
            fields = [
                DiscoveredField("title", "h1, h2, .title, .headline", "string", "", 0.7, False, "text"),
                DiscoveredField("date", "time, .date, .datetime", "date", "", 0.6, False, "text"),
                DiscoveredField("summary", "p, .summary, .desc", "string", "", 0.5, False, "text"),
                DiscoveredField("link", "a[href]", "url", "", 0.8, False, "href"),
            ]
        elif page_type == PageType.FINANCIAL_DATA:
            # 금융 기본 필드
            fields = [
                DiscoveredField("name", "td:first-child, .name, .title", "string", "", 0.7, False, "text"),
                DiscoveredField("value", ".price, .value, td:nth-child(2)", "number", "", 0.6, False, "text"),
                DiscoveredField("change", ".change, .diff, td:nth-child(3)", "number", "", 0.5, False, "text"),
                DiscoveredField("change_rate", ".rate, .percent, td:nth-child(4)", "number", "", 0.5, False, "text"),
            ]
        elif page_type == PageType.DATA_TABLE:
            # 테이블 헤더에서 필드 추출
            table = soup.find('table')
            if table:
                headers = table.find_all('th')
                for i, th in enumerate(headers):
                    name = re.sub(r'\W+', '_', th.get_text(strip=True).lower())[:20] or f'col_{i}'
                    fields.append(DiscoveredField(
                        name=name,
                        selector=f"td:nth-child({i+1})",
                        data_type="string",
                        sample_value="",
                        confidence=0.6,
                        is_list=False,
                        extraction_method="text"
                    ))

        return fields, []

    def _recommend_schedule(
        self,
        url: str,
        page_type: PageType,
        hint: Optional[str]
    ) -> Tuple[str, str, str]:
        """스케줄 추천"""

        # 금융 데이터 - 장중 빈번 수집
        if page_type == PageType.FINANCIAL_DATA:
            if any(kw in url.lower() for kw in ['realtime', '실시간', 'live']):
                return self.SCHEDULE_RULES['realtime'], "실시간 데이터 - 5분 간격 수집", "realtime"
            return self.SCHEDULE_RULES['business_hours'], "금융 데이터 - 장중 30분 간격", "frequent"

        # 뉴스 - 시간당 수집
        if page_type in [PageType.NEWS_LIST, PageType.NEWS_ARTICLE]:
            if hint and any(kw in hint.lower() for kw in ['속보', 'breaking', '실시간']):
                return self.SCHEDULE_RULES['frequent'], "속보성 뉴스 - 15분 간격", "frequent"
            return self.SCHEDULE_RULES['hourly'], "일반 뉴스 - 매시간 수집", "hourly"

        # 데이터 테이블 - 일 1회
        if page_type == PageType.DATA_TABLE:
            return self.SCHEDULE_RULES['daily_morning'], "정적 데이터 - 매일 오전 수집", "daily"

        # 기본값
        return self.SCHEDULE_RULES['hourly'], "기본 스케줄 - 매시간 수집", "hourly"

    def _calculate_confidence(
        self,
        fields: List[DiscoveredField],
        sample_data: List[Dict],
        page_type: PageType
    ) -> float:
        """신뢰도 점수 계산"""
        score = 0.5  # 기본값

        # 필드 수에 따른 가중치
        if len(fields) >= 3:
            score += 0.1
        if len(fields) >= 5:
            score += 0.1

        # 샘플 데이터가 있으면 가중치
        if sample_data and len(sample_data) >= 3:
            score += 0.2

        # 필드 신뢰도 평균
        if fields:
            avg_confidence = sum(f.confidence for f in fields) / len(fields)
            score += avg_confidence * 0.2

        # 페이지 타입이 명확하면 가중치
        if page_type != PageType.UNKNOWN:
            score += 0.1

        return min(1.0, score)

    def _generate_warnings(
        self,
        requires_js: bool,
        has_pagination: bool,
        fields: List[DiscoveredField],
        page_type: PageType
    ) -> List[str]:
        """경고 메시지 생성"""
        warnings = []

        if requires_js:
            warnings.append("JavaScript 렌더링이 필요합니다. Selenium이 사용됩니다.")

        if has_pagination:
            warnings.append("페이지네이션이 감지되었습니다. 전체 데이터 수집을 위해 멀티페이지 크롤링이 필요할 수 있습니다.")

        if len(fields) < 3:
            warnings.append("추출 필드가 적습니다. 수동 검토를 권장합니다.")

        low_confidence_fields = [f for f in fields if f.confidence < 0.5]
        if low_confidence_fields:
            names = ', '.join(f.name for f in low_confidence_fields)
            warnings.append(f"신뢰도가 낮은 필드가 있습니다: {names}")

        if page_type == PageType.UNKNOWN:
            warnings.append("페이지 타입을 자동 감지하지 못했습니다. 수동 설정이 필요할 수 있습니다.")

        return warnings


class BatchDiscoveryService:
    """배치 소스 발견 서비스 - 여러 URL 동시 분석"""

    def __init__(self):
        self.discovery = AutoDiscoveryService()

    async def discover_batch(
        self,
        urls: List[str],
        hints: Optional[Dict[str, str]] = None,
        concurrency: int = 5
    ) -> List[SourceDiscoveryResult]:
        """
        여러 URL 동시 분석

        Args:
            urls: URL 목록
            hints: URL별 힌트 딕셔너리
            concurrency: 동시 처리 수
        """
        hints = hints or {}
        results = []

        semaphore = asyncio.Semaphore(concurrency)

        async def discover_with_limit(url: str):
            async with semaphore:
                try:
                    return await self.discovery.discover(url, hints.get(url))
                except Exception as e:
                    logger.error(f"Failed to discover {url}: {e}")
                    return None

        tasks = [discover_with_limit(url) for url in urls]
        results = await asyncio.gather(*tasks)

        return [r for r in results if r is not None]

    async def discover_from_sitemap(
        self,
        sitemap_url: str,
        pattern: Optional[str] = None,
        limit: int = 50
    ) -> List[SourceDiscoveryResult]:
        """사이트맵에서 URL 추출 후 분석"""

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(sitemap_url)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, 'lxml-xml')
        urls = [loc.text for loc in soup.find_all('loc')]

        if pattern:
            urls = [u for u in urls if re.search(pattern, u)]

        urls = urls[:limit]
        return await self.discover_batch(urls)
