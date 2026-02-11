"""
GPT Service for code generation and error recovery.

This module handles all interactions with the OpenAI GPT-4o-mini API
for generating crawler code and fixing errors automatically.

Enhanced with:
- Timeout handling
- Rate limit handling with exponential backoff
- Token counting and cost tracking
- Circuit breaker pattern
- Proper exception handling
"""

import os
import re
import json
import time
import logging
import asyncio
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import wraps

from openai import OpenAI, APIError, APITimeoutError, RateLimitError as OpenAIRateLimitError

logger = logging.getLogger(__name__)


# ============================================
# 토큰 카운팅 (tiktoken 없이 간단 추정)
# ============================================

def estimate_tokens(text: str) -> int:
    """토큰 수 추정 (간단 버전)"""
    # 평균적으로 영어 4자, 한글 2자 = 1토큰
    # 보수적으로 추정
    return len(text) // 3


# ============================================
# GPT 관련 예외 클래스
# ============================================

class GPTServiceError(Exception):
    """GPT 서비스 오류"""
    def __init__(self, operation: str, reason: str, retryable: bool = True):
        self.operation = operation
        self.reason = reason
        self.retryable = retryable
        self.error_code = "S001"
        super().__init__(f"GPT 서비스 오류 ({operation}): {reason}")


class GPTTimeoutError(GPTServiceError):
    """GPT API 타임아웃"""
    def __init__(self, operation: str, timeout: int):
        super().__init__(operation, f"타임아웃 ({timeout}초)", retryable=True)
        self.timeout = timeout
        self.error_code = "S002"


class GPTRateLimitError(GPTServiceError):
    """GPT API 속도 제한"""
    def __init__(self, retry_after: Optional[int] = None):
        super().__init__("api_call", "속도 제한 도달", retryable=True)
        self.retry_after = retry_after or 60
        self.error_code = "S003"


class GPTTokenLimitError(GPTServiceError):
    """GPT 토큰 한도 초과"""
    def __init__(self, requested: int, limit: int):
        super().__init__("api_call", f"토큰 한도 초과: {requested}/{limit}", retryable=False)
        self.requested = requested
        self.limit = limit
        self.error_code = "S004"


class GPTInvalidResponseError(GPTServiceError):
    """GPT 응답 파싱 실패"""
    def __init__(self, expected_format: str, raw_response: str):
        super().__init__("parse", f"응답 형식 오류: {expected_format} 예상", retryable=True)
        self.expected_format = expected_format
        self.raw_response = raw_response[:500]
        self.error_code = "S005"


# ============================================
# Circuit Breaker (간단 구현)
# ============================================

class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """간단한 Circuit Breaker"""
    failure_threshold: int = 3
    reset_timeout: int = 120
    _failures: int = 0
    _state: CircuitState = CircuitState.CLOSED
    _last_failure_time: Optional[float] = None

    def record_success(self):
        self._failures = 0
        self._state = CircuitState.CLOSED

    def record_failure(self):
        self._failures += 1
        self._last_failure_time = time.time()
        if self._failures >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(f"Circuit OPEN: {self._failures}회 연속 실패")

    def allow_request(self) -> bool:
        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            if self._last_failure_time and \
               time.time() - self._last_failure_time >= self.reset_timeout:
                self._state = CircuitState.HALF_OPEN
                return True
            return False

        # HALF_OPEN
        return True


# 전역 Circuit Breaker
_gpt_circuit = CircuitBreaker(failure_threshold=3, reset_timeout=120)


# ============================================
# 비용 추적
# ============================================

@dataclass
class GPTUsageStats:
    """GPT 사용량 통계"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0

    # gpt-4o-mini 가격 (1K 토큰당 USD)
    INPUT_PRICE_PER_1K = 0.00015
    OUTPUT_PRICE_PER_1K = 0.0006

    def record_usage(self, input_tokens: int, output_tokens: int):
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        cost = (input_tokens / 1000 * self.INPUT_PRICE_PER_1K +
                output_tokens / 1000 * self.OUTPUT_PRICE_PER_1K)
        self.total_cost_usd += cost

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_cost_usd": round(self.total_cost_usd, 4)
        }


# 전역 사용량 통계
_usage_stats = GPTUsageStats()


class GPTService:
    """Service for interacting with OpenAI GPT API."""

    # ============================================
    # 시스템 프롬프트
    # ============================================

    SYSTEM_PROMPT_CRAWLER = """당신은 Python 웹 크롤링 전문가입니다. 10년 이상의 크롤링 경험을 가지고 있으며,
다양한 웹사이트 구조(뉴스, 금융, 공시, 테이블, SPA)에 대한 깊은 이해를 바탕으로
안정적이고 정확한 크롤러 코드를 생성합니다.

핵심 원칙:
- 정확한 CSS 셀렉터 사용 (클래스명 > 태그 구조 > nth-child)
- 견고한 에러 처리 (네트워크, 파싱, 인코딩)
- 빈 결과 방지 (폴백 셀렉터, None 체크)
- 인코딩 자동 감지 (charset, apparent_encoding)
- User-Agent 및 적절한 헤더 설정"""

    # ============================================
    # 사이트 유형별 특화 프롬프트
    # ============================================

    SITE_TYPE_PROMPTS = {
        'news_list': """[뉴스 목록 크롤링 특화 지침]
- 기사 목록 컨테이너를 먼저 찾고, 각 기사 아이템을 반복
- 제목은 보통 <a> 또는 <h2>/<h3> 태그 내부
- 날짜는 <span class="date">, <time>, data-* 속성 확인
- 링크는 절대 경로로 변환 (urljoin 사용)
- 썸네일 이미지는 src 또는 data-src 속성 확인
- 언론사명은 보통 별도 span/a 태그""",

        'news_article': """[뉴스 상세 크롤링 특화 지침]
- 기사 본문은 보통 <article>, <div class="article-body"> 등
- 본문 내 불필요한 광고/관련기사 태그 제거
- 기자명, 입력일시, 수정일시 분리 추출
- meta 태그에서 og:title, og:description 폴백 활용""",

        'financial_data': """[금융 데이터 크롤링 특화 지침]
- 숫자 데이터는 쉼표 제거 후 float/int 변환
- 등락률에서 +/- 부호 보존
- 거래량, 시가총액 등 큰 숫자 처리
- 장 마감 여부에 따른 데이터 상태 확인
- 테이블 헤더와 데이터 행 매핑 정확히""",

        'data_table': """[데이터 테이블 크롤링 특화 지침]
- <table> 태그 찾기: id, class 기반으로 정확한 테이블 특정
- <thead>/<th>로 컬럼 헤더 추출
- <tbody>/<tr>/<td>로 데이터 행 추출
- colspan/rowspan 처리 고려
- 빈 셀은 None 또는 빈 문자열로 처리
- 숫자 컬럼은 타입 변환""",

        'announcement': """[공시/공고 크롤링 특화 지침]
- 공시 목록은 보통 테이블 형식
- 회사명, 공시제목, 공시일자, 공시유형 추출
- 상세 링크는 JavaScript 호출인 경우가 많음 (onclick 파싱)
- 날짜 형식 통일 (YYYY-MM-DD)
- 공시 유형 코드와 텍스트 매핑""",
    }

    # ============================================
    # 고도화된 프롬프트 템플릿
    # ============================================

    CRAWL_CODE_PROMPT = """아래 요구사항에 맞는 크롤링 코드를 생성하세요.

[소스 정보]
- URL: {url}
- 데이터 타입: {data_type}
- 페이지 유형: {page_type}
- 추출 필드: {fields}

{site_type_guide}

[HTML 구조 분석]
{html_analysis}

[필수 요구사항]
1. 함수명: crawl_{source_id}() → List[Dict]
2. requests + BeautifulSoup 사용
3. 타임아웃 30초, User-Agent 헤더 설정
4. response.encoding 자동 감지 (apparent_encoding 폴백)
5. 각 필드에 대해 None 체크 후 안전하게 추출
6. 추출 실패 시 빈 리스트 반환 (raise 금지)
7. 모든 링크는 절대 경로로 변환 (urllib.parse.urljoin)
8. 숫자 필드는 쉼표 제거 후 타입 변환
9. 날짜 필드는 ISO 형식(YYYY-MM-DD)으로 정규화
10. 최소 logging.getLogger(__name__) 사용

[코드 구조]
```
import requests
from bs4 import BeautifulSoup
import logging
from typing import List, Dict
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

def crawl_{{source_id}}() -> List[Dict]:
    url = "..."
    headers = {{"User-Agent": "..."}}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, 'html.parser')
        # 데이터 추출
        results = []
        # ... 반복 추출 ...
        return results
    except Exception as e:
        logger.error(f"크롤링 실패: {{e}}")
        return []
```

코드만 출력하세요. 설명 불필요. ```python 과 ``` 없이 순수 코드만 출력하세요."""

    PLAYWRIGHT_CODE_PROMPT = """동적 웹 페이지를 크롤링하는 Playwright 기반 Python 코드를 생성하세요.

[소스 정보]
- URL: {url}
- 데이터 타입: {data_type}
- 페이지 유형: {page_type}
- 추출 필드: {fields}
- 동적 페이지 사유: {js_reason}

{site_type_guide}

[HTML 구조 분석]
{html_analysis}

[페이지네이션 정보]
{pagination_info}

[필수 요구사항]
1. 함수명: crawl_{source_id}() → List[Dict]
2. playwright.sync_api 사용 (동기 모드)
3. Chromium headless 모드
4. 페이지 로드 대기: networkidle 또는 특정 셀렉터 wait_for_selector
5. 적절한 타임아웃 설정 (30초)
6. 브라우저 리소스 정리 (finally 블록에서 browser.close())
7. 각 필드에 대해 None 체크 후 안전하게 추출
8. 추출 실패 시 빈 리스트 반환
9. 모든 링크는 절대 경로로 변환
10. 숫자/날짜 필드 정규화

[코드 구조]
```
from playwright.sync_api import sync_playwright
import logging
from typing import List, Dict
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

def crawl_{{source_id}}() -> List[Dict]:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_extra_http_headers({{"User-Agent": "..."}})
            page.goto("...", wait_until="networkidle", timeout=30000)
            # 데이터 추출
            # ...
            return results
        except Exception as e:
            logger.error(f"크롤링 실패: {{e}}")
            return results
        finally:
            browser.close()
```

코드만 출력하세요. 설명 불필요. ```python 과 ``` 없이 순수 코드만 출력하세요."""

    PAGINATION_SNIPPET_PROMPT = """다음 크롤러 코드에 페이지네이션 로직을 추가하세요.

[기존 코드]
{base_code}

[페이지네이션 정보]
- 타입: {pagination_type}
- 셀렉터/파라미터: {pagination_selector}
- 최대 페이지: {max_pages}

[페이지네이션 타입별 구현]
1. next_button: 다음 버튼 클릭 반복 (Playwright용)
2. page_param: URL 파라미터 변경 (requests용: ?page=1, ?page=2, ...)
3. infinite_scroll: 스크롤 다운 반복 (Playwright용: page.evaluate로 스크롤)

[요구사항]
- 최대 {max_pages}페이지까지만 크롤링
- 페이지 간 1~2초 딜레이 (time.sleep)
- 새 데이터가 없으면 조기 종료
- 중복 데이터 제거 (제목 또는 URL 기반)
- 전체 결과를 하나의 리스트로 합쳐서 반환

수정된 전체 코드를 출력하세요. ```python 과 ``` 없이."""

    HTML_ANALYSIS_PROMPT = """다음 HTML 구조를 분석하여 크롤링에 필요한 정보를 JSON으로 제공하세요.

[URL]: {url}
[HTML (처음 15000자)]
{html_content}

다음을 분석하세요:
1. 데이터가 포함된 주요 컨테이너 (반복되는 아이템의 부모 요소)
2. 각 데이터 아이템의 구조 (어떤 태그에 어떤 데이터가 있는지)
3. 페이지 유형 감지 (뉴스목록, 데이터테이블, 금융데이터, 공시, 일반)
4. JavaScript 필요 여부 (빈 body, noscript 태그, data-react/vue/angular 속성)
5. 페이지네이션 존재 여부 및 타입

JSON 형식으로 응답:
{{
    "page_type": "news_list|data_table|financial_data|announcement|generic",
    "requires_javascript": true|false,
    "js_indicators": ["발견된 JS 프레임워크 지표"],
    "main_container": {{
        "selector": "데이터 컨테이너 CSS 셀렉터",
        "tag": "태그명",
        "item_count": 0
    }},
    "item_selector": "개별 아이템 CSS 셀렉터",
    "fields": [
        {{
            "name": "필드명",
            "selector": "CSS 셀렉터 (컨테이너 기준 상대)",
            "data_type": "string|number|date|url",
            "extraction_method": "text|attribute|href",
            "attribute_name": "속성명 (attribute 방식일 때)",
            "sample_value": "샘플 값",
            "confidence": 0.95
        }}
    ],
    "pagination": {{
        "exists": true|false,
        "type": "next_button|page_param|infinite_scroll|none",
        "selector": "페이지네이션 셀렉터",
        "page_param": "URL 파라미터명"
    }},
    "encoding": "감지된 인코딩",
    "warnings": ["주의사항"]
}}

JSON만 출력하세요."""

    ERROR_FIX_PROMPT = """크롤링 코드에서 오류가 발생했습니다. 수정된 코드를 생성하세요.

[오류 정보]
- 오류 코드: {error_code}
- 오류 메시지: {error_message}
- 스택 트레이스: {stack_trace}

[기존 코드]
{current_code}

[현재 페이지 HTML (처음 5000자)]
{html_snapshot}

{previous_html_diff}

[지시사항]
1. 오류 원인을 분석하세요
2. HTML 구조가 변경되었다면 새로운 셀렉터를 찾으세요
3. 수정된 전체 코드를 출력하세요
4. 변경된 부분에 # FIXED: 주석을 추가하세요
5. 동일 오류 재발 방지를 위한 폴백 로직을 추가하세요

수정된 코드만 출력하세요. ```python 과 ``` 없이 순수 코드만 출력하세요."""

    ANALYZE_STRUCTURE_PROMPT = """다음 웹 페이지의 HTML 구조를 분석하고 데이터 추출에 적합한 CSS 선택자를 제안하세요.

[URL]: {url}
[HTML (처음 10000자)]
{html_content}

[추출하고 싶은 필드]
{fields}

각 필드에 대해 JSON 형식으로 응답하세요:
{{
    "fields": [
        {{
            "name": "필드명",
            "selector": "CSS 선택자",
            "data_type": "string|number|date",
            "is_list": true|false,
            "extraction_method": "text|attribute|href"
        }}
    ],
    "pagination": {{
        "has_pagination": true|false,
        "next_button_selector": "선택자 또는 null",
        "page_param": "파라미터명 또는 null"
    }},
    "requires_javascript": true|false
}}

JSON만 출력하세요."""

    CODE_REVIEW_PROMPT = """다음 크롤러 코드를 검토하고 문제가 있으면 수정하세요.

[코드]
{code}

[대상 URL]
{url}

[추출 필드]
{fields}

[검토 항목]
1. 함수명이 crawl_ 로 시작하는지
2. 반환값이 List[Dict] 형태인지
3. requests/playwright import가 올바른지
4. 에러 처리가 있는지 (try-except)
5. 타임아웃이 설정되어 있는지
6. User-Agent 헤더가 있는지
7. 인코딩 처리가 있는지
8. 셀렉터가 구체적이고 정확한지 (너무 일반적이지 않은지)
9. None 체크가 되어 있는지
10. 빈 결과 반환이 가능한지

문제가 없으면 그대로 코드를 출력하세요.
문제가 있으면 수정된 전체 코드를 출력하세요. 수정된 부분에 # REVIEWED: 주석을 추가하세요.

코드만 출력하세요. ```python 과 ``` 없이."""

    # 모델별 토큰 한도
    MODEL_TOKEN_LIMITS = {
        "gpt-4o-mini": 128000,
        "gpt-4o": 128000,
        "gpt-4-turbo": 128000,
        "gpt-3.5-turbo": 16385,
    }

    # 재시도 설정
    MAX_RETRIES = 3
    RETRY_DELAYS = [1, 2, 4]  # 지수 백오프

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: int = 60,
        base_url: Optional[str] = None
    ):
        """
        Initialize GPT Service.

        Args:
            api_key: OpenAI API key. If not provided, reads from OPENAI_API_KEY env var.
            model: Model to use. Defaults to AI_MODEL env var or gpt-4o-mini.
            timeout: Request timeout in seconds.
            base_url: API base URL for OpenAI-compatible providers (GLM, DeepSeek, etc).
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key is required")

        self.model = model or os.getenv('AI_MODEL', 'gpt-4o-mini')
        self.timeout = timeout

        client_kwargs = {"api_key": self.api_key, "timeout": timeout}
        ai_base_url = base_url or os.getenv('AI_BASE_URL')
        if ai_base_url:
            client_kwargs["base_url"] = ai_base_url

        self.client = OpenAI(**client_kwargs)
        self.token_limit = self.MODEL_TOKEN_LIMITS.get(self.model, 16000)

    def _call_gpt(
        self,
        prompt: str,
        max_tokens: int = 4000,
        temperature: float = 0.2,
        system_prompt: Optional[str] = None,
        operation: str = "unknown"
    ) -> str:
        """
        Call GPT API with the given prompt.
        Includes retry logic, circuit breaker, and proper exception handling.

        Args:
            prompt: The prompt to send
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature (lower = more deterministic)
            system_prompt: Custom system prompt (optional)
            operation: Operation name for logging

        Returns:
            The generated text response

        Raises:
            GPTTimeoutError: On timeout
            GPTRateLimitError: On rate limit
            GPTTokenLimitError: If request exceeds token limit
            GPTServiceError: On other API errors
        """
        global _gpt_circuit, _usage_stats

        # Circuit Breaker 체크
        if not _gpt_circuit.allow_request():
            raise GPTServiceError(
                operation=operation,
                reason="Circuit breaker OPEN - 서비스 일시 중단",
                retryable=True
            )

        # 토큰 한도 체크
        estimated_input_tokens = estimate_tokens(prompt)
        if system_prompt:
            estimated_input_tokens += estimate_tokens(system_prompt)

        if estimated_input_tokens + max_tokens > self.token_limit:
            raise GPTTokenLimitError(
                requested=estimated_input_tokens + max_tokens,
                limit=self.token_limit
            )

        # 시스템 프롬프트 설정
        sys_prompt = system_prompt or "당신은 Python 크롤링 전문가입니다. 요청에 맞는 코드를 정확하게 생성합니다."

        messages = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": prompt}
        ]

        last_error = None
        _usage_stats.total_requests += 1

        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    timeout=self.timeout
                )

                # 성공 기록
                _gpt_circuit.record_success()
                _usage_stats.successful_requests += 1

                # 토큰 사용량 기록
                if response.usage:
                    _usage_stats.record_usage(
                        response.usage.prompt_tokens,
                        response.usage.completion_tokens
                    )

                logger.info(
                    f"GPT 호출 성공: operation={operation}, "
                    f"tokens={response.usage.prompt_tokens}+{response.usage.completion_tokens if response.usage else 'N/A'}"
                )

                return response.choices[0].message.content.strip()

            except APITimeoutError as e:
                last_error = GPTTimeoutError(operation=operation, timeout=self.timeout)
                _gpt_circuit.record_failure()
                logger.warning(f"GPT 타임아웃 (attempt {attempt + 1}/{self.MAX_RETRIES})")

            except OpenAIRateLimitError as e:
                # Rate limit - Retry-After 헤더 확인
                retry_after = getattr(e, 'retry_after', None)
                last_error = GPTRateLimitError(retry_after=retry_after)
                _gpt_circuit.record_failure()

                if attempt < self.MAX_RETRIES - 1:
                    wait_time = retry_after or self.RETRY_DELAYS[attempt] * 10
                    logger.warning(
                        f"GPT Rate limit, {wait_time}초 후 재시도 "
                        f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    time.sleep(wait_time)
                    continue

            except APIError as e:
                _gpt_circuit.record_failure()

                # 5xx 에러는 재시도
                if hasattr(e, 'status_code') and e.status_code >= 500:
                    last_error = GPTServiceError(
                        operation=operation,
                        reason=f"API 오류: {e.status_code}",
                        retryable=True
                    )

                    if attempt < self.MAX_RETRIES - 1:
                        wait_time = self.RETRY_DELAYS[attempt]
                        logger.warning(
                            f"GPT API 오류 ({e.status_code}), {wait_time}초 후 재시도"
                        )
                        time.sleep(wait_time)
                        continue
                else:
                    # 4xx 에러는 재시도하지 않음
                    last_error = GPTServiceError(
                        operation=operation,
                        reason=str(e),
                        retryable=False
                    )
                    break

            except Exception as e:
                _gpt_circuit.record_failure()
                last_error = GPTServiceError(
                    operation=operation,
                    reason=str(e),
                    retryable=False
                )
                logger.error(f"GPT 호출 실패: {e}")
                break

            # 재시도 대기
            if attempt < self.MAX_RETRIES - 1:
                wait_time = self.RETRY_DELAYS[attempt]
                logger.warning(
                    f"GPT 호출 재시도 예정 ({wait_time}초 후) "
                    f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                time.sleep(wait_time)

        # 모든 재시도 실패
        _usage_stats.failed_requests += 1
        logger.error(f"GPT 호출 최종 실패: operation={operation}")
        raise last_error

    @staticmethod
    def get_usage_stats() -> Dict[str, Any]:
        """전역 사용량 통계 반환"""
        return _usage_stats.to_dict()

    @staticmethod
    def reset_circuit_breaker():
        """Circuit Breaker 리셋"""
        global _gpt_circuit
        _gpt_circuit = CircuitBreaker(failure_threshold=3, reset_timeout=120)
        logger.info("GPT Circuit Breaker 리셋")

    def generate_crawler_code(
        self,
        source_id: str,
        url: str,
        data_type: str,
        fields: List[Dict[str, str]],
        html_sample: str = "",
        page_type: str = "generic",
        requires_js: bool = False,
        pagination_info: Optional[Dict] = None
    ) -> str:
        """
        Generate crawler code for a new source.

        Args:
            source_id: Unique identifier for the source
            url: Target URL to crawl
            data_type: Type of data (html, pdf, excel, csv)
            fields: List of fields to extract with their selectors
            html_sample: Sample HTML for context
            page_type: Page type (news_list, financial_data, etc.)
            requires_js: Whether page requires JavaScript rendering
            pagination_info: Pagination details if applicable

        Returns:
            Generated Python code as string
        """
        fields_str = "\n".join([
            f"  - {f['name']}: selector={f.get('selector', 'auto-detect')}, "
            f"type={f.get('data_type', 'string')}, "
            f"method={f.get('extraction_method', 'text')}"
            for f in fields
        ])

        # 사이트 유형별 가이드 선택
        site_type_guide = self.SITE_TYPE_PROMPTS.get(page_type, "")

        # HTML 분석 정보 구성
        html_analysis = html_sample[:8000] if html_sample else "HTML 샘플 없음 - 셀렉터를 필드 정보에서 추론하세요."

        # Playwright 또는 requests 기반 코드 생성
        if requires_js:
            # 페이지네이션 정보
            pagination_str = "페이지네이션 없음"
            if pagination_info:
                pagination_str = (
                    f"- 타입: {pagination_info.get('type', 'none')}\n"
                    f"- 셀렉터: {pagination_info.get('selector', 'N/A')}\n"
                    f"- 최대 페이지: {pagination_info.get('max_pages', 10)}"
                )

            prompt = self.PLAYWRIGHT_CODE_PROMPT.format(
                url=url,
                data_type=data_type,
                page_type=page_type,
                fields=fields_str,
                js_reason="JavaScript 렌더링 필요 (동적 콘텐츠)",
                site_type_guide=site_type_guide,
                html_analysis=html_analysis,
                pagination_info=pagination_str,
                source_id=source_id.replace('-', '_'),
            )
        else:
            prompt = self.CRAWL_CODE_PROMPT.format(
                url=url,
                data_type=data_type,
                page_type=page_type,
                fields=fields_str,
                site_type_guide=site_type_guide,
                html_analysis=html_analysis,
                source_id=source_id.replace('-', '_'),
            )

        code = self._call_gpt(
            prompt,
            max_tokens=4000,
            system_prompt=self.SYSTEM_PROMPT_CRAWLER,
            operation="generate_crawler"
        )
        code = self._clean_code_output(code)

        return code

    def generate_crawler_code_advanced(
        self,
        source_id: str,
        url: str,
        data_type: str,
        fields: List[Dict[str, str]],
        html_content: str = "",
        page_type: str = "",
        pagination_info: Optional[Dict] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        고도화된 다단계 크롤러 코드 생성.

        Step 1: HTML 심층 분석 (구조 파악, 셀렉터 추출, JS 필요 여부)
        Step 2: 분석 결과 기반 코드 생성
        Step 3: 코드 자가 검증 (리뷰)
        Step 4: 페이지네이션 추가 (필요 시)

        Returns:
            Dict with keys: code, analysis, page_type, requires_js, reviewed, pagination_added
        """
        result = {
            'code': '',
            'analysis': {},
            'page_type': page_type or 'generic',
            'requires_js': False,
            'reviewed': False,
            'pagination_added': False,
            'generation_steps': [],
            'retry_count': 0,
        }

        # Step 1: HTML 심층 분석
        if html_content:
            logger.info(f"[Advanced] Step 1: HTML 구조 분석 - {url}")
            try:
                analysis = self.analyze_html_deep(url, html_content)
                result['analysis'] = analysis
                result['page_type'] = analysis.get('page_type', page_type or 'generic')
                result['requires_js'] = analysis.get('requires_javascript', False)
                result['generation_steps'].append('html_analysis')

                # 분석에서 필드 정보 보강
                if analysis.get('fields') and not any(f.get('selector') for f in fields):
                    analyzed_fields = {f['name']: f for f in analysis.get('fields', [])}
                    for field in fields:
                        if field['name'] in analyzed_fields:
                            af = analyzed_fields[field['name']]
                            field['selector'] = af.get('selector', field.get('selector', ''))
                            field['extraction_method'] = af.get('extraction_method', 'text')

                # 페이지네이션 정보 업데이트
                if not pagination_info and analysis.get('pagination', {}).get('exists'):
                    pagination_info = analysis['pagination']

                logger.info(
                    f"[Advanced] 분석 완료: type={result['page_type']}, "
                    f"js={result['requires_js']}, fields={len(analysis.get('fields', []))}"
                )
            except Exception as e:
                logger.warning(f"[Advanced] HTML 분석 실패, 기본 생성으로 진행: {e}")
                result['generation_steps'].append('html_analysis_failed')

        # Step 2: 코드 생성
        logger.info(f"[Advanced] Step 2: 코드 생성 - type={result['page_type']}, js={result['requires_js']}")
        code = self.generate_crawler_code(
            source_id=source_id,
            url=url,
            data_type=data_type,
            fields=fields,
            html_sample=html_content[:8000] if html_content else "",
            page_type=result['page_type'],
            requires_js=result['requires_js'],
            pagination_info=pagination_info,
        )
        result['code'] = code
        result['generation_steps'].append('code_generation')

        # Step 3: 코드 자가 검증
        logger.info(f"[Advanced] Step 3: 코드 자가 검증")
        try:
            fields_str = ", ".join([f['name'] for f in fields])
            reviewed_code = self._call_gpt(
                self.CODE_REVIEW_PROMPT.format(
                    code=code,
                    url=url,
                    fields=fields_str,
                ),
                max_tokens=4000,
                system_prompt=self.SYSTEM_PROMPT_CRAWLER,
                operation="review_crawler"
            )
            reviewed_code = self._clean_code_output(reviewed_code)
            if reviewed_code and len(reviewed_code) > 50:
                result['code'] = reviewed_code
                result['reviewed'] = True
                result['generation_steps'].append('code_review')
        except Exception as e:
            logger.warning(f"[Advanced] 코드 리뷰 실패, 원본 코드 유지: {e}")
            result['generation_steps'].append('code_review_failed')

        # Step 4: 페이지네이션 추가 (필요 시)
        if pagination_info and pagination_info.get('exists', False) and pagination_info.get('type') != 'none':
            logger.info(f"[Advanced] Step 4: 페이지네이션 추가 - type={pagination_info.get('type')}")
            try:
                paginated_code = self._call_gpt(
                    self.PAGINATION_SNIPPET_PROMPT.format(
                        base_code=result['code'],
                        pagination_type=pagination_info.get('type', 'page_param'),
                        pagination_selector=pagination_info.get('selector', ''),
                        max_pages=pagination_info.get('max_pages', 10),
                    ),
                    max_tokens=5000,
                    system_prompt=self.SYSTEM_PROMPT_CRAWLER,
                    operation="add_pagination"
                )
                paginated_code = self._clean_code_output(paginated_code)
                if paginated_code and len(paginated_code) > 50:
                    result['code'] = paginated_code
                    result['pagination_added'] = True
                    result['generation_steps'].append('pagination_added')
            except Exception as e:
                logger.warning(f"[Advanced] 페이지네이션 추가 실패: {e}")
                result['generation_steps'].append('pagination_failed')

        logger.info(
            f"[Advanced] 생성 완료: steps={result['generation_steps']}, "
            f"code_length={len(result['code'])}"
        )
        return result

    def analyze_html_deep(self, url: str, html_content: str) -> Dict[str, Any]:
        """
        HTML 구조 심층 분석.

        Args:
            url: Target URL
            html_content: Full HTML content

        Returns:
            Detailed analysis result
        """
        prompt = self.HTML_ANALYSIS_PROMPT.format(
            url=url,
            html_content=html_content[:15000]
        )

        response = self._call_gpt(
            prompt,
            max_tokens=3000,
            system_prompt=self.SYSTEM_PROMPT_CRAWLER,
            operation="analyze_html_deep"
        )

        try:
            cleaned = self._clean_code_output(response)
            json_match = re.search(r'\{[\s\S]*\}', cleaned)
            if json_match:
                return json.loads(json_match.group())
            return json.loads(cleaned)
        except json.JSONDecodeError:
            logger.warning(f"HTML 심층 분석 JSON 파싱 실패")
            return {
                'page_type': 'generic',
                'requires_javascript': False,
                'fields': [],
                'pagination': {'exists': False, 'type': 'none'},
            }

    def fix_crawler_code(
        self,
        current_code: str,
        error_code: str,
        error_message: str,
        stack_trace: str,
        html_snapshot: str = "",
        previous_html: str = ""
    ) -> str:
        """
        Fix crawler code based on error information.

        Args:
            current_code: The current (failing) code
            error_code: Error classification code (E001-E006)
            error_message: The error message
            stack_trace: Full stack trace
            html_snapshot: Current page HTML for context
            previous_html: Previous successful HTML for diff comparison

        Returns:
            Fixed Python code as string
        """
        # HTML diff 생성 (이전 HTML이 있는 경우)
        html_diff_section = ""
        if previous_html and html_snapshot:
            html_diff_section = (
                "\n[이전 성공 HTML과 현재 HTML 비교]\n"
                f"이전 HTML 주요 구조 (처음 2000자):\n{previous_html[:2000]}\n\n"
                f"현재 HTML 주요 구조 (처음 2000자):\n{html_snapshot[:2000]}\n"
                "위 두 HTML을 비교하여 변경된 셀렉터를 파악하세요."
            )

        prompt = self.ERROR_FIX_PROMPT.format(
            error_code=error_code,
            error_message=error_message,
            stack_trace=stack_trace[:2000],
            current_code=current_code,
            html_snapshot=html_snapshot[:5000] if html_snapshot else "N/A",
            previous_html_diff=html_diff_section
        )

        code = self._call_gpt(
            prompt,
            max_tokens=4000,
            system_prompt=self.SYSTEM_PROMPT_CRAWLER,
            operation="fix_crawler"
        )
        code = self._clean_code_output(code)

        return code

    def analyze_page_structure(
        self,
        url: str,
        html_content: str,
        fields: List[str]
    ) -> Dict[str, Any]:
        """
        Analyze page structure and suggest selectors.

        Args:
            url: The page URL
            html_content: The HTML content
            fields: List of field names to extract

        Returns:
            Analysis result with suggested selectors
        """
        fields_str = "\n".join([f"  - {f}" for f in fields])

        prompt = self.ANALYZE_STRUCTURE_PROMPT.format(
            url=url,
            html_content=html_content[:10000],
            fields=fields_str
        )

        response = self._call_gpt(prompt, max_tokens=2000, operation="analyze_structure")

        # Parse JSON response
        try:
            # Clean up potential markdown
            cleaned = self._clean_code_output(response)

            # JSON 추출 시도 (응답에 추가 텍스트가 있을 수 있음)
            json_match = re.search(r'\{[\s\S]*\}', cleaned)
            if json_match:
                return json.loads(json_match.group())

            return json.loads(cleaned)

        except json.JSONDecodeError as e:
            logger.warning(f"GPT 응답 JSON 파싱 실패: {e}")

            # 재시도 (한 번 더)
            try:
                retry_prompt = f"""이전 응답을 올바른 JSON으로 수정해주세요. JSON만 출력하세요.

이전 응답:
{response[:1000]}

올바른 JSON 형식으로 출력하세요."""

                retry_response = self._call_gpt(retry_prompt, max_tokens=2000, operation="json_fix")
                cleaned = self._clean_code_output(retry_response)
                json_match = re.search(r'\{[\s\S]*\}', cleaned)
                if json_match:
                    return json.loads(json_match.group())
                return json.loads(cleaned)

            except (json.JSONDecodeError, GPTServiceError) as retry_error:
                # 최종 실패
                raise GPTInvalidResponseError(
                    expected_format="JSON",
                    raw_response=response
                )

    def _clean_code_output(self, code: str) -> str:
        """
        Clean up code output by removing markdown code blocks.

        Args:
            code: The code string to clean

        Returns:
            Cleaned code string
        """
        # Remove markdown code blocks
        if code.startswith("```python"):
            code = code[9:]
        elif code.startswith("```"):
            code = code[3:]

        if code.endswith("```"):
            code = code[:-3]

        return code.strip()

    def generate_pdf_crawler_code(
        self,
        source_id: str,
        url: str,
        fields: List[Dict[str, str]]
    ) -> str:
        """
        Generate code specifically for PDF crawling.

        Args:
            source_id: Unique identifier for the source
            url: URL of the PDF or page containing PDF links
            fields: Fields to extract from PDF

        Returns:
            Generated Python code for PDF extraction
        """
        prompt = f"""PDF 문서에서 데이터를 추출하는 Python 코드를 생성하세요.

[소스 정보]
- URL: {url}
- 추출 필드: {', '.join([f['name'] for f in fields])}

[요구사항]
1. 함수명: crawl_{source_id.replace('-', '_')}
2. pdfplumber 라이브러리 사용
3. 테이블 데이터 추출 지원
4. 반환값: List[Dict] 형태
5. 에러 처리 포함

코드만 출력하세요. ```python 과 ``` 없이."""

        code = self._call_gpt(prompt, max_tokens=3000, operation="generate_pdf_crawler")
        return self._clean_code_output(code)

    def generate_excel_crawler_code(
        self,
        source_id: str,
        url: str,
        fields: List[Dict[str, str]]
    ) -> str:
        """
        Generate code specifically for Excel file crawling.

        Args:
            source_id: Unique identifier for the source
            url: URL of the Excel file
            fields: Fields/columns to extract

        Returns:
            Generated Python code for Excel extraction
        """
        prompt = f"""Excel 파일에서 데이터를 추출하는 Python 코드를 생성하세요.

[소스 정보]
- URL: {url}
- 추출 필드: {', '.join([f['name'] for f in fields])}

[요구사항]
1. 함수명: crawl_{source_id.replace('-', '_')}
2. pandas와 openpyxl 사용
3. 시트 자동 감지 또는 첫 번째 시트 사용
4. 헤더 행 자동 감지
5. 반환값: List[Dict] 형태
6. 에러 처리 포함

코드만 출력하세요. ```python 과 ``` 없이."""

        code = self._call_gpt(prompt, max_tokens=3000, operation="generate_excel_crawler")
        return self._clean_code_output(code)
