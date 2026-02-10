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

    # Prompt templates
    CRAWL_CODE_PROMPT = """당신은 Python 크롤링 전문가입니다. 아래 요구사항에 맞는 크롤링 코드를 생성하세요.

[소스 정보]
- URL: {url}
- 데이터 타입: {data_type}
- 추출 필드: {fields}

[요구사항]
1. 함수명: crawl_{source_id}
2. 반환값: List[Dict] 형태
3. 에러 처리 포함 (try-except)
4. requests 사용 (동적 페이지면 selenium)
5. 타임아웃 30초 설정
6. User-Agent 헤더 설정
7. 인코딩 자동 감지

[HTML 구조 샘플]
{html_sample}

코드만 출력하세요. 설명 불필요. ```python 과 ``` 없이 순수 코드만 출력하세요."""

    ERROR_FIX_PROMPT = """크롤링 코드에서 오류가 발생했습니다. 수정된 코드를 생성하세요.

[오류 정보]
- 오류 코드: {error_code}
- 오류 메시지: {error_message}
- 스택 트레이스: {stack_trace}

[기존 코드]
{current_code}

[현재 페이지 HTML (처음 5000자)]
{html_snapshot}

[지시사항]
1. 오류 원인을 분석하세요
2. 수정된 전체 코드를 출력하세요
3. 변경 사항을 주석으로 표시하세요

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
        html_sample: str = ""
    ) -> str:
        """
        Generate crawler code for a new source.

        Args:
            source_id: Unique identifier for the source
            url: Target URL to crawl
            data_type: Type of data (html, pdf, excel, csv)
            fields: List of fields to extract with their selectors
            html_sample: Sample HTML for context (first 5000 chars)

        Returns:
            Generated Python code as string
        """
        fields_str = "\n".join([
            f"  - {f['name']}: {f.get('selector', 'auto-detect')} ({f.get('data_type', 'string')})"
            for f in fields
        ])

        prompt = self.CRAWL_CODE_PROMPT.format(
            url=url,
            data_type=data_type,
            fields=fields_str,
            source_id=source_id.replace('-', '_'),
            html_sample=html_sample[:5000] if html_sample else "N/A"
        )

        code = self._call_gpt(prompt, max_tokens=4000, operation="generate_crawler")

        # Clean up any markdown code blocks if present
        code = self._clean_code_output(code)

        return code

    def fix_crawler_code(
        self,
        current_code: str,
        error_code: str,
        error_message: str,
        stack_trace: str,
        html_snapshot: str = ""
    ) -> str:
        """
        Fix crawler code based on error information.

        Args:
            current_code: The current (failing) code
            error_code: Error classification code (E001-E006)
            error_message: The error message
            stack_trace: Full stack trace
            html_snapshot: Current page HTML for context

        Returns:
            Fixed Python code as string
        """
        prompt = self.ERROR_FIX_PROMPT.format(
            error_code=error_code,
            error_message=error_message,
            stack_trace=stack_trace[:2000],
            current_code=current_code,
            html_snapshot=html_snapshot[:5000] if html_snapshot else "N/A"
        )

        code = self._call_gpt(prompt, max_tokens=4000, operation="fix_crawler")
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
