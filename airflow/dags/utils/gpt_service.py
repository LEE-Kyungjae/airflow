"""
GPT Service for code generation and error recovery.

This module handles all interactions with the OpenAI GPT-4o-mini API
for generating crawler code and fixing errors automatically.
"""

import os
import logging
from typing import Optional, Dict, Any, List
from openai import OpenAI

logger = logging.getLogger(__name__)


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

    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o-mini"):
        """
        Initialize GPT Service.

        Args:
            api_key: OpenAI API key. If not provided, reads from OPENAI_API_KEY env var.
            model: Model to use. Defaults to gpt-4o-mini.
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key is required")

        self.client = OpenAI(api_key=self.api_key)
        self.model = model

    def _call_gpt(self, prompt: str, max_tokens: int = 4000, temperature: float = 0.2) -> str:
        """
        Call GPT API with the given prompt.

        Args:
            prompt: The prompt to send
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature (lower = more deterministic)

        Returns:
            The generated text response
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "당신은 Python 크롤링 전문가입니다. 요청에 맞는 코드를 정확하게 생성합니다."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=max_tokens,
                temperature=temperature
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"GPT API call failed: {e}")
            raise

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

        code = self._call_gpt(prompt, max_tokens=4000)

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

        code = self._call_gpt(prompt, max_tokens=4000)
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
        import json

        fields_str = "\n".join([f"  - {f}" for f in fields])

        prompt = self.ANALYZE_STRUCTURE_PROMPT.format(
            url=url,
            html_content=html_content[:10000],
            fields=fields_str
        )

        response = self._call_gpt(prompt, max_tokens=2000)

        # Parse JSON response
        try:
            # Clean up potential markdown
            response = self._clean_code_output(response)
            return json.loads(response)
        except json.JSONDecodeError:
            logger.warning("Failed to parse GPT response as JSON, returning raw")
            return {"raw_response": response}

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

        code = self._call_gpt(prompt, max_tokens=3000)
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

        code = self._call_gpt(prompt, max_tokens=3000)
        return self._clean_code_output(code)
