"""
AI Text Refiner for OCR post-processing.

This module uses GPT to correct OCR errors, structure text,
and extract meaningful content from raw OCR output.
"""

import os
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)


@dataclass
class RefinementResult:
    """Result of AI text refinement."""
    success: bool
    original_text: str = ""
    refined_text: str = ""
    corrections: List[Dict[str, str]] = field(default_factory=list)
    structured_data: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'success': self.success,
            'original_text': self.original_text,
            'refined_text': self.refined_text,
            'corrections': self.corrections,
            'structured_data': self.structured_data,
            'confidence': self.confidence,
            'error_message': self.error_message
        }


class AITextRefiner:
    """
    AI-powered text refiner for OCR post-processing.

    Uses GPT to:
    1. Correct OCR errors (misread characters, spacing)
    2. Structure raw text into meaningful fields
    3. Extract specific information from news articles
    """

    # Prompt templates
    REFINE_TEXT_PROMPT = """OCR로 추출된 텍스트에서 오류를 수정하세요.

[원본 텍스트]
{text}

[지시사항]
1. 깨진 글자나 잘못 인식된 문자를 수정하세요
2. 불필요한 공백을 정리하세요
3. 문장 구조를 자연스럽게 다듬으세요
4. 원문의 의미를 최대한 보존하세요

[중요: 숫자 처리 규칙]
- 숫자 값 자체는 절대 변경하지 마세요 (1234 → 1235 금지)
- 숫자 형식만 수정 가능합니다:
  * 천단위 구분: 1000000 → 1,000,000 (허용)
  * OCR 오인식: O → 0, l → 1, S → 5 (명백한 경우만)
- 금액, 통계, 날짜의 숫자는 원본 그대로 유지
- 숫자가 불확실하면 원본 유지하고 corrections에 "uncertain" 표시

JSON 형식으로 응답하세요:
{{
    "refined_text": "수정된 텍스트",
    "corrections": [
        {{"original": "잘못된 부분", "corrected": "수정된 부분", "reason": "수정 이유"}}
    ],
    "confidence": 0.0-1.0
}}

JSON만 출력하세요."""

    EXTRACT_NEWS_PROMPT = """OCR로 추출된 뉴스 이미지 텍스트를 분석하고 구조화하세요.

[원본 텍스트]
{text}

[추가 컨텍스트]
- 이미지 출처: {source}
- 언어: {language}

[지시사항]
1. OCR 오류를 수정하세요
2. 뉴스 구조 요소를 추출하세요:
   - 제목 (headline)
   - 부제목 (subheadline) - 있는 경우
   - 날짜 (date)
   - 기자/작성자 (author)
   - 언론사 (publisher)
   - 본문 (body)
   - 인용문 (quotes) - 있는 경우
   - 키워드 (keywords) - 추출 가능한 경우

JSON 형식으로 응답하세요:
{{
    "refined_text": "수정된 전체 텍스트",
    "structured": {{
        "headline": "뉴스 제목",
        "subheadline": "부제목 또는 null",
        "date": "날짜 (YYYY-MM-DD 형식)",
        "author": "기자명 또는 null",
        "publisher": "언론사명 또는 null",
        "body": "본문 내용",
        "quotes": ["인용문1", "인용문2"],
        "keywords": ["키워드1", "키워드2"]
    }},
    "corrections": [
        {{"original": "원본", "corrected": "수정", "reason": "이유"}}
    ],
    "confidence": 0.0-1.0
}}

JSON만 출력하세요."""

    EXTRACT_TABLE_PROMPT = """OCR로 추출된 표 데이터를 분석하고 구조화하세요.

[원본 텍스트 (행별로 구분)]
{text}

[지시사항]
1. OCR 오류를 수정하세요
2. 표 구조를 파악하세요
3. 헤더 행을 식별하세요
4. 데이터 타입을 추론하세요 (숫자, 날짜, 텍스트)

JSON 형식으로 응답하세요:
{{
    "headers": ["열1", "열2", ...],
    "rows": [
        ["값1", "값2", ...],
        ...
    ],
    "data_types": {{"열1": "string|number|date", ...}},
    "corrections": [...],
    "confidence": 0.0-1.0
}}

JSON만 출력하세요."""

    VERIFY_NUMBERS_PROMPT = """OCR로 추출된 텍스트에서 숫자의 신뢰도를 평가하세요.

[원본 텍스트]
{text}

[지시사항]
1. 텍스트에서 모든 숫자를 찾으세요 (금액, 날짜, 통계, 전화번호 등)
2. 각 숫자의 OCR 신뢰도를 평가하세요
3. 숫자 값은 절대 수정하지 마세요
4. 불확실한 숫자만 플래그 표시하세요

평가 기준:
- O/0, l/1, S/5, B/8 혼동 가능성
- 문맥상 숫자 범위가 타당한지 (나이: 0-150, 연도: 1900-2100 등)
- 자릿수가 맞는지

JSON 형식으로 응답하세요:
{{
    "numbers_found": [
        {{
            "value": "원본 숫자 그대로",
            "type": "money|date|statistic|phone|other",
            "confidence": 0.0-1.0,
            "position": "텍스트 내 위치 설명",
            "uncertain_chars": ["불확실한 문자 위치"],
            "needs_review": true|false
        }}
    ],
    "high_confidence_count": 0,
    "needs_review_count": 0
}}

JSON만 출력하세요."""

    VERIFY_EXTRACTION_PROMPT = """OCR 추출 결과의 정확성을 검증하고 개선하세요.

[추출된 데이터]
{extracted_data}

[원본 OCR 텍스트]
{raw_text}

[예상 필드]
{expected_fields}

[지시사항]
1. 추출된 데이터가 원본 텍스트와 일치하는지 확인
2. 누락된 정보가 있으면 원본에서 추출
3. 잘못 추출된 정보를 수정
4. 각 필드의 신뢰도를 평가

JSON 형식으로 응답하세요:
{{
    "verified_data": {{
        "필드명": {{"value": "값", "confidence": 0.0-1.0, "source": "원본 텍스트 참조"}}
    }},
    "missing_fields": ["누락된 필드"],
    "corrections": [...],
    "overall_confidence": 0.0-1.0
}}

JSON만 출력하세요."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None
    ):
        """
        Initialize AI Text Refiner.

        Args:
            api_key: OpenAI API key
            model: Model to use. Defaults to AI_MODEL env var or gpt-4o-mini.
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.model = model or os.getenv('AI_MODEL', 'gpt-4o-mini')
        self._client = None

    @property
    def client(self):
        """Lazy initialization of OpenAI client."""
        if self._client is None:
            from openai import OpenAI
            if not self.api_key:
                raise ValueError("OpenAI API key is required")
            client_kwargs = {"api_key": self.api_key}
            ai_base_url = os.getenv('AI_BASE_URL')
            if ai_base_url:
                client_kwargs["base_url"] = ai_base_url
            self._client = OpenAI(**client_kwargs)
        return self._client

    def _call_gpt(
        self,
        prompt: str,
        max_tokens: int = 4000,
        temperature: float = 0.1
    ) -> str:
        """
        Call GPT API.

        Args:
            prompt: The prompt
            max_tokens: Maximum tokens
            temperature: Sampling temperature

        Returns:
            Response text
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "당신은 OCR 텍스트 교정 전문가입니다. 정확하고 자연스러운 텍스트로 수정합니다."
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

    def _parse_json_response(self, response: str) -> Dict[str, Any]:
        """
        Parse JSON from GPT response.

        Args:
            response: GPT response text

        Returns:
            Parsed dictionary
        """
        # Clean up markdown code blocks
        if response.startswith("```json"):
            response = response[7:]
        elif response.startswith("```"):
            response = response[3:]
        if response.endswith("```"):
            response = response[:-3]

        response = response.strip()

        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse failed: {e}")
            # Try to extract JSON from response
            json_match = re.search(r'\{[\s\S]*\}', response)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except json.JSONDecodeError:
                    pass
            return {"raw_response": response, "parse_error": str(e)}

    def refine_text(self, text: str) -> RefinementResult:
        """
        Refine OCR text by correcting errors.

        Args:
            text: Raw OCR text

        Returns:
            RefinementResult with corrections
        """
        if not text or not text.strip():
            return RefinementResult(
                success=False,
                original_text=text,
                error_message="Empty text provided"
            )

        try:
            prompt = self.REFINE_TEXT_PROMPT.format(text=text)
            response = self._call_gpt(prompt)
            result = self._parse_json_response(response)

            if "raw_response" in result:
                # Failed to parse, return raw
                return RefinementResult(
                    success=True,
                    original_text=text,
                    refined_text=result.get("raw_response", text),
                    confidence=0.5
                )

            return RefinementResult(
                success=True,
                original_text=text,
                refined_text=result.get("refined_text", text),
                corrections=result.get("corrections", []),
                confidence=result.get("confidence", 0.8)
            )

        except Exception as e:
            logger.error(f"Text refinement failed: {e}")
            return RefinementResult(
                success=False,
                original_text=text,
                error_message=str(e)
            )

    def extract_news_structure(
        self,
        text: str,
        source: str = "unknown",
        language: str = "ko"
    ) -> RefinementResult:
        """
        Extract structured news data from OCR text.

        Args:
            text: Raw OCR text
            source: Image source/URL
            language: Primary language

        Returns:
            RefinementResult with structured news data
        """
        if not text or not text.strip():
            return RefinementResult(
                success=False,
                original_text=text,
                error_message="Empty text provided"
            )

        try:
            prompt = self.EXTRACT_NEWS_PROMPT.format(
                text=text,
                source=source,
                language=language
            )
            response = self._call_gpt(prompt, max_tokens=6000)
            result = self._parse_json_response(response)

            if "raw_response" in result:
                return RefinementResult(
                    success=True,
                    original_text=text,
                    refined_text=result.get("raw_response", text),
                    confidence=0.5
                )

            return RefinementResult(
                success=True,
                original_text=text,
                refined_text=result.get("refined_text", text),
                corrections=result.get("corrections", []),
                structured_data=result.get("structured", {}),
                confidence=result.get("confidence", 0.8)
            )

        except Exception as e:
            logger.error(f"News extraction failed: {e}")
            return RefinementResult(
                success=False,
                original_text=text,
                error_message=str(e)
            )

    def extract_table_structure(
        self,
        rows: List[List[str]]
    ) -> Dict[str, Any]:
        """
        Extract structured table data from OCR rows.

        Args:
            rows: 2D list of row data

        Returns:
            Structured table data
        """
        if not rows:
            return {"success": False, "error": "Empty table data"}

        try:
            # Format rows as text
            text = "\n".join([" | ".join(row) for row in rows])

            prompt = self.EXTRACT_TABLE_PROMPT.format(text=text)
            response = self._call_gpt(prompt)
            result = self._parse_json_response(response)

            result["success"] = True
            return result

        except Exception as e:
            logger.error(f"Table extraction failed: {e}")
            return {"success": False, "error": str(e)}

    def verify_numbers(self, text: str) -> Dict[str, Any]:
        """
        Verify numbers in OCR text without modifying them.

        Numbers are flagged for review but never changed.

        Args:
            text: OCR extracted text

        Returns:
            Number verification results with confidence scores
        """
        if not text or not text.strip():
            return {"success": False, "error": "Empty text"}

        try:
            prompt = self.VERIFY_NUMBERS_PROMPT.format(text=text)
            response = self._call_gpt(prompt, temperature=0.1)
            result = self._parse_json_response(response)
            result["success"] = True
            return result

        except Exception as e:
            logger.error(f"Number verification failed: {e}")
            return {"success": False, "error": str(e)}

    def refine_text_preserve_numbers(self, text: str) -> RefinementResult:
        """
        Refine text while strictly preserving all numbers.

        This method:
        1. Extracts all numbers from text
        2. Replaces them with placeholders
        3. Refines the text
        4. Restores original numbers
        5. Separately verifies number confidence

        Args:
            text: Raw OCR text

        Returns:
            RefinementResult with numbers preserved
        """
        import re

        # Extract numbers with their positions
        number_pattern = re.compile(
            r'[\d,\.]+[%원달러만억천백십]?|'
            r'\d{2,4}[-./년]\d{1,2}[-./월]\d{1,2}일?|'
            r'\d{2,4}[-./]\d{1,2}[-./]\d{1,2}'
        )

        numbers = []
        placeholder_text = text

        for i, match in enumerate(number_pattern.finditer(text)):
            placeholder = f"__NUM_{i}__"
            numbers.append({
                "placeholder": placeholder,
                "original": match.group(),
                "start": match.start(),
                "end": match.end()
            })

        # Replace numbers with placeholders (reverse order to preserve positions)
        for num_info in reversed(numbers):
            placeholder_text = (
                placeholder_text[:num_info["start"]] +
                num_info["placeholder"] +
                placeholder_text[num_info["end"]:]
            )

        # Refine text without numbers
        refined_result = self.refine_text(placeholder_text)

        if not refined_result.success:
            return refined_result

        # Restore original numbers
        restored_text = refined_result.refined_text
        for num_info in numbers:
            restored_text = restored_text.replace(
                num_info["placeholder"],
                num_info["original"]
            )

        # Verify numbers separately
        number_verification = self.verify_numbers(text)

        refined_result.refined_text = restored_text
        refined_result.structured_data["number_verification"] = number_verification
        refined_result.structured_data["numbers_preserved"] = [n["original"] for n in numbers]

        return refined_result

    def verify_and_improve(
        self,
        extracted_data: Dict[str, Any],
        raw_text: str,
        expected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Verify extraction accuracy and improve results.

        Args:
            extracted_data: Previously extracted data
            raw_text: Original OCR text
            expected_fields: List of expected field names

        Returns:
            Verified and improved data
        """
        try:
            prompt = self.VERIFY_EXTRACTION_PROMPT.format(
                extracted_data=json.dumps(extracted_data, ensure_ascii=False, indent=2),
                raw_text=raw_text,
                expected_fields=", ".join(expected_fields)
            )
            response = self._call_gpt(prompt)
            result = self._parse_json_response(response)

            result["success"] = True
            return result

        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return {"success": False, "error": str(e)}

    def batch_refine(
        self,
        texts: List[str],
        mode: str = "simple"
    ) -> List[RefinementResult]:
        """
        Batch refine multiple texts.

        Args:
            texts: List of texts to refine
            mode: 'simple' for basic refinement, 'news' for news extraction

        Returns:
            List of RefinementResult
        """
        results = []

        for text in texts:
            if mode == "news":
                result = self.extract_news_structure(text)
            else:
                result = self.refine_text(text)
            results.append(result)

        return results

    def smart_refine(
        self,
        text: str,
        context: Optional[Dict[str, Any]] = None,
        preserve_numbers: bool = True
    ) -> RefinementResult:
        """
        Intelligently refine text based on detected content type.

        Args:
            text: Raw OCR text
            context: Optional context hints
            preserve_numbers: If True, numbers are never modified (default: True)

        Returns:
            RefinementResult
        """
        # Detect content type
        content_type = self._detect_content_type(text, context)

        if content_type == "news":
            result = self.extract_news_structure(text)
            # Add number verification for news (statistics, dates matter)
            if preserve_numbers:
                num_verify = self.verify_numbers(text)
                result.structured_data["number_verification"] = num_verify
            return result
        elif content_type == "table":
            # Tables usually contain important numbers
            rows = [line.split() for line in text.split('\n') if line.strip()]
            table_result = self.extract_table_structure(rows)
            if preserve_numbers:
                table_result["number_verification"] = self.verify_numbers(text)
            return RefinementResult(
                success=table_result.get("success", False),
                original_text=text,
                structured_data=table_result,
                confidence=table_result.get("confidence", 0.5)
            )
        else:
            # Use number-preserving refinement by default
            if preserve_numbers:
                return self.refine_text_preserve_numbers(text)
            else:
                return self.refine_text(text)

    def _detect_content_type(
        self,
        text: str,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Detect content type from text.

        Args:
            text: Text to analyze
            context: Optional context hints

        Returns:
            Content type: 'news', 'table', or 'general'
        """
        if context and context.get("type"):
            return context["type"]

        # Simple heuristics
        lines = text.split('\n')

        # Check for table-like structure (multiple rows with similar column count)
        if len(lines) >= 3:
            column_counts = [len(line.split()) for line in lines[:5] if line.strip()]
            if column_counts and max(column_counts) >= 3:
                variance = max(column_counts) - min(column_counts)
                if variance <= 1:
                    return "table"

        # Check for news indicators
        news_indicators = ['기자', '뉴스', '보도', '속보', '특종', '기사', '언론', '일보',
                          '신문', '방송', '취재', 'reporter', 'news', 'breaking']
        text_lower = text.lower()
        if any(ind in text_lower for ind in news_indicators):
            return "news"

        # Check for date patterns (common in news)
        date_pattern = re.compile(r'\d{4}[-./년]\d{1,2}[-./월]\d{1,2}')
        if date_pattern.search(text):
            return "news"

        return "general"


class OCRPipeline:
    """
    Complete OCR pipeline combining extraction and refinement.

    Usage:
        pipeline = OCRPipeline()
        result = pipeline.process_image(image_path)
    """

    def __init__(
        self,
        ocr_languages: List[str] = None,
        use_gpu: bool = False,
        openai_api_key: Optional[str] = None,
        openai_model: Optional[str] = None
    ):
        """
        Initialize OCR Pipeline.

        Args:
            ocr_languages: Languages for OCR
            use_gpu: Use GPU for OCR
            openai_api_key: OpenAI API key
            openai_model: GPT model to use
        """
        from .ocr_engine import OCREngine

        self.ocr_engine = OCREngine(
            languages=ocr_languages,
            gpu=use_gpu
        )
        self.refiner = AITextRefiner(
            api_key=openai_api_key,
            model=openai_model
        )

    def process_image(
        self,
        image_source: Union[str, bytes],
        content_type: str = "auto",
        preprocess: bool = True,
        refine: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Process image through complete OCR pipeline.

        Args:
            image_source: Image path, URL, or bytes
            content_type: 'news', 'table', 'general', or 'auto'
            preprocess: Apply image preprocessing
            refine: Apply AI refinement
            **kwargs: Additional options

        Returns:
            Complete processing result
        """
        result = {
            "success": False,
            "ocr_result": None,
            "refined_result": None,
            "final_data": None
        }

        # Step 1: OCR Extraction
        if content_type == "news":
            ocr_result = self.ocr_engine.extract_structured_news(
                image_source,
                preprocess=preprocess,
                **kwargs
            )
            result["ocr_result"] = ocr_result
            raw_text = ocr_result.get("raw_text", "")
        else:
            ocr_result = self.ocr_engine.extract_text(
                image_source,
                preprocess=preprocess,
                **kwargs
            )
            result["ocr_result"] = ocr_result.to_dict()
            raw_text = ocr_result.text if ocr_result.success else ""

        if not raw_text:
            result["error"] = "OCR extraction failed or returned empty text"
            return result

        # Step 2: AI Refinement (if enabled)
        if refine:
            if content_type == "news" or content_type == "auto":
                refined = self.refiner.smart_refine(raw_text)
            elif content_type == "table":
                # Get table rows from OCR
                rows = self.ocr_engine.extract_table_text(image_source)
                table_result = self.refiner.extract_table_structure(rows)
                refined = RefinementResult(
                    success=table_result.get("success", False),
                    original_text=raw_text,
                    structured_data=table_result
                )
            else:
                refined = self.refiner.refine_text(raw_text)

            result["refined_result"] = refined.to_dict()
            result["final_data"] = refined.structured_data or {"text": refined.refined_text}
        else:
            result["final_data"] = {"text": raw_text}

        result["success"] = True
        return result

    def process_news_image(
        self,
        image_source: Union[str, bytes],
        source_name: str = "unknown",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Specialized method for news image processing.

        Args:
            image_source: Image source
            source_name: News source name
            **kwargs: Additional options

        Returns:
            Structured news data
        """
        # Extract with OCR
        ocr_result = self.ocr_engine.extract_structured_news(
            image_source,
            preprocess=True,
            enhance_contrast=True,
            **kwargs
        )

        if not ocr_result.get("success"):
            return ocr_result

        raw_text = ocr_result.get("raw_text", "")

        # Refine with AI
        refined = self.refiner.extract_news_structure(
            raw_text,
            source=source_name,
            language=ocr_result.get("language_detected", "ko")
        )

        return {
            "success": True,
            "ocr_confidence": ocr_result.get("confidence", 0),
            "ai_confidence": refined.confidence,
            "corrections_count": len(refined.corrections),
            "raw_ocr": ocr_result,
            "structured_news": refined.structured_data,
            "refined_text": refined.refined_text,
            "corrections": refined.corrections
        }

    def batch_process(
        self,
        image_sources: List[Union[str, bytes]],
        content_type: str = "auto",
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Process multiple images.

        Args:
            image_sources: List of image sources
            content_type: Content type
            **kwargs: Additional options

        Returns:
            List of results
        """
        return [
            self.process_image(src, content_type=content_type, **kwargs)
            for src in image_sources
        ]
