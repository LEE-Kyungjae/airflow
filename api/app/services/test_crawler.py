"""
Test Crawler Service - 저장 없이 크롤링 테스트

사용자가 정의한 필드로 실제 데이터를 추출하여 미리 확인할 수 있게 합니다.
"""

import re
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class FieldStats:
    """필드별 추출 통계"""
    found: int = 0
    empty: int = 0
    errors: int = 0


@dataclass
class TestCrawlResult:
    """테스트 크롤링 결과"""
    success: bool
    records: List[Dict[str, Any]]
    total_found: int
    extracted_count: int
    extraction_time_ms: int
    field_stats: Dict[str, Dict[str, int]]
    warnings: List[str]
    error: Optional[str] = None


class TestCrawlerService:
    """테스트 크롤링 서비스"""

    def __init__(self):
        self.timeout = 30.0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        }

    async def test_crawl(
        self,
        url: str,
        fields: List[Dict[str, Any]],
        max_records: int = 10
    ) -> TestCrawlResult:
        """
        테스트 크롤링 실행

        Args:
            url: 크롤링 대상 URL
            fields: 추출할 필드 목록 [{name, selector, data_type, attribute?, is_list?}]
            max_records: 최대 추출 레코드 수

        Returns:
            TestCrawlResult: 테스트 결과
        """
        start_time = datetime.utcnow()
        warnings = []

        try:
            # 1. 페이지 fetch
            html_content = await self._fetch_page(url)

            # 2. HTML 파싱
            soup = BeautifulSoup(html_content, 'lxml')

            # 3. 리스트 컨테이너 찾기
            records, total_found, field_stats = self._extract_records(
                soup, fields, max_records
            )

            # 4. 경고 생성
            warnings = self._generate_warnings(field_stats, fields, total_found)

            extraction_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

            return TestCrawlResult(
                success=len(records) > 0,
                records=records,
                total_found=total_found,
                extracted_count=len(records),
                extraction_time_ms=extraction_time,
                field_stats={k: {'found': v.found, 'empty': v.empty, 'errors': v.errors}
                            for k, v in field_stats.items()},
                warnings=warnings
            )

        except httpx.TimeoutException:
            extraction_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            return TestCrawlResult(
                success=False,
                records=[],
                total_found=0,
                extracted_count=0,
                extraction_time_ms=extraction_time,
                field_stats={},
                warnings=[],
                error=f"요청 시간 초과 ({self.timeout}초)"
            )

        except httpx.HTTPStatusError as e:
            extraction_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            return TestCrawlResult(
                success=False,
                records=[],
                total_found=0,
                extracted_count=0,
                extraction_time_ms=extraction_time,
                field_stats={},
                warnings=[],
                error=f"HTTP 오류: {e.response.status_code}"
            )

        except Exception as e:
            logger.error(f"Test crawl failed: {e}", exc_info=True)
            extraction_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            return TestCrawlResult(
                success=False,
                records=[],
                total_found=0,
                extracted_count=0,
                extraction_time_ms=extraction_time,
                field_stats={},
                warnings=[],
                error=str(e)
            )

    async def _fetch_page(self, url: str) -> str:
        """페이지 fetch"""
        async with httpx.AsyncClient(follow_redirects=True, timeout=self.timeout) as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            return response.text

    def _extract_records(
        self,
        soup: BeautifulSoup,
        fields: List[Dict[str, Any]],
        max_records: int
    ) -> Tuple[List[Dict], int, Dict[str, FieldStats]]:
        """
        레코드 추출

        두 가지 전략 사용:
        1. 리스트 기반: 첫 번째 필드의 셀렉터로 아이템들을 찾고, 각 아이템 내에서 다른 필드 추출
        2. 개별 기반: 각 필드를 개별적으로 추출하고 zip으로 결합
        """
        field_stats = {f['name']: FieldStats() for f in fields}
        records = []

        # 전략 1: 리스트 기반 추출 시도
        records, total = self._try_list_extraction(soup, fields, max_records, field_stats)

        if records:
            return records, total, field_stats

        # 전략 2: 개별 필드 추출 후 zip
        records, total = self._try_zip_extraction(soup, fields, max_records, field_stats)

        return records, total, field_stats

    def _try_list_extraction(
        self,
        soup: BeautifulSoup,
        fields: List[Dict[str, Any]],
        max_records: int,
        field_stats: Dict[str, FieldStats]
    ) -> Tuple[List[Dict], int]:
        """리스트 기반 추출 - 공통 부모 요소 찾기"""
        records = []

        # 첫 번째 필드로 모든 아이템 찾기
        first_field = fields[0]
        first_selector = first_field.get('selector', '')

        if not first_selector:
            return [], 0

        first_elements = soup.select(first_selector)
        if not first_elements:
            return [], 0

        total_found = len(first_elements)

        # 공통 부모 요소 찾기 시도
        common_parents = self._find_common_parents(first_elements)

        if common_parents:
            # 부모 요소 기반 추출
            for parent in common_parents[:max_records]:
                record = {}
                for field in fields:
                    value = self._extract_field_value(parent, field)
                    record[field['name']] = value

                    # 통계 업데이트
                    if value:
                        field_stats[field['name']].found += 1
                    else:
                        field_stats[field['name']].empty += 1

                if any(v for v in record.values()):  # 최소 하나의 값이 있으면 추가
                    records.append(record)
        else:
            # 부모를 찾지 못하면 첫 번째 필드 요소 기반으로 시도
            for elem in first_elements[:max_records]:
                record = {}
                for field in fields:
                    if field == first_field:
                        value = self._extract_value_from_element(elem, field)
                    else:
                        # 동일 레벨이나 부모에서 찾기
                        value = self._find_sibling_or_parent_value(elem, field)

                    record[field['name']] = value

                    if value:
                        field_stats[field['name']].found += 1
                    else:
                        field_stats[field['name']].empty += 1

                if any(v for v in record.values()):
                    records.append(record)

        return records, total_found

    def _try_zip_extraction(
        self,
        soup: BeautifulSoup,
        fields: List[Dict[str, Any]],
        max_records: int,
        field_stats: Dict[str, FieldStats]
    ) -> Tuple[List[Dict], int]:
        """개별 필드 추출 후 zip으로 결합"""
        field_values = {}
        max_items = 0

        for field in fields:
            selector = field.get('selector', '')
            if not selector:
                field_values[field['name']] = []
                continue

            elements = soup.select(selector)
            values = [self._extract_value_from_element(elem, field) for elem in elements]
            field_values[field['name']] = values
            max_items = max(max_items, len(values))

        # 통계 업데이트
        for name, values in field_values.items():
            found = sum(1 for v in values if v)
            empty = len(values) - found
            field_stats[name].found = found
            field_stats[name].empty = empty

        # zip으로 레코드 생성
        records = []
        for i in range(min(max_items, max_records)):
            record = {}
            for field in fields:
                name = field['name']
                values = field_values.get(name, [])
                record[name] = values[i] if i < len(values) else None
            records.append(record)

        return records, max_items

    def _find_common_parents(self, elements: List) -> List:
        """요소들의 공통 부모 찾기"""
        if not elements:
            return []

        # 각 요소의 부모 찾기
        parents = []
        for elem in elements:
            parent = elem.parent
            # 리스트 아이템 컨테이너 찾기 (li, tr, div 등)
            while parent and parent.name not in ['li', 'tr', 'article', 'div', 'body']:
                parent = parent.parent

            if parent and parent.name != 'body':
                parents.append(parent)

        # 유니크한 부모들 반환
        seen = set()
        unique_parents = []
        for p in parents:
            p_id = id(p)
            if p_id not in seen:
                seen.add(p_id)
                unique_parents.append(p)

        return unique_parents

    def _extract_field_value(self, container, field: Dict[str, Any]) -> Any:
        """컨테이너 내에서 필드 값 추출"""
        selector = field.get('selector', '')

        if not selector:
            return None

        # 컨테이너 내에서 선택자 적용
        try:
            # 셀렉터가 태그 이름만인 경우
            if selector in ['a', 'span', 'div', 'p', 'h1', 'h2', 'h3', 'td', 'th']:
                elem = container.find(selector)
            else:
                # CSS 선택자인 경우
                elem = container.select_one(selector)

            if elem:
                return self._extract_value_from_element(elem, field)

        except Exception as e:
            logger.debug(f"Selector failed: {selector}, error: {e}")

        return None

    def _extract_value_from_element(self, elem, field: Dict[str, Any]) -> Any:
        """요소에서 값 추출"""
        if elem is None:
            return None

        extraction_method = field.get('extraction_method', 'text')
        attribute = field.get('attribute')
        data_type = field.get('data_type', 'string')

        # 추출 방식에 따른 값 가져오기
        if extraction_method == 'href' or attribute == 'href':
            value = elem.get('href', '')
        elif extraction_method == 'attribute' and attribute:
            value = elem.get(attribute, '')
        else:
            value = elem.get_text(strip=True)

        # 데이터 타입에 따른 변환
        if data_type == 'number' and value:
            value = self._parse_number(value)
        elif data_type == 'date' and value:
            value = self._parse_date(value)

        return value if value else None

    def _find_sibling_or_parent_value(self, elem, field: Dict[str, Any]) -> Any:
        """형제 또는 부모 요소에서 값 찾기"""
        selector = field.get('selector', '')

        if not selector:
            return None

        # 부모에서 찾기
        parent = elem.parent
        for _ in range(3):  # 최대 3레벨 위까지
            if parent is None:
                break

            try:
                found = parent.select_one(selector)
                if found and found != elem:
                    return self._extract_value_from_element(found, field)
            except Exception:
                pass

            parent = parent.parent

        return None

    def _parse_number(self, value: str) -> Optional[float]:
        """숫자 파싱 (한글 단위 포함)"""
        if not value:
            return None

        # 한글 단위 변환
        value = str(value)
        value = value.replace('조', '0000억')
        value = value.replace('억', '0000만')
        value = value.replace('만', '0000')
        value = value.replace('천', '000')

        # 숫자만 추출
        numbers = re.findall(r'-?[\d,]+\.?\d*', value.replace(',', ''))
        if numbers:
            try:
                return float(numbers[0].replace(',', ''))
            except ValueError:
                pass

        return None

    def _parse_date(self, value: str) -> Optional[str]:
        """날짜 파싱"""
        if not value:
            return None

        # 이미 ISO 형식이면 그대로 반환
        if re.match(r'\d{4}-\d{2}-\d{2}', value):
            return value

        # 한글 날짜 형식 변환
        korean_pattern = r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일'
        match = re.search(korean_pattern, value)
        if match:
            return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"

        # 슬래시/점 구분자
        patterns = [
            r'(\d{4})[./](\d{1,2})[./](\d{1,2})',  # 2024/01/22
            r'(\d{1,2})[./](\d{1,2})[./](\d{4})',  # 01/22/2024
        ]

        for pattern in patterns:
            match = re.search(pattern, value)
            if match:
                groups = match.groups()
                if len(groups[0]) == 4:
                    return f"{groups[0]}-{int(groups[1]):02d}-{int(groups[2]):02d}"
                else:
                    return f"{groups[2]}-{int(groups[0]):02d}-{int(groups[1]):02d}"

        return value  # 파싱 실패 시 원본 반환

    def _generate_warnings(
        self,
        field_stats: Dict[str, FieldStats],
        fields: List[Dict[str, Any]],
        total_found: int
    ) -> List[str]:
        """경고 메시지 생성"""
        warnings = []

        for field in fields:
            name = field['name']
            stats = field_stats.get(name)

            if not stats:
                continue

            if stats.found == 0:
                warnings.append(f"'{name}' 필드에서 데이터를 찾지 못했습니다. 셀렉터를 확인하세요.")
            elif stats.empty > 0:
                empty_rate = stats.empty / (stats.found + stats.empty) * 100
                if empty_rate > 30:
                    warnings.append(f"'{name}' 필드가 {stats.empty}개 레코드에서 비어있습니다 ({empty_rate:.0f}%)")

        if total_found == 0:
            warnings.append("데이터를 찾지 못했습니다. URL과 셀렉터를 확인하세요.")
        elif total_found < 3:
            warnings.append(f"추출된 레코드가 적습니다 ({total_found}개). 셀렉터를 확인하세요.")

        return warnings
