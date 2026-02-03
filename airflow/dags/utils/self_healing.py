"""
Self-Healing System - 자가 치유 AI 시스템

오류 발생 시 자동 진단 및 복구:
1. 출처 업데이트 상태 확인
2. Wellknown Case 매칭
3. AI 자동 해결 시도 (최대 N회 루프)
4. 관리자 개입 요청
5. 새로운 솔루션 Wellknown Case화
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from openai import OpenAI

logger = logging.getLogger(__name__)


class HealingStatus(str, Enum):
    """치유 상태"""
    PENDING = "pending"
    DIAGNOSING = "diagnosing"
    SOURCE_CHECK = "source_check"
    AI_SOLVING = "ai_solving"
    WAITING_ADMIN = "waiting_admin"
    RESOLVED = "resolved"
    FAILED = "failed"


class ErrorCategory(str, Enum):
    """에러 카테고리"""
    SOURCE_NOT_UPDATED = "source_not_updated"    # 출처가 업데이트 안됨
    STRUCTURE_CHANGED = "structure_changed"      # 사이트 구조 변경
    SELECTOR_BROKEN = "selector_broken"          # CSS 셀렉터 깨짐
    AUTH_REQUIRED = "auth_required"              # 인증 필요
    RATE_LIMITED = "rate_limited"                # 접근 제한
    NETWORK_ERROR = "network_error"              # 네트워크 오류
    PARSE_ERROR = "parse_error"                  # 파싱 오류
    DATA_VALIDATION = "data_validation"          # 데이터 검증 실패
    UNKNOWN = "unknown"


@dataclass
class RetrySchedule:
    """재시도 스케줄"""
    intervals: List[int] = None  # 분 단위

    def __post_init__(self):
        if self.intervals is None:
            # 3분, 10분, 30분, 2시간, 12시간, 1일, 2일, 3일, 5일, 30일
            self.intervals = [3, 10, 30, 120, 720, 1440, 2880, 4320, 7200, 43200]

    def get_next_retry(self, attempt: int) -> Optional[int]:
        """다음 재시도 간격 반환 (분)"""
        if attempt < len(self.intervals):
            return self.intervals[attempt]
        return None


@dataclass
class WellknownCase:
    """알려진 문제 케이스"""
    case_id: str
    error_pattern: str
    error_category: ErrorCategory
    solution_code: str
    solution_description: str
    success_count: int
    failure_count: int
    last_used: datetime
    created_at: datetime
    created_by: str  # 'ai' or 'admin'

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        return self.success_count / total if total > 0 else 0.0


@dataclass
class HealingSession:
    """치유 세션"""
    session_id: str
    source_id: str
    crawler_id: str
    error_code: str
    error_message: str
    stack_trace: str
    status: HealingStatus
    diagnosis: Optional[Dict[str, Any]]
    matched_case: Optional[str]  # WellknownCase ID
    attempts: List[Dict[str, Any]]
    current_attempt: int
    max_attempts: int
    admin_notified: bool
    resolved_at: Optional[datetime]
    resolution: Optional[str]
    created_at: datetime


class SelfHealingEngine:
    """자가 치유 엔진"""

    MAX_AI_ATTEMPTS = 5  # AI 자동 해결 최대 시도 횟수
    ADMIN_ADDITIONAL_ATTEMPTS = 3  # 관리자 승인 후 추가 시도

    def __init__(self, mongo_service=None):
        self.api_key = os.getenv('OPENAI_API_KEY')
        if self.api_key:
            self.client = OpenAI(api_key=self.api_key)
        else:
            self.client = None

        self.mongo = mongo_service
        self.retry_schedule = RetrySchedule()

    async def diagnose(
        self,
        source_id: str,
        crawler_id: str,
        error_code: str,
        error_message: str,
        stack_trace: str,
        html_snapshot: str = "",
        last_success_data: Optional[Dict] = None
    ) -> HealingSession:
        """
        오류 진단 시작

        1. 출처 업데이트 상태 확인
        2. Wellknown Case 매칭
        3. AI 분석
        """
        session_id = self._generate_session_id(source_id, error_code)

        session = HealingSession(
            session_id=session_id,
            source_id=source_id,
            crawler_id=crawler_id,
            error_code=error_code,
            error_message=error_message,
            stack_trace=stack_trace,
            status=HealingStatus.DIAGNOSING,
            diagnosis=None,
            matched_case=None,
            attempts=[],
            current_attempt=0,
            max_attempts=self.MAX_AI_ATTEMPTS,
            admin_notified=False,
            resolved_at=None,
            resolution=None,
            created_at=datetime.utcnow()
        )

        # 1단계: 진단
        diagnosis = await self._perform_diagnosis(
            error_code, error_message, stack_trace, html_snapshot
        )
        session.diagnosis = diagnosis

        # 2단계: 출처 업데이트 확인 필요한 경우
        if diagnosis.get('category') == ErrorCategory.SOURCE_NOT_UPDATED.value:
            session.status = HealingStatus.SOURCE_CHECK
            return session

        # 3단계: Wellknown Case 매칭
        matched_case = await self._match_wellknown_case(
            diagnosis, error_message, stack_trace
        )

        if matched_case:
            session.matched_case = matched_case.case_id
            session.status = HealingStatus.AI_SOLVING
        else:
            session.status = HealingStatus.WAITING_ADMIN
            session.admin_notified = True

        return session

    async def _perform_diagnosis(
        self,
        error_code: str,
        error_message: str,
        stack_trace: str,
        html_snapshot: str
    ) -> Dict[str, Any]:
        """GPT 기반 오류 진단"""

        if not self.client:
            return self._basic_diagnosis(error_code, error_message)

        prompt = f"""크롤러 오류를 분석하고 진단하세요.

[오류 정보]
오류 코드: {error_code}
오류 메시지: {error_message}
스택 트레이스: {stack_trace[:2000]}

[현재 페이지 HTML (일부)]
{html_snapshot[:3000]}

다음 JSON 형식으로 응답하세요:
{{
    "category": "source_not_updated|structure_changed|selector_broken|auth_required|rate_limited|network_error|parse_error|data_validation|unknown",
    "root_cause": "근본 원인 설명",
    "confidence": 0.0-1.0,
    "is_source_issue": true|false,
    "is_code_issue": true|false,
    "suggested_fix_type": "retry|selector_update|code_rewrite|manual_intervention",
    "key_indicators": ["판단 근거 1", "판단 근거 2"],
    "similar_pattern_hash": "유사 패턴 식별 해시"
}}

JSON만 출력하세요."""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "크롤러 오류 진단 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1000,
                temperature=0.1
            )

            result = response.choices[0].message.content.strip()
            if '```' in result:
                result = result.split('```')[1].replace('json', '').strip()

            return json.loads(result)

        except Exception as e:
            logger.error(f"GPT diagnosis failed: {e}")
            return self._basic_diagnosis(error_code, error_message)

    def _basic_diagnosis(self, error_code: str, error_message: str) -> Dict[str, Any]:
        """기본 진단 (GPT 없이)"""
        msg_lower = error_message.lower()

        category = ErrorCategory.UNKNOWN

        if 'timeout' in msg_lower or 'timed out' in msg_lower:
            category = ErrorCategory.NETWORK_ERROR
        elif 'selector' in msg_lower or 'not found' in msg_lower or 'none' in msg_lower:
            category = ErrorCategory.SELECTOR_BROKEN
        elif '401' in msg_lower or '403' in msg_lower or 'auth' in msg_lower:
            category = ErrorCategory.AUTH_REQUIRED
        elif '429' in msg_lower or 'rate' in msg_lower or 'blocked' in msg_lower:
            category = ErrorCategory.RATE_LIMITED
        elif 'parse' in msg_lower or 'json' in msg_lower or 'decode' in msg_lower:
            category = ErrorCategory.PARSE_ERROR

        return {
            "category": category.value,
            "root_cause": f"Error code {error_code}: {error_message[:200]}",
            "confidence": 0.5,
            "is_source_issue": category == ErrorCategory.SOURCE_NOT_UPDATED,
            "is_code_issue": category in [ErrorCategory.SELECTOR_BROKEN, ErrorCategory.PARSE_ERROR],
            "suggested_fix_type": "retry" if category == ErrorCategory.NETWORK_ERROR else "code_rewrite",
            "key_indicators": [error_code, error_message[:100]],
            "similar_pattern_hash": self._generate_pattern_hash(error_message)
        }

    async def _match_wellknown_case(
        self,
        diagnosis: Dict[str, Any],
        error_message: str,
        stack_trace: str
    ) -> Optional[WellknownCase]:
        """Wellknown Case 매칭"""
        if not self.mongo:
            return None

        pattern_hash = diagnosis.get('similar_pattern_hash') or self._generate_pattern_hash(error_message)
        category = diagnosis.get('category', 'unknown')

        # DB에서 매칭되는 케이스 검색
        cases = self.mongo.db.wellknown_cases.find({
            '$or': [
                {'error_pattern': pattern_hash},
                {'error_category': category}
            ],
            'success_rate': {'$gte': 0.6}  # 성공률 60% 이상만
        }).sort('success_count', -1).limit(5)

        best_case = None
        for case_doc in cases:
            case = WellknownCase(
                case_id=str(case_doc['_id']),
                error_pattern=case_doc['error_pattern'],
                error_category=ErrorCategory(case_doc['error_category']),
                solution_code=case_doc['solution_code'],
                solution_description=case_doc['solution_description'],
                success_count=case_doc['success_count'],
                failure_count=case_doc['failure_count'],
                last_used=case_doc['last_used'],
                created_at=case_doc['created_at'],
                created_by=case_doc['created_by']
            )

            if case.success_rate >= 0.6:
                best_case = case
                break

        return best_case

    async def attempt_healing(
        self,
        session: HealingSession,
        current_code: str,
        html_snapshot: str = ""
    ) -> Tuple[bool, str, Optional[str]]:
        """
        치유 시도

        Returns:
            (성공여부, 결과메시지, 새로운코드 또는 None)
        """
        session.current_attempt += 1
        attempt_record = {
            'attempt': session.current_attempt,
            'timestamp': datetime.utcnow().isoformat(),
            'method': None,
            'result': None
        }

        # 1. Wellknown Case 적용 시도
        if session.matched_case:
            case = await self._get_wellknown_case(session.matched_case)
            if case:
                attempt_record['method'] = f'wellknown_case_{case.case_id}'
                success, new_code = await self._apply_wellknown_solution(
                    case, current_code, session.diagnosis
                )

                if success:
                    attempt_record['result'] = 'success'
                    session.attempts.append(attempt_record)
                    await self._update_case_stats(case.case_id, success=True)
                    return True, "Wellknown Case 솔루션 적용 성공", new_code
                else:
                    attempt_record['result'] = 'failed'
                    await self._update_case_stats(case.case_id, success=False)

        # 2. AI 자동 해결 시도
        attempt_record['method'] = 'ai_auto_fix'

        new_code = await self._ai_generate_fix(
            current_code=current_code,
            error_message=session.error_message,
            stack_trace=session.stack_trace,
            diagnosis=session.diagnosis,
            html_snapshot=html_snapshot,
            previous_attempts=session.attempts
        )

        if new_code:
            attempt_record['result'] = 'generated'
            session.attempts.append(attempt_record)
            return True, "AI 코드 수정 생성 완료", new_code
        else:
            attempt_record['result'] = 'failed'
            session.attempts.append(attempt_record)

            # 최대 시도 횟수 체크
            if session.current_attempt >= session.max_attempts:
                if not session.admin_notified:
                    session.status = HealingStatus.WAITING_ADMIN
                    session.admin_notified = True
                    return False, "최대 시도 횟수 초과. 관리자 검토 필요.", None
                else:
                    session.status = HealingStatus.FAILED
                    return False, "모든 자동 복구 시도 실패", None

            return False, f"시도 {session.current_attempt}/{session.max_attempts} 실패", None

    async def _ai_generate_fix(
        self,
        current_code: str,
        error_message: str,
        stack_trace: str,
        diagnosis: Dict[str, Any],
        html_snapshot: str,
        previous_attempts: List[Dict]
    ) -> Optional[str]:
        """AI 기반 코드 수정 생성"""

        if not self.client:
            return None

        # 이전 시도 정보 포함
        attempts_info = ""
        if previous_attempts:
            attempts_info = "\n[이전 시도 이력]\n"
            for att in previous_attempts[-3:]:  # 최근 3개만
                attempts_info += f"- 시도 {att['attempt']}: {att['method']} -> {att['result']}\n"

        prompt = f"""크롤러 코드를 수정하여 오류를 해결하세요.

[진단 결과]
카테고리: {diagnosis.get('category')}
근본 원인: {diagnosis.get('root_cause')}
수정 유형: {diagnosis.get('suggested_fix_type')}

[오류 정보]
{error_message}

[스택 트레이스]
{stack_trace[:1500]}
{attempts_info}
[현재 코드]
{current_code}

[현재 페이지 HTML (샘플)]
{html_snapshot[:4000]}

[지시사항]
1. 오류 원인을 분석하고 수정된 코드를 생성하세요
2. 이전 시도에서 실패한 방법은 피하세요
3. 셀렉터가 깨진 경우 HTML에서 새로운 셀렉터를 찾으세요
4. 변경 사항을 주석으로 표시하세요
5. 방어적 프로그래밍 (try-except, None 체크) 추가

수정된 전체 코드만 출력하세요. ```python 없이."""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "크롤러 코드 오류 수정 전문가입니다. 정확하고 안정적인 코드를 생성합니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=4000,
                temperature=0.2
            )

            code = response.choices[0].message.content.strip()

            # 코드 블록 제거
            if code.startswith('```'):
                code = code.split('```')[1]
                if code.startswith('python'):
                    code = code[6:]
                code = code.strip()

            return code

        except Exception as e:
            logger.error(f"AI fix generation failed: {e}")
            return None

    async def check_source_update(
        self,
        source_id: str,
        url: str,
        last_known_hash: Optional[str] = None
    ) -> Dict[str, Any]:
        """출처 업데이트 상태 확인"""
        import httpx
        import hashlib

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })

                content = response.text
                current_hash = hashlib.md5(content.encode()).hexdigest()

                # 해시 비교
                is_updated = last_known_hash is None or current_hash != last_known_hash

                return {
                    'accessible': True,
                    'status_code': response.status_code,
                    'is_updated': is_updated,
                    'content_hash': current_hash,
                    'content_length': len(content),
                    'checked_at': datetime.utcnow().isoformat()
                }

        except Exception as e:
            return {
                'accessible': False,
                'error': str(e),
                'checked_at': datetime.utcnow().isoformat()
            }

    async def schedule_retry(
        self,
        session: HealingSession,
        attempt_number: int
    ) -> Optional[datetime]:
        """재시도 스케줄링"""
        interval = self.retry_schedule.get_next_retry(attempt_number)

        if interval is None:
            return None

        next_retry = datetime.utcnow() + timedelta(minutes=interval)

        # DB에 스케줄 저장
        if self.mongo:
            self.mongo.db.healing_schedules.insert_one({
                'session_id': session.session_id,
                'source_id': session.source_id,
                'scheduled_at': next_retry,
                'attempt_number': attempt_number + 1,
                'created_at': datetime.utcnow()
            })

        return next_retry

    async def register_wellknown_case(
        self,
        session: HealingSession,
        solution_code: str,
        description: str,
        created_by: str = 'ai'
    ) -> str:
        """성공한 솔루션을 Wellknown Case로 등록"""
        if not self.mongo:
            return ""

        pattern_hash = self._generate_pattern_hash(session.error_message)
        category = session.diagnosis.get('category', 'unknown')

        case_data = {
            'error_pattern': pattern_hash,
            'error_category': category,
            'solution_code': solution_code,
            'solution_description': description,
            'original_error': session.error_message[:500],
            'success_count': 1,
            'failure_count': 0,
            'last_used': datetime.utcnow(),
            'created_at': datetime.utcnow(),
            'created_by': created_by,
            'source_session': session.session_id
        }

        result = self.mongo.db.wellknown_cases.insert_one(case_data)
        logger.info(f"Registered new wellknown case: {result.inserted_id}")

        return str(result.inserted_id)

    async def admin_approve_continue(
        self,
        session: HealingSession,
        additional_attempts: int = 3
    ) -> HealingSession:
        """관리자 승인 후 추가 시도 허용"""
        session.max_attempts += additional_attempts
        session.status = HealingStatus.AI_SOLVING
        session.admin_notified = True

        logger.info(f"Admin approved {additional_attempts} additional attempts for session {session.session_id}")

        return session

    async def _get_wellknown_case(self, case_id: str) -> Optional[WellknownCase]:
        """Wellknown Case 조회"""
        if not self.mongo:
            return None

        from bson import ObjectId
        case_doc = self.mongo.db.wellknown_cases.find_one({'_id': ObjectId(case_id)})

        if not case_doc:
            return None

        return WellknownCase(
            case_id=str(case_doc['_id']),
            error_pattern=case_doc['error_pattern'],
            error_category=ErrorCategory(case_doc['error_category']),
            solution_code=case_doc['solution_code'],
            solution_description=case_doc['solution_description'],
            success_count=case_doc['success_count'],
            failure_count=case_doc['failure_count'],
            last_used=case_doc['last_used'],
            created_at=case_doc['created_at'],
            created_by=case_doc['created_by']
        )

    async def _apply_wellknown_solution(
        self,
        case: WellknownCase,
        current_code: str,
        diagnosis: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """Wellknown Case 솔루션 적용"""
        try:
            # 솔루션 코드가 템플릿인 경우 현재 코드와 병합
            if '{{ORIGINAL_CODE}}' in case.solution_code:
                new_code = case.solution_code.replace('{{ORIGINAL_CODE}}', current_code)
            else:
                new_code = case.solution_code

            return True, new_code

        except Exception as e:
            logger.error(f"Failed to apply wellknown solution: {e}")
            return False, None

    async def _update_case_stats(self, case_id: str, success: bool):
        """Wellknown Case 통계 업데이트"""
        if not self.mongo:
            return

        from bson import ObjectId
        update = {
            '$set': {'last_used': datetime.utcnow()},
            '$inc': {'success_count' if success else 'failure_count': 1}
        }

        self.mongo.db.wellknown_cases.update_one(
            {'_id': ObjectId(case_id)},
            update
        )

    def _generate_session_id(self, source_id: str, error_code: str) -> str:
        """세션 ID 생성"""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        return f"heal_{source_id[:8]}_{error_code}_{timestamp}"

    def _generate_pattern_hash(self, error_message: str) -> str:
        """에러 패턴 해시 생성"""
        # 동적 부분 제거 후 해시
        import re
        normalized = re.sub(r'\d+', 'N', error_message.lower())
        normalized = re.sub(r'0x[a-f0-9]+', 'ADDR', normalized)
        normalized = re.sub(r'line \d+', 'line N', normalized)
        normalized = re.sub(r'at \d+:\d+', 'at N:N', normalized)

        return hashlib.md5(normalized.encode()).hexdigest()[:12]


class HealingOrchestrator:
    """치유 프로세스 오케스트레이터"""

    def __init__(self, mongo_service=None):
        self.engine = SelfHealingEngine(mongo_service)
        self.mongo = mongo_service

    async def run_healing_pipeline(
        self,
        source_id: str,
        crawler_id: str,
        error_code: str,
        error_message: str,
        stack_trace: str,
        current_code: str,
        html_snapshot: str = "",
        url: str = ""
    ) -> Dict[str, Any]:
        """
        전체 치유 파이프라인 실행

        1. 진단
        2. 출처 확인 (필요시)
        3. AI 해결 시도
        4. 결과 반환
        """
        result = {
            'success': False,
            'session_id': None,
            'status': None,
            'new_code': None,
            'message': '',
            'next_action': None,
            'retry_at': None
        }

        # 1. 진단
        session = await self.engine.diagnose(
            source_id=source_id,
            crawler_id=crawler_id,
            error_code=error_code,
            error_message=error_message,
            stack_trace=stack_trace,
            html_snapshot=html_snapshot
        )

        result['session_id'] = session.session_id
        result['status'] = session.status.value

        # 2. 출처 체크 필요한 경우
        if session.status == HealingStatus.SOURCE_CHECK:
            source_status = await self.engine.check_source_update(source_id, url)

            if not source_status.get('accessible'):
                # 출처 접근 불가 - 재시도 스케줄
                retry_at = await self.engine.schedule_retry(session, 0)
                result['message'] = "출처 접근 불가. 재시도 예약됨."
                result['retry_at'] = retry_at.isoformat() if retry_at else None
                result['next_action'] = 'wait_retry'
                return result

            if not source_status.get('is_updated'):
                # 출처가 업데이트 안됨 - 재시도 스케줄
                retry_at = await self.engine.schedule_retry(session, 0)
                result['message'] = "출처가 아직 업데이트되지 않음. 재시도 예약됨."
                result['retry_at'] = retry_at.isoformat() if retry_at else None
                result['next_action'] = 'wait_retry'
                return result

        # 3. AI 해결 시도
        if session.status == HealingStatus.AI_SOLVING:
            success, message, new_code = await self.engine.attempt_healing(
                session=session,
                current_code=current_code,
                html_snapshot=html_snapshot
            )

            result['success'] = success
            result['message'] = message
            result['new_code'] = new_code
            result['status'] = session.status.value

            if success and new_code:
                result['next_action'] = 'apply_and_test'

                # 성공 시 Wellknown Case 등록
                if not session.matched_case:
                    case_id = await self.engine.register_wellknown_case(
                        session=session,
                        solution_code=new_code,
                        description=f"Auto-fixed: {error_code}",
                        created_by='ai'
                    )
                    result['new_case_id'] = case_id

            elif session.status == HealingStatus.WAITING_ADMIN:
                result['next_action'] = 'notify_admin'
            else:
                result['next_action'] = 'retry'

        # 4. 관리자 개입 필요
        elif session.status == HealingStatus.WAITING_ADMIN:
            result['message'] = "자동 해결 실패. 관리자 검토가 필요합니다."
            result['next_action'] = 'notify_admin'

        # 세션 저장
        await self._save_session(session)

        return result

    async def _save_session(self, session: HealingSession):
        """세션 저장"""
        if not self.mongo:
            return

        session_data = {
            'session_id': session.session_id,
            'source_id': session.source_id,
            'crawler_id': session.crawler_id,
            'error_code': session.error_code,
            'error_message': session.error_message[:1000],
            'status': session.status.value,
            'diagnosis': session.diagnosis,
            'matched_case': session.matched_case,
            'attempts': session.attempts,
            'current_attempt': session.current_attempt,
            'max_attempts': session.max_attempts,
            'admin_notified': session.admin_notified,
            'resolved_at': session.resolved_at,
            'resolution': session.resolution,
            'created_at': session.created_at,
            'updated_at': datetime.utcnow()
        }

        self.mongo.db.healing_sessions.update_one(
            {'session_id': session.session_id},
            {'$set': session_data},
            upsert=True
        )
