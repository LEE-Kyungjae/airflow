"""
자동 복구 엔진 (Auto Recovery Engine)
예외 발생 시 자동으로 적절한 복구 전략을 실행

특징:
- 에러 코드 기반 복구 전략 선택
- 단계별 복구 시도 (escalation)
- Wellknown case 학습
- 복구 성공률 추적
- 관리자 알림 자동화
"""

import logging
import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


# ============================================
# 복구 전략 정의
# ============================================

class RecoveryStrategy(str, Enum):
    """복구 전략 타입"""
    RETRY = "retry"                          # 단순 재시도
    RETRY_WITH_BACKOFF = "retry_with_backoff"  # 지수 백오프 재시도
    INCREASE_TIMEOUT = "increase_timeout"     # 타임아웃 증가
    FIX_SELECTORS = "fix_selectors"           # GPT로 선택자 수정
    REGENERATE_CODE = "regenerate_code"       # GPT로 코드 재생성
    SWITCH_PROXY = "switch_proxy"             # 프록시 전환
    WAIT_AND_RETRY = "wait_and_retry"         # 대기 후 재시도
    APPLY_WELLKNOWN = "apply_wellknown"       # 알려진 해결책 적용
    NOTIFY_ADMIN = "notify_admin"             # 관리자 알림
    SKIP = "skip"                             # 건너뛰기
    FAIL = "fail"                             # 실패 처리


class RecoveryResult(str, Enum):
    """복구 결과"""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"
    ESCALATED = "escalated"


@dataclass
class RecoveryAttempt:
    """복구 시도 기록"""
    strategy: RecoveryStrategy
    result: RecoveryResult
    started_at: datetime
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> float:
        """소요 시간 (초)"""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0


@dataclass
class RecoverySession:
    """복구 세션"""
    session_id: str
    source_id: str
    error_code: str
    error_message: str
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    final_result: Optional[RecoveryResult] = None
    attempts: List[RecoveryAttempt] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)

    def add_attempt(self, attempt: RecoveryAttempt):
        """시도 추가"""
        self.attempts.append(attempt)

    @property
    def attempt_count(self) -> int:
        return len(self.attempts)

    @property
    def is_resolved(self) -> bool:
        return self.final_result == RecoveryResult.SUCCESS

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "source_id": self.source_id,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "final_result": self.final_result.value if self.final_result else None,
            "attempt_count": self.attempt_count,
            "attempts": [
                {
                    "strategy": a.strategy.value,
                    "result": a.result.value,
                    "duration": a.duration,
                    "error": a.error
                }
                for a in self.attempts
            ]
        }


# ============================================
# 에러 코드별 복구 전략 설정
# ============================================

ERROR_RECOVERY_CONFIG = {
    "E001": {  # 타임아웃
        "strategies": [
            RecoveryStrategy.INCREASE_TIMEOUT,
            RecoveryStrategy.RETRY_WITH_BACKOFF,
            RecoveryStrategy.NOTIFY_ADMIN
        ],
        "max_attempts": 5,
        "timeout_multiplier": 1.5
    },
    "E002": {  # 선택자 없음
        "strategies": [
            RecoveryStrategy.APPLY_WELLKNOWN,
            RecoveryStrategy.FIX_SELECTORS,
            RecoveryStrategy.REGENERATE_CODE,
            RecoveryStrategy.NOTIFY_ADMIN
        ],
        "max_attempts": 3
    },
    "E003": {  # 인증 필요
        "strategies": [
            RecoveryStrategy.NOTIFY_ADMIN,
            RecoveryStrategy.FAIL
        ],
        "max_attempts": 1
    },
    "E004": {  # 사이트 구조 변경
        "strategies": [
            RecoveryStrategy.APPLY_WELLKNOWN,
            RecoveryStrategy.REGENERATE_CODE,
            RecoveryStrategy.NOTIFY_ADMIN
        ],
        "max_attempts": 3
    },
    "E005": {  # Rate Limit
        "strategies": [
            RecoveryStrategy.WAIT_AND_RETRY,
            RecoveryStrategy.SWITCH_PROXY,
            RecoveryStrategy.NOTIFY_ADMIN
        ],
        "max_attempts": 5,
        "wait_times": [60, 300, 900, 1800, 3600]  # 1분, 5분, 15분, 30분, 1시간
    },
    "E006": {  # 파싱 에러
        "strategies": [
            RecoveryStrategy.FIX_SELECTORS,
            RecoveryStrategy.REGENERATE_CODE,
            RecoveryStrategy.NOTIFY_ADMIN
        ],
        "max_attempts": 3
    },
    "E007": {  # 연결 에러
        "strategies": [
            RecoveryStrategy.RETRY_WITH_BACKOFF,
            RecoveryStrategy.SWITCH_PROXY,
            RecoveryStrategy.NOTIFY_ADMIN
        ],
        "max_attempts": 5
    },
    "E008": {  # HTTP 에러
        "strategies": [
            RecoveryStrategy.RETRY_WITH_BACKOFF,
            RecoveryStrategy.NOTIFY_ADMIN
        ],
        "max_attempts": 3
    },
    "E009": {  # 파일 에러
        "strategies": [
            RecoveryStrategy.NOTIFY_ADMIN,
            RecoveryStrategy.FAIL
        ],
        "max_attempts": 1
    },
    "E010": {  # 알 수 없음
        "strategies": [
            RecoveryStrategy.RETRY,
            RecoveryStrategy.NOTIFY_ADMIN,
            RecoveryStrategy.FAIL
        ],
        "max_attempts": 2
    },
}


# ============================================
# 복구 액션 핸들러 (인터페이스)
# ============================================

class RecoveryHandler(ABC):
    """복구 핸들러 기본 클래스"""

    @abstractmethod
    async def execute(
        self,
        session: RecoverySession,
        context: Dict[str, Any]
    ) -> Tuple[RecoveryResult, Dict[str, Any]]:
        """
        복구 실행

        Returns:
            (결과, 추가 컨텍스트)
        """
        pass


class RetryHandler(RecoveryHandler):
    """단순 재시도"""

    async def execute(self, session: RecoverySession, context: Dict[str, Any]):
        try:
            crawl_func = context.get("crawl_function")
            if crawl_func:
                result = await crawl_func()
                if result.get("success"):
                    return RecoveryResult.SUCCESS, {"data": result}

            return RecoveryResult.FAILED, {"error": "재시도 실패"}
        except Exception as e:
            return RecoveryResult.FAILED, {"error": str(e)}


class RetryWithBackoffHandler(RecoveryHandler):
    """지수 백오프 재시도"""

    async def execute(self, session: RecoverySession, context: Dict[str, Any]):
        attempt_num = session.attempt_count
        delay = min(2 ** attempt_num, 60)  # 최대 60초

        logger.info(f"백오프 대기: {delay}초")
        await asyncio.sleep(delay)

        try:
            crawl_func = context.get("crawl_function")
            if crawl_func:
                result = await crawl_func()
                if result.get("success"):
                    return RecoveryResult.SUCCESS, {"data": result}

            return RecoveryResult.FAILED, {"error": "백오프 재시도 실패"}
        except Exception as e:
            return RecoveryResult.FAILED, {"error": str(e)}


class IncreaseTimeoutHandler(RecoveryHandler):
    """타임아웃 증가"""

    async def execute(self, session: RecoverySession, context: Dict[str, Any]):
        current_timeout = context.get("timeout", 30)
        config = ERROR_RECOVERY_CONFIG.get(session.error_code, {})
        multiplier = config.get("timeout_multiplier", 1.5)

        new_timeout = int(current_timeout * multiplier)
        context["timeout"] = new_timeout

        logger.info(f"타임아웃 증가: {current_timeout}s → {new_timeout}s")

        try:
            crawl_func = context.get("crawl_function")
            if crawl_func:
                result = await crawl_func(timeout=new_timeout)
                if result.get("success"):
                    return RecoveryResult.SUCCESS, {"new_timeout": new_timeout}

            return RecoveryResult.FAILED, {"error": "타임아웃 증가 후에도 실패"}
        except Exception as e:
            return RecoveryResult.FAILED, {"error": str(e)}


class WaitAndRetryHandler(RecoveryHandler):
    """대기 후 재시도"""

    async def execute(self, session: RecoverySession, context: Dict[str, Any]):
        config = ERROR_RECOVERY_CONFIG.get(session.error_code, {})
        wait_times = config.get("wait_times", [60])

        attempt_idx = min(session.attempt_count, len(wait_times) - 1)
        wait_time = wait_times[attempt_idx]

        # retry_after 헤더가 있으면 그 값 사용
        if "retry_after" in context:
            wait_time = context["retry_after"]

        logger.info(f"대기 후 재시도: {wait_time}초 대기")
        await asyncio.sleep(wait_time)

        try:
            crawl_func = context.get("crawl_function")
            if crawl_func:
                result = await crawl_func()
                if result.get("success"):
                    return RecoveryResult.SUCCESS, {"waited": wait_time}

            return RecoveryResult.FAILED, {"error": "대기 후에도 실패"}
        except Exception as e:
            return RecoveryResult.FAILED, {"error": str(e)}


class FixSelectorsHandler(RecoveryHandler):
    """GPT로 선택자 수정"""

    async def execute(self, session: RecoverySession, context: Dict[str, Any]):
        try:
            from .gpt_service import GPTService

            gpt = GPTService()
            current_code = context.get("current_code", "")
            html_snapshot = context.get("html_snapshot", "")

            if not current_code:
                return RecoveryResult.FAILED, {"error": "현재 코드 없음"}

            # GPT로 코드 수정
            fixed_code = gpt.fix_crawler_code(
                current_code=current_code,
                error_code=session.error_code,
                error_message=session.error_message,
                stack_trace=context.get("stack_trace", ""),
                html_snapshot=html_snapshot
            )

            # 수정된 코드 저장
            context["fixed_code"] = fixed_code

            # 코드 검증 및 실행
            crawl_func = context.get("code_executor")
            if crawl_func:
                result = await crawl_func(fixed_code)
                if result.get("success"):
                    return RecoveryResult.SUCCESS, {"fixed_code": fixed_code}

            return RecoveryResult.ESCALATED, {"fixed_code": fixed_code}

        except Exception as e:
            logger.error(f"선택자 수정 실패: {e}")
            return RecoveryResult.FAILED, {"error": str(e)}


class RegenerateCodeHandler(RecoveryHandler):
    """GPT로 코드 재생성"""

    async def execute(self, session: RecoverySession, context: Dict[str, Any]):
        try:
            from .gpt_service import GPTService

            gpt = GPTService()
            source_config = context.get("source_config", {})

            if not source_config:
                return RecoveryResult.FAILED, {"error": "소스 설정 없음"}

            # GPT로 코드 재생성
            new_code = gpt.generate_crawler_code(
                source_id=session.source_id,
                url=source_config.get("url", ""),
                data_type=source_config.get("type", "html"),
                fields=source_config.get("fields", []),
                html_sample=context.get("html_snapshot", "")
            )

            context["new_code"] = new_code

            # 코드 검증 및 실행
            crawl_func = context.get("code_executor")
            if crawl_func:
                result = await crawl_func(new_code)
                if result.get("success"):
                    return RecoveryResult.SUCCESS, {"new_code": new_code}

            return RecoveryResult.ESCALATED, {"new_code": new_code}

        except Exception as e:
            logger.error(f"코드 재생성 실패: {e}")
            return RecoveryResult.FAILED, {"error": str(e)}


class ApplyWellknownHandler(RecoveryHandler):
    """Wellknown case 적용"""

    async def execute(self, session: RecoverySession, context: Dict[str, Any]):
        try:
            wellknown_cases = context.get("wellknown_cases", [])

            if not wellknown_cases:
                # Wellknown case 검색
                pattern_hash = self._generate_pattern_hash(
                    session.error_code,
                    session.error_message
                )

                mongo = context.get("mongo")
                if mongo:
                    cases = mongo.db.wellknown_cases.find({
                        "pattern_hash": pattern_hash,
                        "success_rate": {"$gte": 0.6}
                    }).sort("success_rate", -1).limit(3)

                    wellknown_cases = list(cases)

            if not wellknown_cases:
                return RecoveryResult.SKIPPED, {"reason": "적용 가능한 wellknown case 없음"}

            # 가장 성공률 높은 case 적용
            best_case = wellknown_cases[0]
            solution = best_case.get("solution", {})

            # 솔루션 적용
            if solution.get("type") == "code_patch":
                context["fixed_code"] = solution.get("code")

                crawl_func = context.get("code_executor")
                if crawl_func:
                    result = await crawl_func(solution.get("code"))
                    if result.get("success"):
                        # 성공 기록
                        if mongo:
                            mongo.db.wellknown_cases.update_one(
                                {"_id": best_case["_id"]},
                                {
                                    "$inc": {"success_count": 1},
                                    "$set": {"last_success": datetime.utcnow()}
                                }
                            )
                        return RecoveryResult.SUCCESS, {"applied_case": best_case["_id"]}

            return RecoveryResult.FAILED, {"error": "wellknown case 적용 실패"}

        except Exception as e:
            logger.error(f"Wellknown case 적용 실패: {e}")
            return RecoveryResult.FAILED, {"error": str(e)}

    def _generate_pattern_hash(self, error_code: str, message: str) -> str:
        """에러 패턴 해시 생성"""
        import re
        normalized = re.sub(r'\d+', 'N', message)
        normalized = re.sub(r'https?://\S+', 'URL', normalized)
        normalized = normalized.lower().strip()
        content = f"{error_code}:{normalized}"
        return hashlib.md5(content.encode()).hexdigest()


class NotifyAdminHandler(RecoveryHandler):
    """관리자 알림"""

    async def execute(self, session: RecoverySession, context: Dict[str, Any]):
        try:
            # 알림 생성
            notification = {
                "type": "recovery_escalation",
                "source_id": session.source_id,
                "error_code": session.error_code,
                "error_message": session.error_message,
                "attempts": session.attempt_count,
                "timestamp": datetime.utcnow().isoformat(),
                "context": {
                    "url": context.get("source_config", {}).get("url"),
                    "last_error": session.attempts[-1].error if session.attempts else None
                }
            }

            # MongoDB에 알림 저장
            mongo = context.get("mongo")
            if mongo:
                mongo.db.admin_notifications.insert_one(notification)

            # 이메일/슬랙 알림 (옵션)
            notifier = context.get("notifier")
            if notifier:
                await notifier.send_alert(notification)

            logger.warning(
                f"관리자 알림 전송: source={session.source_id}, "
                f"error={session.error_code}"
            )

            return RecoveryResult.ESCALATED, {"notification_sent": True}

        except Exception as e:
            logger.error(f"관리자 알림 실패: {e}")
            return RecoveryResult.FAILED, {"error": str(e)}


# ============================================
# 복구 엔진
# ============================================

class AutoRecoveryEngine:
    """자동 복구 엔진"""

    HANDLERS: Dict[RecoveryStrategy, RecoveryHandler] = {
        RecoveryStrategy.RETRY: RetryHandler(),
        RecoveryStrategy.RETRY_WITH_BACKOFF: RetryWithBackoffHandler(),
        RecoveryStrategy.INCREASE_TIMEOUT: IncreaseTimeoutHandler(),
        RecoveryStrategy.WAIT_AND_RETRY: WaitAndRetryHandler(),
        RecoveryStrategy.FIX_SELECTORS: FixSelectorsHandler(),
        RecoveryStrategy.REGENERATE_CODE: RegenerateCodeHandler(),
        RecoveryStrategy.APPLY_WELLKNOWN: ApplyWellknownHandler(),
        RecoveryStrategy.NOTIFY_ADMIN: NotifyAdminHandler(),
    }

    def __init__(self, mongo=None, notifier=None):
        self.mongo = mongo
        self.notifier = notifier
        self.active_sessions: Dict[str, RecoverySession] = {}

    async def recover(
        self,
        source_id: str,
        error_code: str,
        error_message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> RecoverySession:
        """
        자동 복구 실행

        Args:
            source_id: 소스 ID
            error_code: 에러 코드 (E001-E010)
            error_message: 에러 메시지
            context: 추가 컨텍스트 (current_code, html_snapshot, crawl_function 등)

        Returns:
            RecoverySession
        """
        # 세션 생성
        session_id = f"recovery_{source_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        session = RecoverySession(
            session_id=session_id,
            source_id=source_id,
            error_code=error_code,
            error_message=error_message,
            context=context or {}
        )

        self.active_sessions[source_id] = session

        # 복구 설정 가져오기
        config = ERROR_RECOVERY_CONFIG.get(error_code, {
            "strategies": [RecoveryStrategy.NOTIFY_ADMIN],
            "max_attempts": 1
        })

        strategies = config["strategies"]
        max_attempts = config["max_attempts"]

        # 컨텍스트 준비
        ctx = {
            "mongo": self.mongo,
            "notifier": self.notifier,
            **(context or {})
        }

        logger.info(
            f"복구 시작: source={source_id}, error={error_code}, "
            f"strategies={[s.value for s in strategies]}"
        )

        # 전략별 복구 시도
        for strategy in strategies:
            if session.attempt_count >= max_attempts:
                logger.warning(f"최대 시도 횟수 도달: {max_attempts}")
                break

            handler = self.HANDLERS.get(strategy)
            if not handler:
                logger.warning(f"핸들러 없음: {strategy}")
                continue

            # 복구 시도
            attempt = RecoveryAttempt(
                strategy=strategy,
                result=RecoveryResult.FAILED,
                started_at=datetime.utcnow()
            )

            try:
                result, details = await handler.execute(session, ctx)
                attempt.result = result
                attempt.details = details
                attempt.completed_at = datetime.utcnow()

                logger.info(
                    f"복구 시도: strategy={strategy.value}, result={result.value}"
                )

                if result == RecoveryResult.SUCCESS:
                    session.final_result = RecoveryResult.SUCCESS
                    session.completed_at = datetime.utcnow()
                    session.add_attempt(attempt)

                    # 성공 패턴 학습
                    await self._learn_success(session, strategy, ctx)

                    logger.info(f"복구 성공: source={source_id}")
                    return session

                elif result == RecoveryResult.SKIPPED:
                    # 다음 전략으로
                    session.add_attempt(attempt)
                    continue

                elif result == RecoveryResult.ESCALATED:
                    # 에스컬레이션 (관리자 알림 등)
                    session.add_attempt(attempt)
                    # 다음 전략 계속

                else:
                    # 실패 - 다음 전략으로
                    session.add_attempt(attempt)

            except Exception as e:
                attempt.result = RecoveryResult.FAILED
                attempt.error = str(e)
                attempt.completed_at = datetime.utcnow()
                session.add_attempt(attempt)
                logger.error(f"복구 실행 중 오류: {e}")

        # 모든 전략 실패
        session.final_result = RecoveryResult.FAILED
        session.completed_at = datetime.utcnow()

        logger.warning(f"복구 실패: source={source_id}, attempts={session.attempt_count}")

        # 실패 기록
        if self.mongo:
            self.mongo.db.recovery_failures.insert_one(session.to_dict())

        return session

    async def _learn_success(
        self,
        session: RecoverySession,
        strategy: RecoveryStrategy,
        context: Dict[str, Any]
    ):
        """성공 패턴 학습"""
        try:
            if not self.mongo:
                return

            # 패턴 해시 생성
            pattern_hash = self._generate_pattern_hash(
                session.error_code,
                session.error_message
            )

            solution = {
                "type": "strategy",
                "strategy": strategy.value,
            }

            # 코드 수정인 경우 코드 저장
            if strategy in (RecoveryStrategy.FIX_SELECTORS, RecoveryStrategy.REGENERATE_CODE):
                solution["type"] = "code_patch"
                solution["code"] = context.get("fixed_code") or context.get("new_code")

            # Wellknown case 업데이트/생성
            self.mongo.db.wellknown_cases.update_one(
                {"pattern_hash": pattern_hash},
                {
                    "$set": {
                        "pattern_hash": pattern_hash,
                        "error_code": session.error_code,
                        "solution": solution,
                        "last_success": datetime.utcnow(),
                    },
                    "$inc": {"success_count": 1},
                    "$setOnInsert": {"created_at": datetime.utcnow()}
                },
                upsert=True
            )

            # 성공률 계산 업데이트
            case = self.mongo.db.wellknown_cases.find_one({"pattern_hash": pattern_hash})
            if case:
                total_attempts = case.get("total_attempts", 0) + 1
                success_count = case.get("success_count", 1)
                success_rate = success_count / total_attempts

                self.mongo.db.wellknown_cases.update_one(
                    {"pattern_hash": pattern_hash},
                    {
                        "$set": {
                            "success_rate": success_rate,
                            "total_attempts": total_attempts
                        }
                    }
                )

            logger.info(f"성공 패턴 학습: {pattern_hash[:8]}...")

        except Exception as e:
            logger.error(f"패턴 학습 실패: {e}")

    def _generate_pattern_hash(self, error_code: str, message: str) -> str:
        """에러 패턴 해시 생성"""
        import re
        normalized = re.sub(r'\d+', 'N', message)
        normalized = re.sub(r'https?://\S+', 'URL', normalized)
        normalized = normalized.lower().strip()
        content = f"{error_code}:{normalized}"
        return hashlib.md5(content.encode()).hexdigest()

    def get_session(self, source_id: str) -> Optional[RecoverySession]:
        """활성 세션 조회"""
        return self.active_sessions.get(source_id)

    def get_all_sessions(self) -> Dict[str, Dict]:
        """모든 세션 상태 조회"""
        return {
            source_id: session.to_dict()
            for source_id, session in self.active_sessions.items()
        }


# ============================================
# 전역 인스턴스 및 헬퍼
# ============================================

_recovery_engine: Optional[AutoRecoveryEngine] = None


def get_recovery_engine(mongo=None, notifier=None) -> AutoRecoveryEngine:
    """전역 복구 엔진 가져오기"""
    global _recovery_engine
    if _recovery_engine is None:
        _recovery_engine = AutoRecoveryEngine(mongo=mongo, notifier=notifier)
    return _recovery_engine


async def auto_recover(
    source_id: str,
    error_code: str,
    error_message: str,
    context: Optional[Dict[str, Any]] = None,
    mongo=None
) -> RecoverySession:
    """
    자동 복구 헬퍼 함수

    Usage:
        session = await auto_recover(
            source_id="my-source",
            error_code="E002",
            error_message="Selector not found: .article",
            context={
                "current_code": "...",
                "html_snapshot": "...",
                "crawl_function": async_crawl_func
            }
        )
    """
    engine = get_recovery_engine(mongo=mongo)
    return await engine.recover(
        source_id=source_id,
        error_code=error_code,
        error_message=error_message,
        context=context
    )
