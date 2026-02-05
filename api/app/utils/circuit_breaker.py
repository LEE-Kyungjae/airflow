"""
Circuit Breaker 패턴 구현
서비스 장애 시 자동으로 요청 차단하여 시스템 보호
"""

import asyncio
import time
import logging
from enum import Enum
from typing import Optional, Callable, Any, Dict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from collections import deque
import threading

from api.app.exceptions import CircuitOpenError

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit 상태"""
    CLOSED = "closed"       # 정상 - 요청 허용
    OPEN = "open"           # 열림 - 요청 차단
    HALF_OPEN = "half_open"  # 반열림 - 테스트 요청만 허용


@dataclass
class CircuitStats:
    """Circuit 통계"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rejected_requests: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    @property
    def failure_rate(self) -> float:
        """실패율 계산"""
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests

    @property
    def success_rate(self) -> float:
        """성공률 계산"""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests


@dataclass
class CircuitBreakerConfig:
    """Circuit Breaker 설정"""
    # 실패 임계값
    failure_threshold: int = 5           # 연속 실패 횟수
    failure_rate_threshold: float = 0.5  # 실패율 임계값 (50%)

    # 시간 설정
    reset_timeout: int = 60              # OPEN 상태 유지 시간 (초)
    half_open_max_calls: int = 3         # HALF_OPEN에서 테스트 요청 수

    # 슬라이딩 윈도우
    window_size: int = 10                # 통계 윈도우 크기
    min_calls_in_window: int = 5         # 최소 호출 수 (이하면 OPEN 안 함)

    # 성공 복구 조건
    success_threshold: int = 3           # HALF_OPEN에서 연속 성공 횟수


class CircuitBreaker:
    """Circuit Breaker 구현"""

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()
        self._last_state_change = datetime.utcnow()
        self._lock = threading.RLock()

        # 슬라이딩 윈도우 (성공=True, 실패=False)
        self._window: deque = deque(maxlen=self.config.window_size)

        # HALF_OPEN 상태에서의 호출 카운터
        self._half_open_calls = 0

    @property
    def state(self) -> CircuitState:
        """현재 상태 (자동 전이 포함)"""
        with self._lock:
            self._check_state_transition()
            return self._state

    @property
    def stats(self) -> CircuitStats:
        """통계 조회"""
        return self._stats

    def _check_state_transition(self):
        """상태 전이 체크"""
        now = datetime.utcnow()

        if self._state == CircuitState.OPEN:
            # OPEN → HALF_OPEN: reset_timeout 경과 시
            elapsed = (now - self._last_state_change).total_seconds()
            if elapsed >= self.config.reset_timeout:
                self._transition_to(CircuitState.HALF_OPEN)

    def _transition_to(self, new_state: CircuitState):
        """상태 전이"""
        old_state = self._state
        self._state = new_state
        self._last_state_change = datetime.utcnow()

        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._stats.consecutive_successes = 0

        logger.info(
            f"Circuit '{self.name}' 상태 변경: {old_state.value} → {new_state.value}"
        )

    def _should_open(self) -> bool:
        """OPEN 상태로 전환해야 하는지 확인"""
        # 최소 호출 수 미달
        if len(self._window) < self.config.min_calls_in_window:
            return False

        # 연속 실패 임계값 초과
        if self._stats.consecutive_failures >= self.config.failure_threshold:
            return True

        # 윈도우 내 실패율 임계값 초과
        failures_in_window = sum(1 for r in self._window if not r)
        failure_rate = failures_in_window / len(self._window)

        return failure_rate >= self.config.failure_rate_threshold

    def _should_close(self) -> bool:
        """CLOSED 상태로 복구해야 하는지 확인"""
        return self._stats.consecutive_successes >= self.config.success_threshold

    def record_success(self):
        """성공 기록"""
        with self._lock:
            self._stats.total_requests += 1
            self._stats.successful_requests += 1
            self._stats.consecutive_successes += 1
            self._stats.consecutive_failures = 0
            self._stats.last_success_time = datetime.utcnow()
            self._window.append(True)

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                if self._should_close():
                    self._transition_to(CircuitState.CLOSED)

    def record_failure(self, error: Optional[Exception] = None):
        """실패 기록"""
        with self._lock:
            self._stats.total_requests += 1
            self._stats.failed_requests += 1
            self._stats.consecutive_failures += 1
            self._stats.consecutive_successes = 0
            self._stats.last_failure_time = datetime.utcnow()
            self._window.append(False)

            if self._state == CircuitState.HALF_OPEN:
                # HALF_OPEN에서 실패하면 바로 OPEN
                self._transition_to(CircuitState.OPEN)
            elif self._state == CircuitState.CLOSED and self._should_open():
                self._transition_to(CircuitState.OPEN)

            if error:
                logger.warning(
                    f"Circuit '{self.name}' 실패 기록: {error}",
                    extra={"consecutive_failures": self._stats.consecutive_failures}
                )

    def allow_request(self) -> bool:
        """요청 허용 여부"""
        state = self.state  # 자동 전이 트리거

        if state == CircuitState.CLOSED:
            return True

        if state == CircuitState.OPEN:
            return False

        # HALF_OPEN: 제한된 수의 테스트 요청 허용
        with self._lock:
            if self._half_open_calls < self.config.half_open_max_calls:
                return True
            return False

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Circuit Breaker로 함수 호출"""
        if not self.allow_request():
            self._stats.rejected_requests += 1
            raise CircuitOpenError(
                service_name=self.name,
                failure_count=self._stats.consecutive_failures,
                reset_time=self.config.reset_timeout
            )

        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            raise

    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """Circuit Breaker로 비동기 함수 호출"""
        if not self.allow_request():
            self._stats.rejected_requests += 1
            raise CircuitOpenError(
                service_name=self.name,
                failure_count=self._stats.consecutive_failures,
                reset_time=self.config.reset_timeout
            )

        try:
            result = await func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            raise

    def reset(self):
        """수동 리셋"""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._stats = CircuitStats()
            self._window.clear()
            self._half_open_calls = 0
            self._last_state_change = datetime.utcnow()
            logger.info(f"Circuit '{self.name}' 수동 리셋")

    def get_status(self) -> Dict[str, Any]:
        """상태 정보 조회"""
        return {
            "name": self.name,
            "state": self.state.value,
            "stats": {
                "total_requests": self._stats.total_requests,
                "successful_requests": self._stats.successful_requests,
                "failed_requests": self._stats.failed_requests,
                "rejected_requests": self._stats.rejected_requests,
                "failure_rate": round(self._stats.failure_rate * 100, 2),
                "consecutive_failures": self._stats.consecutive_failures,
                "consecutive_successes": self._stats.consecutive_successes,
            },
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "reset_timeout": self.config.reset_timeout,
                "failure_rate_threshold": self.config.failure_rate_threshold,
            },
            "last_state_change": self._last_state_change.isoformat(),
        }


# ============================================
# Circuit Breaker 레지스트리
# ============================================

class CircuitBreakerRegistry:
    """Circuit Breaker 중앙 관리"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._breakers: Dict[str, CircuitBreaker] = {}
            return cls._instance

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Circuit Breaker 가져오기 또는 생성"""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, config)
        return self._breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Circuit Breaker 조회"""
        return self._breakers.get(name)

    def get_all_status(self) -> Dict[str, Dict]:
        """모든 Circuit 상태 조회"""
        return {
            name: breaker.get_status()
            for name, breaker in self._breakers.items()
        }

    def reset_all(self):
        """모든 Circuit 리셋"""
        for breaker in self._breakers.values():
            breaker.reset()


# 전역 레지스트리
circuit_registry = CircuitBreakerRegistry()


# ============================================
# 데코레이터
# ============================================

def circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    reset_timeout: int = 60,
    failure_rate_threshold: float = 0.5
):
    """Circuit Breaker 데코레이터"""
    config = CircuitBreakerConfig(
        failure_threshold=failure_threshold,
        reset_timeout=reset_timeout,
        failure_rate_threshold=failure_rate_threshold
    )
    breaker = circuit_registry.get_or_create(name, config)

    def decorator(func: Callable):
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await breaker.call_async(func, *args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


# ============================================
# 사전 정의된 Circuit Breaker
# ============================================

# GPT API용
gpt_circuit = circuit_registry.get_or_create(
    "gpt_api",
    CircuitBreakerConfig(
        failure_threshold=3,
        reset_timeout=120,
        failure_rate_threshold=0.5,
        half_open_max_calls=1
    )
)

# 외부 크롤링 대상용 (소스별로 동적 생성)
def get_source_circuit(source_id: str) -> CircuitBreaker:
    """소스별 Circuit Breaker"""
    return circuit_registry.get_or_create(
        f"source_{source_id}",
        CircuitBreakerConfig(
            failure_threshold=5,
            reset_timeout=300,  # 5분
            failure_rate_threshold=0.7,
            half_open_max_calls=2
        )
    )

# MongoDB용
mongo_circuit = circuit_registry.get_or_create(
    "mongodb",
    CircuitBreakerConfig(
        failure_threshold=3,
        reset_timeout=30,
        failure_rate_threshold=0.5,
        half_open_max_calls=1
    )
)