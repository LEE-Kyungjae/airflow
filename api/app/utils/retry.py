"""
재시도 로직 구현
지수 백오프, 지터, 조건부 재시도 지원
"""

import asyncio
import random
import time
import logging
from enum import Enum
from typing import Callable, Optional, Tuple, Type, Union, List, Any
from dataclasses import dataclass, field
from functools import wraps

logger = logging.getLogger(__name__)


class RetryStrategy(str, Enum):
    """재시도 전략"""
    FIXED = "fixed"                # 고정 간격
    LINEAR = "linear"              # 선형 증가
    EXPONENTIAL = "exponential"    # 지수 백오프
    FIBONACCI = "fibonacci"        # 피보나치 시퀀스


@dataclass
class RetryConfig:
    """재시도 설정"""
    max_retries: int = 3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    base_delay: float = 1.0        # 기본 대기 시간 (초)
    max_delay: float = 60.0        # 최대 대기 시간 (초)
    jitter: bool = True            # 무작위 지터 추가
    jitter_range: Tuple[float, float] = (0.5, 1.5)  # 지터 배율 범위

    # 재시도할 예외 타입 (비어있으면 모든 예외)
    retry_exceptions: List[Type[Exception]] = field(default_factory=list)

    # 재시도하지 않을 예외 타입
    exclude_exceptions: List[Type[Exception]] = field(default_factory=list)

    # 조건부 재시도 함수 (예외를 받아 재시도 여부 반환)
    retry_condition: Optional[Callable[[Exception], bool]] = None

    # 재시도 전 콜백
    on_retry: Optional[Callable[[int, Exception, float], None]] = None


def calculate_delay(
    attempt: int,
    config: RetryConfig
) -> float:
    """재시도 대기 시간 계산"""
    if config.strategy == RetryStrategy.FIXED:
        delay = config.base_delay

    elif config.strategy == RetryStrategy.LINEAR:
        delay = config.base_delay * (attempt + 1)

    elif config.strategy == RetryStrategy.EXPONENTIAL:
        delay = config.base_delay * (2 ** attempt)

    elif config.strategy == RetryStrategy.FIBONACCI:
        # 피보나치 시퀀스: 1, 1, 2, 3, 5, 8, 13, ...
        a, b = 1, 1
        for _ in range(attempt):
            a, b = b, a + b
        delay = config.base_delay * a

    else:
        delay = config.base_delay

    # 최대 대기 시간 제한
    delay = min(delay, config.max_delay)

    # 지터 적용
    if config.jitter:
        jitter_multiplier = random.uniform(*config.jitter_range)
        delay *= jitter_multiplier

    return delay


def should_retry(
    exception: Exception,
    config: RetryConfig
) -> bool:
    """재시도 여부 결정"""
    # 제외 예외 체크
    if config.exclude_exceptions:
        for exc_type in config.exclude_exceptions:
            if isinstance(exception, exc_type):
                return False

    # 커스텀 조건 체크
    if config.retry_condition:
        return config.retry_condition(exception)

    # 특정 예외만 재시도
    if config.retry_exceptions:
        for exc_type in config.retry_exceptions:
            if isinstance(exception, exc_type):
                return True
        return False

    # 기본: 모든 예외 재시도
    return True


def retry_with_backoff(
    config: Optional[RetryConfig] = None,
    **config_kwargs
):
    """동기 함수용 재시도 데코레이터"""
    if config is None:
        config = RetryConfig(**config_kwargs)

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    last_exception = e

                    # 마지막 시도면 예외 발생
                    if attempt >= config.max_retries:
                        logger.error(
                            f"최대 재시도 횟수 초과: {func.__name__}",
                            extra={"attempts": attempt + 1, "error": str(e)}
                        )
                        raise

                    # 재시도 여부 확인
                    if not should_retry(e, config):
                        logger.warning(
                            f"재시도 불가 예외: {type(e).__name__}",
                            extra={"function": func.__name__}
                        )
                        raise

                    # 대기 시간 계산
                    delay = calculate_delay(attempt, config)

                    logger.warning(
                        f"재시도 예정: {func.__name__} (attempt {attempt + 1}/{config.max_retries})",
                        extra={
                            "delay": round(delay, 2),
                            "error": str(e)
                        }
                    )

                    # 콜백 호출
                    if config.on_retry:
                        config.on_retry(attempt + 1, e, delay)

                    time.sleep(delay)

            raise last_exception

        return wrapper
    return decorator


def async_retry_with_backoff(
    config: Optional[RetryConfig] = None,
    **config_kwargs
):
    """비동기 함수용 재시도 데코레이터"""
    if config is None:
        config = RetryConfig(**config_kwargs)

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(config.max_retries + 1):
                try:
                    return await func(*args, **kwargs)

                except Exception as e:
                    last_exception = e

                    # 마지막 시도면 예외 발생
                    if attempt >= config.max_retries:
                        logger.error(
                            f"최대 재시도 횟수 초과: {func.__name__}",
                            extra={"attempts": attempt + 1, "error": str(e)}
                        )
                        raise

                    # 재시도 여부 확인
                    if not should_retry(e, config):
                        logger.warning(
                            f"재시도 불가 예외: {type(e).__name__}",
                            extra={"function": func.__name__}
                        )
                        raise

                    # 대기 시간 계산
                    delay = calculate_delay(attempt, config)

                    logger.warning(
                        f"재시도 예정: {func.__name__} (attempt {attempt + 1}/{config.max_retries})",
                        extra={
                            "delay": round(delay, 2),
                            "error": str(e)
                        }
                    )

                    # 콜백 호출
                    if config.on_retry:
                        config.on_retry(attempt + 1, e, delay)

                    await asyncio.sleep(delay)

            raise last_exception

        return wrapper
    return decorator


class RetryContext:
    """재시도 컨텍스트 매니저"""

    def __init__(self, config: Optional[RetryConfig] = None, **config_kwargs):
        self.config = config or RetryConfig(**config_kwargs)
        self.attempt = 0
        self.last_exception: Optional[Exception] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None:
            return False

        self.last_exception = exc_val

        # 재시도 가능한지 확인
        if self.attempt >= self.config.max_retries:
            return False

        if not should_retry(exc_val, self.config):
            return False

        return True  # 예외 억제

    def should_continue(self) -> bool:
        """계속 시도해야 하는지"""
        return self.attempt <= self.config.max_retries

    def next_attempt(self) -> float:
        """다음 시도 및 대기 시간 반환"""
        delay = calculate_delay(self.attempt, self.config)
        self.attempt += 1
        return delay


async def retry_async_operation(
    operation: Callable,
    config: Optional[RetryConfig] = None,
    **config_kwargs
) -> Any:
    """비동기 연산 재시도 헬퍼"""
    if config is None:
        config = RetryConfig(**config_kwargs)

    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return await operation()

        except Exception as e:
            last_exception = e

            if attempt >= config.max_retries:
                raise

            if not should_retry(e, config):
                raise

            delay = calculate_delay(attempt, config)

            if config.on_retry:
                config.on_retry(attempt + 1, e, delay)

            await asyncio.sleep(delay)

    raise last_exception


def retry_sync_operation(
    operation: Callable,
    config: Optional[RetryConfig] = None,
    **config_kwargs
) -> Any:
    """동기 연산 재시도 헬퍼"""
    if config is None:
        config = RetryConfig(**config_kwargs)

    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return operation()

        except Exception as e:
            last_exception = e

            if attempt >= config.max_retries:
                raise

            if not should_retry(e, config):
                raise

            delay = calculate_delay(attempt, config)

            if config.on_retry:
                config.on_retry(attempt + 1, e, delay)

            time.sleep(delay)

    raise last_exception


# ============================================
# 사전 정의된 재시도 설정
# ============================================

# GPT API용 (Rate Limit 고려)
GPT_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    strategy=RetryStrategy.EXPONENTIAL,
    base_delay=2.0,
    max_delay=60.0,
    jitter=True,
)

# HTTP 요청용
HTTP_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    strategy=RetryStrategy.EXPONENTIAL,
    base_delay=1.0,
    max_delay=30.0,
    jitter=True,
)

# 데이터베이스용
DB_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    strategy=RetryStrategy.LINEAR,
    base_delay=0.5,
    max_delay=5.0,
    jitter=False,
)

# Self-Healing용 (긴 간격)
HEALING_RETRY_CONFIG = RetryConfig(
    max_retries=5,
    strategy=RetryStrategy.FIBONACCI,
    base_delay=60.0,  # 1분부터 시작
    max_delay=1800.0,  # 최대 30분
    jitter=True,
)