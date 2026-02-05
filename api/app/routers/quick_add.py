"""
Quick Add Router - 원클릭 소스 등록 API

URL만 입력하면 10분 내 완전 자동화:
1. 자동 페이지 분석
2. 필드 자동 추출
3. 스케줄 자동 설정
4. 크롤러 코드 자동 생성
5. DAG 자동 생성 및 활성화
"""

import logging
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from pydantic import BaseModel, Field, HttpUrl

from app.services.mongo_service import MongoService
from app.services.auto_discovery import AutoDiscoveryService, BatchDiscoveryService, SourceDiscoveryResult
from app.services.airflow_trigger import AirflowTrigger
from app.services.test_crawler import TestCrawlerService
from app.services.instant_etl import InstantETLService

logger = logging.getLogger(__name__)
router = APIRouter()


# ============== Request/Response Models ==============

class QuickAddRequest(BaseModel):
    """원클릭 등록 요청"""
    url: str = Field(..., description="크롤링 대상 URL")
    name: Optional[str] = Field(None, description="소스 이름 (자동 생성됨)")
    hint: Optional[str] = Field(None, description="힌트 (예: '뉴스', '주가', '환율')")
    auto_start: bool = Field(True, description="즉시 시작 여부")

    model_config = {
        "json_schema_extra": {
            "example": {
                "url": "https://finance.naver.com/sise/",
                "hint": "주식시황",
                "auto_start": True
            }
        }
    }


class BatchAddRequest(BaseModel):
    """배치 등록 요청"""
    urls: List[str] = Field(..., min_length=1, max_length=50, description="URL 목록")
    hints: Optional[dict] = Field(None, description="URL별 힌트")
    auto_start: bool = Field(True, description="즉시 시작 여부")


class AnalyzeRequest(BaseModel):
    """분석만 요청 (등록 없이)"""
    url: str = Field(..., description="분석할 URL")
    hint: Optional[str] = Field(None, description="힌트")
    deep_analysis: bool = Field(True, description="GPT 심층 분석 여부")


class DiscoveryResponse(BaseModel):
    """자동 발견 결과"""
    url: str
    page_type: str
    strategy: str
    requires_js: bool
    has_pagination: bool
    recommended_schedule: str
    schedule_reason: str
    data_freshness: str
    confidence_score: float
    fields: List[dict]
    sample_data: List[dict]
    warnings: List[str]


class QuickAddResponse(BaseModel):
    """원클릭 등록 응답"""
    success: bool
    source_id: Optional[str] = None
    crawler_id: Optional[str] = None
    dag_id: Optional[str] = None
    message: str
    discovery: Optional[DiscoveryResponse] = None
    estimated_first_run: Optional[str] = None


class BatchAddResponse(BaseModel):
    """배치 등록 응답"""
    success: bool
    total: int
    succeeded: int
    failed: int
    results: List[QuickAddResponse]


class TestCrawlRequest(BaseModel):
    """테스트 크롤링 요청"""
    url: str = Field(..., description="크롤링 대상 URL")
    fields: List[dict] = Field(..., description="추출할 필드 목록")
    max_records: int = Field(10, ge=1, le=100, description="최대 추출 레코드 수")

    model_config = {
        "json_schema_extra": {
            "example": {
                "url": "https://news.naver.com",
                "fields": [
                    {"name": "title", "selector": ".news_tit", "data_type": "string"},
                    {"name": "link", "selector": ".news_tit", "data_type": "url", "attribute": "href"}
                ],
                "max_records": 10
            }
        }
    }


class TestCrawlResponse(BaseModel):
    """테스트 크롤링 응답"""
    success: bool
    records: List[dict]
    total_found: int
    extracted_count: int
    extraction_time_ms: int
    field_stats: dict
    warnings: List[str]


class CreateRequest(BaseModel):
    """커스텀 필드로 ETL 생성 요청"""
    url: str = Field(..., description="크롤링 대상 URL")
    name: Optional[str] = Field(None, description="소스 이름 (자동 생성 가능)")
    fields: List[dict] = Field(..., description="추출할 필드 목록")
    schedule: Optional[str] = Field(None, description="Cron 스케줄 (자동 추천 가능)")
    run_test: bool = Field(True, description="생성 전 테스트 실행 여부")
    auto_start: bool = Field(True, description="DAG 즉시 트리거 여부")
    instant: bool = Field(True, description="즉시 생성 모드 (source_manager DAG 건너뛰기)")

    model_config = {
        "json_schema_extra": {
            "example": {
                "url": "https://news.naver.com",
                "name": "네이버뉴스",
                "fields": [
                    {"name": "title", "selector": ".news_tit", "data_type": "string"},
                    {"name": "link", "selector": ".news_tit", "data_type": "url", "attribute": "href"}
                ],
                "schedule": "0 9 * * *",
                "run_test": True,
                "auto_start": True,
                "instant": True
            }
        }
    }


class TestResultSummary(BaseModel):
    """테스트 결과 요약"""
    success: bool
    records_found: int
    warnings: List[str] = []


# ============== Dependencies ==============

def get_mongo():
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


# ============== Endpoints ==============

@router.post("/analyze", response_model=DiscoveryResponse)
async def analyze_url(
    request: AnalyzeRequest,
    mongo: MongoService = Depends(get_mongo)
):
    """
    URL 자동 분석 (등록 없이)

    - 페이지 타입 자동 감지
    - 추출 필드 자동 추천
    - 스케줄 자동 추천
    - 샘플 데이터 추출
    """
    discovery = AutoDiscoveryService()

    try:
        result = await discovery.discover(
            url=request.url,
            hint=request.hint,
            deep_analysis=request.deep_analysis
        )

        return DiscoveryResponse(
            url=result.url,
            page_type=result.page_type.value,
            strategy=result.strategy.value,
            requires_js=result.requires_js,
            has_pagination=result.has_pagination,
            recommended_schedule=result.recommended_schedule,
            schedule_reason=result.schedule_reason,
            data_freshness=result.data_freshness,
            confidence_score=result.confidence_score,
            fields=[{
                'name': f.name,
                'selector': f.selector,
                'data_type': f.data_type,
                'sample_value': f.sample_value,
                'confidence': f.confidence
            } for f in result.recommended_fields],
            sample_data=result.sample_data,
            warnings=result.warnings
        )

    except Exception as e:
        logger.error(f"Analysis failed for {request.url}: {e}")
        raise HTTPException(status_code=500, detail=f"분석 실패: {str(e)}")


@router.post("/test", response_model=TestCrawlResponse)
async def test_crawl(request: TestCrawlRequest):
    """
    테스트 크롤링 - 저장 없이 실제 데이터 추출

    /analyze → /test → /create 워크플로우:
    1. /analyze로 필드 자동 감지
    2. /test로 실제 데이터 추출 테스트
    3. /create로 ETL 생성
    """
    crawler = TestCrawlerService()

    try:
        result = await crawler.test_crawl(
            url=request.url,
            fields=request.fields,
            max_records=request.max_records
        )

        if result.error:
            raise HTTPException(status_code=400, detail=result.error)

        return TestCrawlResponse(
            success=result.success,
            records=result.records,
            total_found=result.total_found,
            extracted_count=result.extracted_count,
            extraction_time_ms=result.extraction_time_ms,
            field_stats=result.field_stats,
            warnings=result.warnings
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Test crawl failed for {request.url}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"테스트 크롤링 실패: {str(e)}")


@router.post("", response_model=QuickAddResponse, status_code=201)
async def quick_add_source(
    request: QuickAddRequest,
    background_tasks: BackgroundTasks,
    mongo: MongoService = Depends(get_mongo)
):
    """
    원클릭 소스 등록

    URL만 입력하면:
    1. 자동 분석
    2. 소스 등록
    3. 크롤러 생성 트리거
    4. DAG 생성

    10분 내 완전 자동화!
    """
    discovery = AutoDiscoveryService()

    try:
        # 1. 자동 분석
        logger.info(f"Starting quick add for: {request.url}")
        result = await discovery.discover(
            url=request.url,
            hint=request.hint,
            deep_analysis=True
        )

        # 신뢰도 체크
        if result.confidence_score < 0.3:
            raise HTTPException(
                status_code=400,
                detail=f"분석 신뢰도가 너무 낮습니다 ({result.confidence_score:.0%}). 수동 설정이 필요합니다."
            )

        # 2. 소스 이름 생성
        source_name = request.name
        if not source_name:
            from urllib.parse import urlparse
            parsed = urlparse(request.url)
            domain = parsed.netloc.replace('www.', '').split('.')[0]
            source_name = f"{domain}_{result.page_type.value}_{datetime.now().strftime('%m%d')}"

        # 중복 체크
        existing = mongo.get_source_by_name(source_name)
        if existing:
            source_name = f"{source_name}_{datetime.now().strftime('%H%M')}"

        # 3. 소스 데이터 구성
        fields = [{
            'name': f.name,
            'selector': f.selector,
            'data_type': f.data_type,
            'is_list': f.is_list,
            'extraction_method': f.extraction_method
        } for f in result.recommended_fields]

        source_data = {
            'name': source_name,
            'url': request.url,
            'type': 'html',  # TODO: result.strategy 기반 결정
            'fields': fields,
            'schedule': result.recommended_schedule,
            'status': 'pending',
            'metadata': {
                'page_type': result.page_type.value,
                'strategy': result.strategy.value,
                'requires_js': result.requires_js,
                'has_pagination': result.has_pagination,
                'data_freshness': result.data_freshness,
                'confidence_score': result.confidence_score,
                'auto_discovered': True,
                'hint': request.hint
            }
        }

        # 4. 소스 등록
        source_id = mongo.create_source(source_data)
        logger.info(f"Created source: {source_id}")

        # 5. Airflow DAG 트리거
        dag_id = None
        if request.auto_start:
            airflow = AirflowTrigger()
            trigger_conf = {
                "source_id": source_id,
                **source_data
            }

            trigger_result = await airflow.trigger_dag("source_manager", conf=trigger_conf)

            if trigger_result["success"]:
                dag_id = f"crawler_{source_id.replace('-', '_')}"
                logger.info(f"Triggered source_manager DAG for {source_id}")
            else:
                logger.warning(f"DAG trigger failed: {trigger_result['message']}")

        # 6. 응답 구성
        discovery_response = DiscoveryResponse(
            url=result.url,
            page_type=result.page_type.value,
            strategy=result.strategy.value,
            requires_js=result.requires_js,
            has_pagination=result.has_pagination,
            recommended_schedule=result.recommended_schedule,
            schedule_reason=result.schedule_reason,
            data_freshness=result.data_freshness,
            confidence_score=result.confidence_score,
            fields=[{
                'name': f.name,
                'selector': f.selector,
                'data_type': f.data_type,
                'sample_value': f.sample_value,
                'confidence': f.confidence
            } for f in result.recommended_fields],
            sample_data=result.sample_data,
            warnings=result.warnings
        )

        return QuickAddResponse(
            success=True,
            source_id=source_id,
            dag_id=dag_id,
            message=f"소스 '{source_name}' 등록 완료. 크롤러 코드가 자동 생성됩니다.",
            discovery=discovery_response,
            estimated_first_run=_calculate_first_run(result.recommended_schedule)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Quick add failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"등록 실패: {str(e)}")


@router.post("/create", response_model=QuickAddResponse, status_code=201)
async def create_with_custom_fields(
    request: CreateRequest,
    background_tasks: BackgroundTasks,
    mongo: MongoService = Depends(get_mongo)
):
    """
    커스텀 필드로 ETL 생성

    /analyze에서 받은 필드를 수정한 후 이 엔드포인트로 생성합니다.

    워크플로우:
    1. /analyze로 필드 자동 감지
    2. 필드 수정 (필요시)
    3. /test로 테스트 (선택)
    4. /create로 최종 생성

    instant=True (기본값): 즉시 크롤러 코드 생성 + DAG 파일 작성 (2분 이내)
    instant=False: source_manager DAG 트리거 (기존 방식, 5-10분)
    """
    try:
        # 1. run_test가 true면 먼저 테스트 실행
        test_result = None
        if request.run_test:
            crawler = TestCrawlerService()
            test_result = await crawler.test_crawl(
                url=request.url,
                fields=request.fields,
                max_records=5
            )

            if not test_result.success:
                raise HTTPException(
                    status_code=400,
                    detail=f"테스트 크롤링 실패: {test_result.error or '데이터를 추출하지 못했습니다'}"
                )

        # 2. 소스 이름 생성
        source_name = request.name
        if not source_name:
            from urllib.parse import urlparse
            parsed = urlparse(request.url)
            domain = parsed.netloc.replace('www.', '').split('.')[0]
            source_name = f"{domain}_custom_{datetime.now().strftime('%m%d')}"

        # 중복 체크
        existing = mongo.get_source_by_name(source_name)
        if existing:
            source_name = f"{source_name}_{datetime.now().strftime('%H%M')}"

        # 3. 스케줄 결정
        schedule = request.schedule
        if not schedule:
            schedule = "0 * * * *"  # 기본: 매시간

        # 4. 소스 데이터 구성
        source_data = {
            'name': source_name,
            'url': request.url,
            'type': 'html',
            'fields': request.fields,
            'schedule': schedule,
            'status': 'pending',
            'metadata': {
                'custom_fields': True,
                'instant_mode': request.instant,
                'run_test': request.run_test,
                'test_success': test_result.success if test_result else None,
                'test_records_found': test_result.extracted_count if test_result else None
            }
        }

        # 5. 소스 등록
        source_id = mongo.create_source(source_data)
        logger.info(f"Created custom source: {source_id}")

        dag_id = None
        message = ""

        # 6. Instant 모드: 즉시 크롤러 코드 생성 + DAG 파일 작성
        if request.instant:
            instant_service = InstantETLService()
            etl_result = await instant_service.create_instant_etl(
                source_id=source_id,
                url=request.url,
                fields=request.fields,
                source_name=source_name,
                schedule=schedule,
                metadata=source_data.get('metadata')
            )

            if etl_result.success:
                dag_id = etl_result.dag_id

                # 크롤러 정보 저장
                mongo.db.crawlers.insert_one({
                    '_id': source_id,
                    'source_id': source_id,
                    'code': etl_result.crawler_code,
                    'version': 1,
                    'status': 'active',
                    'dag_id': dag_id,
                    'created_by': 'instant',
                    'created_at': datetime.utcnow()
                })

                # 소스 상태 업데이트
                mongo.update_source_status(source_id, 'active')

                message = f"소스 '{source_name}' 즉시 생성 완료! DAG 파일이 작성되었습니다. ({etl_result.generation_time_ms}ms)"
                logger.info(f"Instant ETL created: {dag_id} in {etl_result.generation_time_ms}ms")
            else:
                message = f"즉시 생성 실패, source_manager DAG로 대체: {etl_result.error}"
                logger.warning(f"Instant ETL failed, falling back: {etl_result.error}")

                # 폴백: source_manager DAG 트리거
                if request.auto_start:
                    airflow = AirflowTrigger()
                    await airflow.trigger_dag("source_manager", conf={"source_id": source_id, **source_data})

        # 7. 기존 방식: source_manager DAG 트리거
        elif request.auto_start:
            airflow = AirflowTrigger()
            trigger_conf = {
                "source_id": source_id,
                **source_data
            }

            trigger_result = await airflow.trigger_dag("source_manager", conf=trigger_conf)

            if trigger_result["success"]:
                dag_id = f"crawler_{source_id.replace('-', '_')}"
                logger.info(f"Triggered source_manager DAG for {source_id}")
            else:
                logger.warning(f"DAG trigger failed: {trigger_result['message']}")

            message = f"소스 '{source_name}' 등록 완료. 크롤러 코드가 자동 생성됩니다. (5-10분 소요)"

        # 8. 응답 구성
        return QuickAddResponse(
            success=True,
            source_id=source_id,
            dag_id=dag_id,
            message=message or f"소스 '{source_name}' 등록 완료.",
            estimated_first_run=_calculate_first_run(schedule)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create with custom fields failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"생성 실패: {str(e)}")


@router.post("/batch", response_model=BatchAddResponse, status_code=201)
async def batch_add_sources(
    request: BatchAddRequest,
    background_tasks: BackgroundTasks,
    mongo: MongoService = Depends(get_mongo)
):
    """
    배치 소스 등록

    여러 URL을 한번에 등록
    """
    batch_discovery = BatchDiscoveryService()
    results = []
    succeeded = 0
    failed = 0

    # 배치 분석
    try:
        discoveries = await batch_discovery.discover_batch(
            urls=request.urls,
            hints=request.hints,
            concurrency=5
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"배치 분석 실패: {str(e)}")

    # 개별 등록
    for discovery_result in discoveries:
        try:
            # 소스 이름 생성
            from urllib.parse import urlparse
            parsed = urlparse(discovery_result.url)
            domain = parsed.netloc.replace('www.', '').split('.')[0]
            source_name = f"{domain}_{discovery_result.page_type.value}_{datetime.now().strftime('%m%d%H%M')}"

            # 필드 구성
            fields = [{
                'name': f.name,
                'selector': f.selector,
                'data_type': f.data_type,
                'is_list': f.is_list,
                'extraction_method': f.extraction_method
            } for f in discovery_result.recommended_fields]

            # 소스 등록
            source_data = {
                'name': source_name,
                'url': discovery_result.url,
                'type': 'html',
                'fields': fields,
                'schedule': discovery_result.recommended_schedule,
                'status': 'pending',
                'metadata': {
                    'page_type': discovery_result.page_type.value,
                    'strategy': discovery_result.strategy.value,
                    'auto_discovered': True,
                    'batch_registered': True
                }
            }

            source_id = mongo.create_source(source_data)

            # DAG 트리거
            if request.auto_start:
                airflow = AirflowTrigger()
                await airflow.trigger_dag("source_manager", conf={
                    "source_id": source_id,
                    **source_data
                })

            results.append(QuickAddResponse(
                success=True,
                source_id=source_id,
                message=f"등록 완료: {source_name}"
            ))
            succeeded += 1

        except Exception as e:
            results.append(QuickAddResponse(
                success=False,
                message=f"실패: {discovery_result.url} - {str(e)}"
            ))
            failed += 1

    return BatchAddResponse(
        success=failed == 0,
        total=len(request.urls),
        succeeded=succeeded,
        failed=failed,
        results=results
    )


@router.get("/templates")
async def get_source_templates():
    """
    소스 템플릿 목록

    자주 사용되는 뉴스/금융 사이트 템플릿
    """
    templates = [
        {
            "id": "naver_news",
            "name": "네이버 뉴스",
            "category": "news",
            "url_pattern": "https://news.naver.com/*",
            "recommended_schedule": "*/30 * * * *",
            "fields": ["title", "date", "summary", "link", "source"]
        },
        {
            "id": "naver_finance",
            "name": "네이버 금융",
            "category": "financial",
            "url_pattern": "https://finance.naver.com/*",
            "recommended_schedule": "*/5 9-16 * * 1-5",
            "fields": ["name", "price", "change", "change_rate", "volume"]
        },
        {
            "id": "krx_disclosure",
            "name": "KRX 공시",
            "category": "announcement",
            "url_pattern": "https://kind.krx.co.kr/*",
            "recommended_schedule": "*/15 8-18 * * 1-5",
            "fields": ["title", "company", "date", "type", "link"]
        },
        {
            "id": "exchange_rate",
            "name": "환율 정보",
            "category": "exchange_rate",
            "url_pattern": "*exchange*|*환율*",
            "recommended_schedule": "0 9,12,15,18 * * 1-5",
            "fields": ["currency", "rate", "change", "buy_rate", "sell_rate"]
        },
        {
            "id": "yonhap_news",
            "name": "연합뉴스",
            "category": "news",
            "url_pattern": "https://www.yna.co.kr/*",
            "recommended_schedule": "*/15 * * * *",
            "fields": ["title", "date", "summary", "link", "category"]
        }
    ]

    return {"templates": templates}


@router.post("/from-template/{template_id}")
async def add_from_template(
    template_id: str,
    url: str = Query(..., description="실제 URL"),
    name: Optional[str] = None,
    mongo: MongoService = Depends(get_mongo)
):
    """
    템플릿 기반 소스 등록
    """
    templates = {
        "naver_news": {
            "type": "html",
            "schedule": "*/30 * * * *",
            "fields": [
                {"name": "title", "selector": ".news_tit", "data_type": "string"},
                {"name": "date", "selector": ".info_group span", "data_type": "date"},
                {"name": "summary", "selector": ".news_dsc", "data_type": "string"},
                {"name": "link", "selector": ".news_tit", "data_type": "url", "extraction_method": "href"},
                {"name": "source", "selector": ".info_group a", "data_type": "string"}
            ]
        },
        "naver_finance": {
            "type": "html",
            "schedule": "*/5 9-16 * * 1-5",
            "fields": [
                {"name": "name", "selector": "td:nth-child(1)", "data_type": "string"},
                {"name": "price", "selector": "td:nth-child(2)", "data_type": "number"},
                {"name": "change", "selector": "td:nth-child(3)", "data_type": "number"},
                {"name": "change_rate", "selector": "td:nth-child(4)", "data_type": "number"},
                {"name": "volume", "selector": "td:nth-child(5)", "data_type": "number"}
            ]
        }
    }

    template = templates.get(template_id)
    if not template:
        raise HTTPException(status_code=404, detail=f"템플릿을 찾을 수 없습니다: {template_id}")

    source_name = name or f"{template_id}_{datetime.now().strftime('%m%d%H%M')}"

    source_data = {
        'name': source_name,
        'url': url,
        'type': template['type'],
        'fields': template['fields'],
        'schedule': template['schedule'],
        'status': 'pending',
        'metadata': {
            'template_id': template_id,
            'template_based': True
        }
    }

    source_id = mongo.create_source(source_data)

    # DAG 트리거
    airflow = AirflowTrigger()
    await airflow.trigger_dag("source_manager", conf={
        "source_id": source_id,
        **source_data
    })

    return {
        "success": True,
        "source_id": source_id,
        "message": f"템플릿 '{template_id}' 기반으로 소스 등록 완료"
    }


# ============== Helper Functions ==============

def _calculate_first_run(cron_schedule: str) -> str:
    """다음 실행 시간 계산"""
    try:
        from croniter import croniter
        now = datetime.now()
        cron = croniter(cron_schedule, now)
        next_run = cron.get_next(datetime)
        return next_run.isoformat()
    except Exception:
        return "스케줄에 따라 실행됨"
