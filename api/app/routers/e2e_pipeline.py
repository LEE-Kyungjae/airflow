"""
E2E Pipeline API Router - 완전 자동화 파이프라인 엔드포인트
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter()


class PipelineRequest(BaseModel):
    url: str
    name: Optional[str] = None
    hint: Optional[str] = None
    auto_start: bool = True


class PipelineResponse(BaseModel):
    pipeline_id: str
    status: str
    message: str


class PipelineStatusResponse(BaseModel):
    pipeline_id: str
    url: str
    current_stage: str
    stages_completed: List[str]
    stage_results: dict
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    source_id: Optional[str] = None
    dag_id: Optional[str] = None


@router.post("/pipeline", response_model=PipelineStatusResponse)
async def run_pipeline(request: PipelineRequest):
    """
    E2E 자동화 파이프라인 실행 (동기)

    URL 하나만 입력하면:
    1. 페이지 자동 분석 (Auto-Discovery)
    2. 크롤러 코드 AI 생성 + 테스트
    3. DAG 자동 배포
    4. 모니터링 설정
    5. 헬스 체크
    """
    from app.services.e2e_pipeline import E2EPipelineService

    service = E2EPipelineService()

    result = await service.execute_pipeline(
        url=request.url,
        name=request.name,
        hint=request.hint,
        auto_start=request.auto_start,
    )

    return PipelineStatusResponse(
        pipeline_id=result.pipeline_id,
        url=result.url,
        current_stage=result.current_stage.value,
        stages_completed=result.stages_completed,
        stage_results=result.stage_results,
        started_at=result.started_at,
        completed_at=result.completed_at,
        error=result.error,
        source_id=result.source_id,
        dag_id=result.dag_id,
    )


@router.post("/pipeline/async", response_model=PipelineResponse)
async def start_pipeline_async(
    request: PipelineRequest,
    background_tasks: BackgroundTasks,
):
    """
    E2E 파이프라인 비동기 실행 (백그라운드)

    즉시 pipeline_id를 반환하고, 상태는 GET /pipeline/{id}로 조회
    """
    from app.services.e2e_pipeline import E2EPipelineService
    import uuid

    pipeline_id = f"pipe_{uuid.uuid4().hex[:12]}"

    service = E2EPipelineService()

    async def run():
        await service.execute_pipeline(
            url=request.url,
            name=request.name,
            hint=request.hint,
            auto_start=request.auto_start,
        )

    background_tasks.add_task(run)

    return PipelineResponse(
        pipeline_id=pipeline_id,
        status="started",
        message=f"E2E pipeline started for {request.url}",
    )


@router.get("/pipeline/{pipeline_id}", response_model=PipelineStatusResponse)
async def get_pipeline_status_endpoint(pipeline_id: str):
    """파이프라인 진행 상태 조회"""
    from app.services.e2e_pipeline import get_pipeline_status

    result = await get_pipeline_status(pipeline_id)

    if not result:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    return PipelineStatusResponse(
        pipeline_id=result.pipeline_id,
        url=result.url,
        current_stage=result.current_stage.value,
        stages_completed=result.stages_completed,
        stage_results=result.stage_results,
        started_at=result.started_at,
        completed_at=result.completed_at,
        error=result.error,
        source_id=result.source_id,
        dag_id=result.dag_id,
    )


@router.get("/pipelines", response_model=List[PipelineStatusResponse])
async def list_pipelines(limit: int = 20):
    """최근 파이프라인 실행 목록 조회"""
    from app.services.e2e_pipeline import E2EPipelineService

    service = E2EPipelineService()

    if not service.mongo:
        return []

    docs = list(
        service.mongo.db.pipeline_runs.find()
        .sort('started_at', -1)
        .limit(limit)
    )

    results = []
    for doc in docs:
        results.append(PipelineStatusResponse(
            pipeline_id=doc['pipeline_id'],
            url=doc['url'],
            current_stage=doc['current_stage'],
            stages_completed=doc.get('stages_completed', []),
            stage_results=doc.get('stage_results', {}),
            started_at=doc.get('started_at'),
            completed_at=doc.get('completed_at'),
            error=doc.get('error'),
            source_id=doc.get('source_id'),
            dag_id=doc.get('dag_id'),
        ))

    return results