"""
Real-time Monitoring Router - 실시간 모니터링 API

1. 실시간 파이프라인 상태
2. 에러 분류 및 현황
3. 자가 치유 세션 모니터링
4. 알림 관리
"""

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
import asyncio
import json

from app.services.mongo_service import MongoService
from app.services.alerts import AlertDispatcher, AlertSeverity
from app.core import get_logger

logger = get_logger(__name__)
router = APIRouter()


# ============== Models ==============

class PipelineStatus(BaseModel):
    """파이프라인 상태"""
    source_id: str
    source_name: str
    status: str  # running, success, failed, pending, healing
    last_run: Optional[datetime]
    last_success: Optional[datetime]
    error_count: int
    success_rate: float
    avg_execution_time_ms: int
    next_scheduled: Optional[str]


class ErrorSummary(BaseModel):
    """에러 요약"""
    error_code: str
    error_type: str
    count: int
    auto_recoverable: bool
    unresolved_count: int
    last_occurred: datetime
    affected_sources: List[str]


class HealingSessionStatus(BaseModel):
    """치유 세션 상태"""
    session_id: str
    source_id: str
    source_name: str
    status: str
    error_code: str
    current_attempt: int
    max_attempts: int
    started_at: datetime
    last_activity: datetime
    next_action: Optional[str]


class SystemHealth(BaseModel):
    """시스템 헬스"""
    overall_score: float  # 0-100
    status: str  # healthy, degraded, critical
    active_sources: int
    failed_sources: int
    active_healing_sessions: int
    pending_alerts: int
    components: Dict[str, str]
    last_check: datetime


class AlertConfig(BaseModel):
    """알림 설정"""
    alert_type: str  # slack, discord, email, webhook
    enabled: bool
    webhook_url: Optional[str]
    recipients: Optional[List[str]]
    triggers: List[str]  # error, healing_failed, source_inactive, etc.


class AlertEvent(BaseModel):
    """알림 이벤트"""
    event_id: str
    alert_type: str
    severity: str  # info, warning, error, critical
    title: str
    message: str
    source_id: Optional[str]
    created_at: datetime
    acknowledged: bool


# ============== Dependencies ==============

def get_mongo():
    mongo = MongoService()
    try:
        yield mongo
    finally:
        mongo.close()


# ============== WebSocket Connection Manager ==============

class ConnectionManager:
    """WebSocket 연결 관리"""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass


manager = ConnectionManager()


# ============== Endpoints ==============

@router.get("/status/realtime")
async def get_realtime_status(
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    실시간 파이프라인 상태

    모든 소스의 현재 상태를 반환
    """
    sources = mongo.list_sources(limit=500)

    pipeline_statuses = []
    for source in sources:
        # 최근 실행 결과
        recent_results = mongo.get_crawl_results(
            source_id=source['_id'],
            limit=10
        )

        # 성공률 계산
        if recent_results:
            success_count = sum(1 for r in recent_results if r['status'] == 'success')
            success_rate = success_count / len(recent_results)
            avg_time = sum(r.get('execution_time_ms', 0) for r in recent_results) / len(recent_results)
        else:
            success_rate = 0
            avg_time = 0

        # 활성 치유 세션 확인
        healing_session = mongo.db.healing_sessions.find_one({
            'source_id': source['_id'],
            'status': {'$in': ['diagnosing', 'ai_solving', 'source_check']}
        })

        status = source.get('status', 'unknown')
        if healing_session:
            status = 'healing'

        pipeline_statuses.append({
            'source_id': source['_id'],
            'source_name': source['name'],
            'status': status,
            'last_run': source.get('last_run'),
            'last_success': source.get('last_success'),
            'error_count': source.get('error_count', 0),
            'success_rate': round(success_rate * 100, 1),
            'avg_execution_time_ms': int(avg_time),
            'schedule': source.get('schedule'),
            'page_type': source.get('metadata', {}).get('page_type'),
            'healing_session': healing_session['session_id'] if healing_session else None
        })

    # 요약 통계
    total = len(pipeline_statuses)
    active = sum(1 for p in pipeline_statuses if p['status'] == 'active')
    failed = sum(1 for p in pipeline_statuses if p['status'] in ['error', 'failed'])
    healing = sum(1 for p in pipeline_statuses if p['status'] == 'healing')

    return {
        'timestamp': datetime.utcnow().isoformat(),
        'summary': {
            'total': total,
            'active': active,
            'failed': failed,
            'healing': healing,
            'pending': total - active - failed - healing
        },
        'pipelines': pipeline_statuses
    }


@router.get("/errors/summary")
async def get_error_summary(
    hours: int = Query(24, ge=1, le=168, description="조회 기간 (시간)"),
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    에러 요약

    에러 코드별 현황 및 분류
    """
    since = datetime.utcnow() - timedelta(hours=hours)

    # 에러 코드별 집계
    pipeline = [
        {'$match': {'created_at': {'$gte': since}}},
        {'$group': {
            '_id': '$error_code',
            'count': {'$sum': 1},
            'unresolved': {'$sum': {'$cond': ['$resolved', 0, 1]}},
            'auto_recoverable': {'$first': '$auto_recoverable'},
            'error_type': {'$first': '$error_type'},
            'last_occurred': {'$max': '$created_at'},
            'sources': {'$addToSet': '$source_id'}
        }},
        {'$sort': {'count': -1}}
    ]

    error_stats = list(mongo.db.error_logs.aggregate(pipeline))

    summaries = []
    for stat in error_stats:
        summaries.append({
            'error_code': stat['_id'],
            'error_type': stat.get('error_type', 'unknown'),
            'count': stat['count'],
            'unresolved_count': stat['unresolved'],
            'auto_recoverable': stat.get('auto_recoverable', False),
            'last_occurred': stat['last_occurred'].isoformat() if stat.get('last_occurred') else None,
            'affected_sources_count': len(stat.get('sources', []))
        })

    # 시간대별 에러 트렌드
    trend_pipeline = [
        {'$match': {'created_at': {'$gte': since}}},
        {'$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d %H:00',
                    'date': '$created_at'
                }
            },
            'count': {'$sum': 1}
        }},
        {'$sort': {'_id': 1}}
    ]

    trend = list(mongo.db.error_logs.aggregate(trend_pipeline))

    return {
        'period_hours': hours,
        'total_errors': sum(s['count'] for s in summaries),
        'unresolved_count': sum(s['unresolved_count'] for s in summaries),
        'error_summaries': summaries,
        'hourly_trend': [{'hour': t['_id'], 'count': t['count']} for t in trend]
    }


@router.get("/healing/sessions")
async def get_healing_sessions(
    status: Optional[str] = Query(None, description="상태 필터"),
    limit: int = Query(50, ge=1, le=200),
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    자가 치유 세션 목록
    """
    query = {}
    if status:
        query['status'] = status

    sessions = list(mongo.db.healing_sessions.find(query).sort('created_at', -1).limit(limit))

    result = []
    for session in sessions:
        # 소스 이름 조회
        source = mongo.get_source(session['source_id'])
        source_name = source['name'] if source else 'Unknown'

        result.append({
            'session_id': session['session_id'],
            'source_id': session['source_id'],
            'source_name': source_name,
            'status': session['status'],
            'error_code': session.get('error_code'),
            'current_attempt': session.get('current_attempt', 0),
            'max_attempts': session.get('max_attempts', 5),
            'diagnosis': session.get('diagnosis', {}).get('category'),
            'matched_case': session.get('matched_case'),
            'admin_notified': session.get('admin_notified', False),
            'started_at': session['created_at'].isoformat(),
            'last_activity': session.get('updated_at', session['created_at']).isoformat()
        })

    # 상태별 카운트
    status_counts = {}
    for s in ['pending', 'diagnosing', 'source_check', 'ai_solving', 'waiting_admin', 'resolved', 'failed']:
        status_counts[s] = mongo.db.healing_sessions.count_documents({'status': s})

    return {
        'total': len(result),
        'status_counts': status_counts,
        'sessions': result
    }


@router.post("/healing/{session_id}/admin-approve")
async def admin_approve_healing(
    session_id: str,
    additional_attempts: int = Query(3, ge=1, le=10),
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    관리자 승인 - 추가 치유 시도 허용
    """
    session = mongo.db.healing_sessions.find_one({'session_id': session_id})
    if not session:
        raise HTTPException(status_code=404, detail="세션을 찾을 수 없습니다")

    if session['status'] != 'waiting_admin':
        raise HTTPException(status_code=400, detail=f"승인 가능한 상태가 아닙니다: {session['status']}")

    # 추가 시도 허용
    mongo.db.healing_sessions.update_one(
        {'session_id': session_id},
        {
            '$set': {
                'status': 'ai_solving',
                'admin_approved': True,
                'admin_approved_at': datetime.utcnow()
            },
            '$inc': {'max_attempts': additional_attempts}
        }
    )

    return {
        'success': True,
        'session_id': session_id,
        'message': f'{additional_attempts}회 추가 시도가 승인되었습니다'
    }


@router.get("/health")
async def get_system_health(
    mongo: MongoService = Depends(get_mongo)
) -> SystemHealth:
    """
    시스템 헬스 체크
    """
    now = datetime.utcnow()
    hour_ago = now - timedelta(hours=1)

    # 컴포넌트 상태
    components = {}

    # MongoDB
    try:
        mongo.db.command('ping')
        components['mongodb'] = 'healthy'
    except Exception:
        components['mongodb'] = 'unhealthy'

    # Airflow (간접 체크)
    recent_runs = mongo.db.crawl_results.count_documents({
        'executed_at': {'$gte': hour_ago}
    })
    components['airflow'] = 'healthy' if recent_runs > 0 else 'degraded'

    # 소스 상태
    total_sources = mongo.db.sources.count_documents({})
    active_sources = mongo.db.sources.count_documents({'status': 'active'})
    failed_sources = mongo.db.sources.count_documents({'status': 'error'})

    # 치유 세션
    active_healing = mongo.db.healing_sessions.count_documents({
        'status': {'$in': ['diagnosing', 'ai_solving', 'source_check']}
    })

    # 미처리 알림
    pending_alerts = mongo.db.healing_sessions.count_documents({
        'status': 'waiting_admin',
        'admin_notified': True
    })

    # 헬스 점수 계산
    score = 100
    if failed_sources > 0:
        score -= min(30, failed_sources * 5)
    if active_healing > 5:
        score -= 10
    if pending_alerts > 3:
        score -= 15
    if components.get('mongodb') != 'healthy':
        score -= 30
    if components.get('airflow') != 'healthy':
        score -= 20

    score = max(0, score)

    # 상태 결정
    if score >= 80:
        status = 'healthy'
    elif score >= 50:
        status = 'degraded'
    else:
        status = 'critical'

    return SystemHealth(
        overall_score=score,
        status=status,
        active_sources=active_sources,
        failed_sources=failed_sources,
        active_healing_sessions=active_healing,
        pending_alerts=pending_alerts,
        components=components,
        last_check=now
    )


@router.get("/wellknown-cases")
async def get_wellknown_cases(
    limit: int = Query(50, ge=1, le=200),
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    Wellknown Case 목록
    """
    cases = list(mongo.db.wellknown_cases.find().sort('success_count', -1).limit(limit))

    result = []
    for case in cases:
        total = case.get('success_count', 0) + case.get('failure_count', 0)
        success_rate = case['success_count'] / total if total > 0 else 0

        result.append({
            'case_id': str(case['_id']),
            'error_pattern': case['error_pattern'],
            'error_category': case['error_category'],
            'solution_description': case.get('solution_description', ''),
            'success_count': case['success_count'],
            'failure_count': case.get('failure_count', 0),
            'success_rate': round(success_rate * 100, 1),
            'last_used': case.get('last_used').isoformat() if case.get('last_used') else None,
            'created_by': case.get('created_by', 'unknown')
        })

    return {
        'total': len(result),
        'cases': result
    }


@router.websocket("/ws/live")
async def websocket_live_updates(websocket: WebSocket):
    """
    실시간 업데이트 WebSocket

    이벤트:
    - pipeline_status: 파이프라인 상태 변경
    - error_occurred: 에러 발생
    - healing_update: 치유 세션 업데이트
    - alert: 알림
    """
    await manager.connect(websocket)

    try:
        while True:
            # 클라이언트로부터 메시지 수신 (ping/pong 또는 구독 설정)
            data = await websocket.receive_text()

            try:
                message = json.loads(data)
                if message.get('type') == 'ping':
                    await websocket.send_json({'type': 'pong', 'timestamp': datetime.utcnow().isoformat()})
            except json.JSONDecodeError:
                pass

    except WebSocketDisconnect:
        manager.disconnect(websocket)


@router.post("/alerts/webhook-test")
async def test_webhook(
    webhook_url: str,
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    웹훅 테스트
    """
    import httpx

    test_payload = {
        'type': 'test',
        'title': 'ETL 파이프라인 알림 테스트',
        'message': '웹훅 연결이 정상적으로 설정되었습니다.',
        'timestamp': datetime.utcnow().isoformat()
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                webhook_url,
                json=test_payload,
                timeout=10.0
            )

            return {
                'success': response.status_code < 400,
                'status_code': response.status_code,
                'message': '웹훅 테스트 완료'
            }

    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': '웹훅 연결 실패'
        }


class TestAlertRequest(BaseModel):
    """테스트 알림 요청"""
    severity: str = Field("info", description="알림 심각도 (info, warning, error, critical)")
    title: str = Field("테스트 알림", description="알림 제목")
    message: str = Field("이것은 테스트 알림입니다.", description="알림 메시지")
    source_id: Optional[str] = Field(None, description="소스 ID (선택)")


@router.post("/alerts/test")
async def send_test_alert(
    request: TestAlertRequest,
    mongo: MongoService = Depends(get_mongo)
) -> Dict[str, Any]:
    """
    테스트 알림 발송

    설정된 모든 알림 채널(Email, Slack, Discord, Webhook)로 테스트 알림을 발송합니다.
    """
    try:
        # Map string severity to enum
        severity_map = {
            "info": AlertSeverity.INFO,
            "warning": AlertSeverity.WARNING,
            "error": AlertSeverity.ERROR,
            "critical": AlertSeverity.CRITICAL,
        }
        severity = severity_map.get(request.severity.lower(), AlertSeverity.INFO)

        # Create dispatcher with mongo for persistence
        dispatcher = AlertDispatcher(mongo_service=mongo)

        # Send test alert (skip throttle for tests)
        result = await dispatcher.send_alert(
            title=request.title,
            message=request.message,
            severity=severity,
            source_id=request.source_id,
            metadata={"test": True, "requested_at": datetime.utcnow().isoformat()},
            skip_throttle=True,
        )

        logger.info(
            "Test alert sent",
            severity=request.severity,
            result=result,
        )

        return {
            "success": result.get("sent", False),
            "channels": result.get("channels", {}),
            "alert_id": result.get("alert_id"),
            "message": "테스트 알림이 발송되었습니다." if result.get("sent") else "알림 발송에 실패했습니다. 환경 변수를 확인하세요."
        }

    except Exception as e:
        logger.error("Test alert failed", error=str(e))
        return {
            "success": False,
            "error": str(e),
            "message": "알림 발송 중 오류가 발생했습니다."
        }


# ============== Utility Functions ==============

async def broadcast_event(event_type: str, data: dict):
    """이벤트 브로드캐스트"""
    await manager.broadcast({
        'type': event_type,
        'data': data,
        'timestamp': datetime.utcnow().isoformat()
    })
