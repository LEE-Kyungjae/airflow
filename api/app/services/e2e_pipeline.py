"""
E2E Automation Pipeline - 완전 자동화 크롤러 파이프라인

URL 하나만 입력하면 모든 것을 자동으로:
1. Auto-Discovery: 페이지 분석, 필드 추출, 전략 결정
2. Code Generation: AI 다단계 코드 생성 + 테스트 루프
3. DAG Deploy: Airflow DAG 자동 배포
4. Monitoring Setup: 모니터링 + 알림 설정
5. Health Check: 첫 실행 결과 검증
"""

import os
import re
import uuid
import json
import logging
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class PipelineStage(str, Enum):
    """파이프라인 단계"""
    INIT = "init"
    DISCOVERY = "discovery"
    CODE_GENERATION = "code_generation"
    CODE_TESTING = "code_testing"
    DAG_DEPLOY = "dag_deploy"
    MONITORING_SETUP = "monitoring_setup"
    HEALTH_CHECK = "health_check"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PipelineProgress:
    """파이프라인 진행 상태"""
    pipeline_id: str
    url: str
    current_stage: PipelineStage
    stages_completed: List[str] = field(default_factory=list)
    stage_results: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    source_id: Optional[str] = None
    dag_id: Optional[str] = None
    crawler_code: Optional[str] = None


class E2EPipelineService:
    """완전 자동화 크롤러 파이프라인"""

    def __init__(self, mongo_service=None):
        self.mongo = mongo_service

    async def execute_pipeline(
        self,
        url: str,
        name: Optional[str] = None,
        hint: Optional[str] = None,
        auto_start: bool = True,
    ) -> PipelineProgress:
        """
        E2E 파이프라인 실행

        Args:
            url: 크롤링 대상 URL
            name: 소스 이름 (없으면 자동 생성)
            hint: 힌트 (뉴스, 금융 등)
            auto_start: DAG 즉시 시작 여부
        """
        pipeline_id = f"pipe_{uuid.uuid4().hex[:12]}"
        progress = PipelineProgress(
            pipeline_id=pipeline_id,
            url=url,
            current_stage=PipelineStage.INIT,
            started_at=datetime.utcnow().isoformat(),
        )

        try:
            # === Stage 1: Auto-Discovery ===
            progress.current_stage = PipelineStage.DISCOVERY
            await self._save_progress(progress)

            discovery_result = await self._run_discovery(url, hint)
            progress.stage_results['discovery'] = {
                'page_type': discovery_result.get('page_type'),
                'strategy': discovery_result.get('strategy'),
                'requires_js': discovery_result.get('requires_js', False),
                'field_count': len(discovery_result.get('fields', [])),
                'recommended_schedule': discovery_result.get('schedule'),
                'confidence': discovery_result.get('confidence', 0),
            }
            progress.stages_completed.append('discovery')

            if not name:
                name = discovery_result.get('recommended_name') or self._generate_name(url)

            # === Stage 2: Code Generation ===
            progress.current_stage = PipelineStage.CODE_GENERATION
            await self._save_progress(progress)

            source_id = self._generate_source_id(name)
            fields = discovery_result.get('fields', [])
            schedule = discovery_result.get('schedule', '0 9 * * *')
            html_content = discovery_result.get('html_content')
            metadata = {
                'page_type': discovery_result.get('page_type', 'generic'),
                'requires_js': discovery_result.get('requires_js', False),
                'pagination': discovery_result.get('pagination_type'),
            }

            code_result = await self._run_code_generation(
                source_id=source_id,
                url=url,
                fields=fields,
                metadata=metadata,
                html_content=html_content,
            )
            progress.stage_results['code_generation'] = {
                'method': code_result.get('method'),
                'steps': code_result.get('steps', []),
            }
            progress.crawler_code = code_result.get('code')
            progress.stages_completed.append('code_generation')

            # === Stage 3: Code Testing ===
            progress.current_stage = PipelineStage.CODE_TESTING
            await self._save_progress(progress)

            test_result = await self._run_code_testing(
                code=progress.crawler_code,
                url=url,
                fields=fields,
            )
            progress.stage_results['code_testing'] = {
                'success': test_result.get('success', False),
                'record_count': test_result.get('record_count', 0),
                'retry_count': test_result.get('retry_count', 0),
            }
            if test_result.get('final_code'):
                progress.crawler_code = test_result['final_code']
            progress.stages_completed.append('code_testing')

            # === Stage 4: DAG Deploy ===
            progress.current_stage = PipelineStage.DAG_DEPLOY
            await self._save_progress(progress)

            deploy_result = await self._run_dag_deploy(
                source_id=source_id,
                source_name=name,
                url=url,
                schedule=schedule,
                crawler_code=progress.crawler_code,
                fields=fields,
                metadata={**metadata, 'pipeline_id': pipeline_id},
            )
            progress.stage_results['dag_deploy'] = {
                'dag_id': deploy_result.get('dag_id'),
                'dag_path': deploy_result.get('dag_path'),
            }
            progress.source_id = source_id
            progress.dag_id = deploy_result.get('dag_id')
            progress.stages_completed.append('dag_deploy')

            # === Stage 5: Monitoring Setup ===
            progress.current_stage = PipelineStage.MONITORING_SETUP
            await self._save_progress(progress)

            monitoring_result = await self._setup_monitoring(
                source_id=source_id,
                source_name=name,
                url=url,
                schedule=schedule,
            )
            progress.stage_results['monitoring_setup'] = monitoring_result
            progress.stages_completed.append('monitoring_setup')

            # === Stage 6: Health Check ===
            progress.current_stage = PipelineStage.HEALTH_CHECK
            await self._save_progress(progress)

            health_result = await self._run_health_check(
                source_id=source_id,
                dag_id=progress.dag_id,
            )
            progress.stage_results['health_check'] = health_result
            progress.stages_completed.append('health_check')

            # === Complete ===
            progress.current_stage = PipelineStage.COMPLETED
            progress.completed_at = datetime.utcnow().isoformat()
            await self._save_progress(progress)

            logger.info(f"[{pipeline_id}] E2E Pipeline completed successfully")

        except Exception as e:
            logger.error(f"[{pipeline_id}] Pipeline failed at {progress.current_stage}: {e}", exc_info=True)
            progress.current_stage = PipelineStage.FAILED
            progress.error = str(e)
            progress.completed_at = datetime.utcnow().isoformat()
            await self._save_progress(progress)

        return progress

    async def _run_discovery(self, url: str, hint: Optional[str]) -> Dict[str, Any]:
        """Auto-Discovery 실행"""
        from app.services.auto_discovery import AutoDiscoveryService

        service = AutoDiscoveryService()
        result = await service.discover(url, hint=hint, deep_analysis=True)

        fields = []
        for f in result.recommended_fields:
            fields.append({
                'name': f.name,
                'selector': f.selector,
                'data_type': f.data_type,
                'sample_value': f.sample_value,
                'confidence': f.confidence,
                'is_list': f.is_list,
                'extraction_method': f.extraction_method,
            })

        return {
            'page_type': result.page_type.value,
            'strategy': result.strategy.value,
            'requires_js': result.requires_js,
            'has_pagination': result.has_pagination,
            'pagination_type': result.pagination_type,
            'fields': fields,
            'schedule': result.recommended_schedule,
            'schedule_reason': result.schedule_reason,
            'data_freshness': result.data_freshness,
            'confidence': result.confidence_score,
            'warnings': result.warnings,
            'recommended_name': None,
        }

    async def _run_code_generation(
        self,
        source_id: str,
        url: str,
        fields: List[Dict],
        metadata: Dict,
        html_content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """AI 코드 생성"""
        from app.services.instant_etl import InstantETLService

        service = InstantETLService()

        if service.gpt_service and html_content:
            gen_result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: service.gpt_service.generate_crawler_code_advanced(
                    source_id=source_id,
                    url=url,
                    data_type=metadata.get('page_type', 'html'),
                    fields=fields,
                    html_content=html_content,
                    page_type=metadata.get('page_type', ''),
                    pagination_info=metadata.get('pagination'),
                )
            )
            return {
                'code': gen_result['code'],
                'method': 'advanced_ai',
                'steps': gen_result.get('generation_steps', []),
            }
        elif service.gpt_service:
            code = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: service.gpt_service.generate_crawler_code(
                    source_id=source_id,
                    url=url,
                    data_type='html',
                    fields=fields,
                )
            )
            return {'code': code, 'method': 'basic_ai', 'steps': ['basic_generation']}
        else:
            code = service._generate_template_code(source_id, url, fields)
            return {'code': code, 'method': 'template', 'steps': ['template_generation']}

    async def _run_code_testing(
        self,
        code: str,
        url: str,
        fields: List[Dict],
    ) -> Dict[str, Any]:
        """코드 테스트 + 실패 시 재생성 루프"""
        from app.services.instant_etl import InstantETLService

        service = InstantETLService()
        test_result = await service._test_crawler_code(code, url, fields)

        retry_count = 0
        current_code = code

        while not test_result.success and retry_count < 3:
            retry_count += 1

            if service.gpt_service and test_result.error_message:
                try:
                    err_msg = test_result.error_message
                    trace = test_result.stack_trace or ""
                    fixed = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: service.gpt_service.fix_crawler_code(
                            current_code=current_code,
                            error_code="E010",
                            error_message=err_msg,
                            stack_trace=trace,
                        )
                    )
                    if fixed:
                        current_code = fixed
                    test_result = await service._test_crawler_code(current_code, url, fields)
                except Exception:
                    break
            else:
                break

        return {
            'success': test_result.success,
            'syntax_valid': test_result.syntax_valid,
            'record_count': test_result.record_count,
            'retry_count': retry_count,
            'error': test_result.error_message,
            'final_code': current_code if test_result.success else code,
        }

    async def _run_dag_deploy(
        self,
        source_id: str,
        source_name: str,
        url: str,
        schedule: str,
        crawler_code: str,
        fields: List[Dict],
        metadata: Dict,
    ) -> Dict[str, Any]:
        """DAG 배포"""
        from app.services.instant_etl import InstantETLService

        service = InstantETLService()

        dag_id = f"crawler_{source_id.replace('-', '_')}"
        dag_content = service._generate_dag_content(
            dag_id=dag_id,
            source_id=source_id,
            source_name=source_name,
            url=url,
            schedule=schedule,
            crawler_code=crawler_code,
            fields=fields,
            metadata=metadata,
        )

        dag_path = await service._write_dag_file(dag_id, dag_content)

        # Save source to MongoDB
        if self.mongo:
            self.mongo.db.sources.update_one(
                {'source_id': source_id},
                {'$set': {
                    'source_id': source_id,
                    'name': source_name,
                    'url': url,
                    'schedule': schedule,
                    'fields': fields,
                    'dag_id': dag_id,
                    'dag_path': dag_path,
                    'status': 'active',
                    'page_type': metadata.get('page_type', 'generic'),
                    'strategy': metadata.get('strategy', 'static_html'),
                    'pipeline_id': metadata.get('pipeline_id'),
                    'created_at': datetime.utcnow(),
                    'created_by': 'e2e_pipeline',
                }},
                upsert=True,
            )

            self.mongo.db.crawlers.update_one(
                {'crawler_id': source_id},
                {'$set': {
                    'crawler_id': source_id,
                    'source_id': source_id,
                    'code': crawler_code,
                    'version': 1,
                    'created_at': datetime.utcnow(),
                    'created_by': 'e2e_pipeline',
                }},
                upsert=True,
            )

        return {'dag_id': dag_id, 'dag_path': dag_path}

    async def _setup_monitoring(
        self,
        source_id: str,
        source_name: str,
        url: str,
        schedule: str,
    ) -> Dict[str, Any]:
        """모니터링 설정"""
        if not self.mongo:
            return {'configured': False, 'reason': 'No MongoDB connection'}

        monitor_config = {
            'source_id': source_id,
            'source_name': source_name,
            'url': url,
            'checks': {
                'freshness': {'enabled': True, 'max_delay_minutes': 120},
                'record_count': {'enabled': True, 'min_records': 1, 'anomaly_threshold': 0.5},
                'error_rate': {'enabled': True, 'max_consecutive_failures': 3},
            },
            'alerts': {'channels': ['system']},
            'created_at': datetime.utcnow(),
        }

        self.mongo.db.monitoring_configs.update_one(
            {'source_id': source_id},
            {'$set': monitor_config},
            upsert=True,
        )

        return {'configured': True, 'checks': list(monitor_config['checks'].keys())}

    async def _run_health_check(
        self,
        source_id: str,
        dag_id: str,
    ) -> Dict[str, Any]:
        """배포 후 헬스 체크"""
        dag_dir = os.getenv('AIRFLOW_DAG_DIR', '/opt/airflow/dags/dynamic_crawlers')
        dag_path = os.path.join(dag_dir, f"{dag_id}.py")
        dag_exists = os.path.exists(dag_path)

        source_exists = False
        if self.mongo:
            source_doc = self.mongo.db.sources.find_one({'source_id': source_id})
            source_exists = source_doc is not None

        return {
            'dag_file_exists': dag_exists,
            'source_registered': source_exists,
            'healthy': dag_exists or source_exists,
        }

    async def _save_progress(self, progress: PipelineProgress):
        """진행 상태 저장"""
        if not self.mongo:
            return

        data = asdict(progress)
        data['current_stage'] = progress.current_stage.value
        self.mongo.db.pipeline_runs.update_one(
            {'pipeline_id': progress.pipeline_id},
            {'$set': data},
            upsert=True,
        )

    def _generate_source_id(self, name: str) -> str:
        """소스 ID 생성"""
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', name.lower())[:30]
        short_id = uuid.uuid4().hex[:8]
        return f"{safe_name}_{short_id}"

    def _generate_name(self, url: str) -> str:
        """URL에서 소스 이름 자동 생성"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.hostname or 'unknown'
        path = parsed.path.strip('/').replace('/', '_')[:20]
        return f"{domain}_{path}" if path else domain


async def get_pipeline_status(
    pipeline_id: str,
    mongo_service=None,
) -> Optional[PipelineProgress]:
    """파이프라인 상태 조회"""
    if not mongo_service:
        return None

    doc = mongo_service.db.pipeline_runs.find_one({'pipeline_id': pipeline_id})
    if not doc:
        return None

    return PipelineProgress(
        pipeline_id=doc['pipeline_id'],
        url=doc['url'],
        current_stage=PipelineStage(doc['current_stage']),
        stages_completed=doc.get('stages_completed', []),
        stage_results=doc.get('stage_results', {}),
        started_at=doc.get('started_at'),
        completed_at=doc.get('completed_at'),
        error=doc.get('error'),
        source_id=doc.get('source_id'),
        dag_id=doc.get('dag_id'),
        crawler_code=doc.get('crawler_code'),
    )