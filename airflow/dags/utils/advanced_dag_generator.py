"""
Advanced DAG Generator - 고급 크롤러 DAG 생성기

자가 치유 + ETL 파이프라인 통합 DAG 템플릿 생성:
1. 크롤링 실행
2. 데이터 변환 (Transform)
3. 데이터 적재 (Load)
4. 품질 검증
5. 에러 발생 시 자가 치유 프로세스
6. 이력 관리
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, List, Optional


def generate_advanced_dag(
    dag_id: str,
    source_id: str,
    crawler_id: str,
    source_name: str,
    url: str,
    schedule: str,
    crawler_code: str,
    data_type: str,
    fields: List[Dict],
    metadata: Optional[Dict] = None
) -> str:
    """
    고급 크롤러 DAG 생성

    Features:
    - ETL 파이프라인 통합
    - 자가 치유 시스템
    - 데이터 품질 검증
    - 이력 관리
    - 실시간 모니터링 이벤트
    """

    metadata = metadata or {}
    page_type = metadata.get('page_type', 'generic')
    data_freshness = metadata.get('data_freshness', 'hourly')

    # 데이터 카테고리 매핑
    category_mapping = {
        'news_list': 'NEWS_ARTICLE',
        'news_article': 'NEWS_ARTICLE',
        'financial_data': 'FINANCIAL_DATA',
        'stock_price': 'STOCK_PRICE',
        'exchange_rate': 'EXCHANGE_RATE',
        'market_index': 'MARKET_INDEX',
        'data_table': 'GENERIC',
        'announcement': 'ANNOUNCEMENT',
    }
    data_category = category_mapping.get(page_type, 'GENERIC')

    # 코드 이스케이프
    escaped_code = crawler_code.replace('\\', '\\\\').replace("'''", "\\'\\'\\'")
    fields_json = json.dumps(fields, ensure_ascii=False)
    metadata_json = json.dumps(metadata, ensure_ascii=False)

    dag_template = f'''"""
Advanced Auto-generated Crawler DAG for {source_name}

Source ID: {source_id}
Crawler ID: {crawler_id}
Page Type: {page_type}
Data Category: {data_category}
Generated at: {datetime.utcnow().isoformat()}

Features:
- ETL Pipeline Integration
- Self-Healing System
- Data Quality Validation
- History Management
"""

import sys
import logging
import traceback
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, '/opt/airflow/dags')
from utils.mongo_service import MongoService
from utils.gpt_service import GPTService
from utils.error_handler import ErrorHandler, ErrorCode
from utils.self_healing import SelfHealingEngine, HealingOrchestrator, HealingStatus
from utils.etl_pipeline import ETLPipeline, DataCategory, TransformConfig, LoadConfig

logger = logging.getLogger(__name__)

# ============== Configuration ==============

SOURCE_ID = "{source_id}"
CRAWLER_ID = "{crawler_id}"
SOURCE_NAME = "{source_name}"
URL = "{url}"
DATA_TYPE = "{data_type}"
PAGE_TYPE = "{page_type}"
DATA_CATEGORY = DataCategory.{data_category}
FIELDS = {fields_json}
METADATA = {metadata_json}

CRAWLER_CODE = \'\'\'
{escaped_code}
\'\'\'

# ============== DAG Configuration ==============

default_args = {{
    'owner': 'crawler-system',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,  # 자가 치유 시스템이 재시도 관리
    'retry_delay': timedelta(minutes=1),
}}


# ============== Task Functions ==============

def extract_data(**context) -> Dict[str, Any]:
    """
    Step 1: Extract - 크롤러 코드 실행
    """
    run_id = context['run_id']
    logger.info(f"[{{run_id}}] Starting extraction for {{SOURCE_NAME}}")

    # 크롤러 코드 실행
    local_vars = {{}}
    try:
        exec(CRAWLER_CODE, {{}}, local_vars)
    except Exception as e:
        logger.error(f"Failed to load crawler code: {{e}}")
        raise

    # crawl_ 함수 찾기
    crawl_func = None
    for name, obj in local_vars.items():
        if name.startswith('crawl_') and callable(obj):
            crawl_func = obj
            break

    if not crawl_func:
        raise ValueError("No crawl function found in code")

    # 실행
    start_time = datetime.utcnow()
    html_snapshot = ""

    try:
        result = crawl_func()
        execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

        # 결과 정규화
        if isinstance(result, dict):
            data = [result]
        elif isinstance(result, list):
            data = result
        else:
            data = []

        logger.info(f"[{{run_id}}] Extracted {{len(data)}} records in {{execution_time}}ms")

        return {{
            'success': True,
            'data': data,
            'record_count': len(data),
            'execution_time_ms': execution_time,
            'extracted_at': datetime.utcnow().isoformat()
        }}

    except Exception as e:
        execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        stack_trace = traceback.format_exc()

        logger.error(f"[{{run_id}}] Extraction failed: {{e}}")

        return {{
            'success': False,
            'data': [],
            'record_count': 0,
            'execution_time_ms': execution_time,
            'error_message': str(e),
            'stack_trace': stack_trace,
            'html_snapshot': html_snapshot
        }}


def check_extraction_result(**context) -> str:
    """
    Branch: 추출 성공 여부 확인
    """
    ti = context['ti']
    result = ti.xcom_pull(task_ids='extract')

    if result.get('success') and result.get('record_count', 0) > 0:
        return 'transform'
    else:
        return 'start_healing'


def transform_data(**context) -> Dict[str, Any]:
    """
    Step 2: Transform - 데이터 변환
    """
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract')
    run_id = context['run_id']

    raw_data = extract_result.get('data', [])

    if not raw_data:
        logger.warning(f"[{{run_id}}] No data to transform")
        return {{'success': True, 'data': [], 'record_count': 0}}

    logger.info(f"[{{run_id}}] Transforming {{len(raw_data)}} records")

    # ETL 파이프라인의 Transform 사용
    from utils.etl_pipeline import DataTransformer, TransformConfig

    config = TransformConfig(
        category=DATA_CATEGORY,
        required_fields=[f['name'] for f in FIELDS[:2]],  # 첫 2개 필드 필수
        dedup_fields=['content_hash'] if DATA_CATEGORY.value.startswith('news') else [],
    )

    transformer = DataTransformer(config)
    transformed = transformer.transform(raw_data)

    # 품질 통계
    quality_scores = [r.get('_quality_score', 0) for r in transformed]
    avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0

    logger.info(f"[{{run_id}}] Transformed {{len(transformed)}} records (avg quality: {{avg_quality:.2f}})")

    return {{
        'success': True,
        'data': transformed,
        'record_count': len(transformed),
        'original_count': len(raw_data),
        'dropped_count': len(raw_data) - len(transformed),
        'avg_quality_score': round(avg_quality, 3)
    }}


def load_data(**context) -> Dict[str, Any]:
    """
    Step 3: Load - 데이터 적재
    """
    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='transform')
    run_id = context['run_id']

    data = transform_result.get('data', [])

    if not data:
        logger.warning(f"[{{run_id}}] No data to load")
        return {{'success': True, 'loaded': 0}}

    logger.info(f"[{{run_id}}] Loading {{len(data)}} records")

    mongo = MongoService()

    try:
        # ETL 파이프라인의 Load 사용
        from utils.etl_pipeline import DataLoader, LoadConfig

        # 카테고리별 컬렉션 설정
        collection_map = {{
            'NEWS_ARTICLE': 'news_articles',
            'FINANCIAL_DATA': 'financial_data',
            'STOCK_PRICE': 'stock_prices',
            'EXCHANGE_RATE': 'exchange_rates',
            'MARKET_INDEX': 'market_indices',
            'ANNOUNCEMENT': 'announcements',
            'GENERIC': 'crawl_data'
        }}

        collection_name = collection_map.get(DATA_CATEGORY.value, 'crawl_data')

        config = LoadConfig(
            collection_name=collection_name,
            create_index=True,
            index_fields=['_data_date', '_source_id'],
            upsert=True,
            upsert_key=['content_hash'] if 'content_hash' in data[0] else ['_source_id', '_data_date', '_order_index']
        )

        loader = DataLoader(mongo, config)

        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            load_result = loop.run_until_complete(loader.load(data, SOURCE_ID))
        finally:
            loop.close()

        logger.info(f"[{{run_id}}] Loaded {{load_result['loaded']}} records, {{load_result.get('duplicates', 0)}} duplicates")

        return {{
            'success': True,
            'loaded': load_result['loaded'],
            'duplicates': load_result.get('duplicates', 0),
            'errors': load_result.get('errors', []),
            'collection': collection_name
        }}

    finally:
        mongo.close()


def validate_quality(**context) -> Dict[str, Any]:
    """
    Step 4: Validate - 데이터 품질 검증
    """
    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='transform')
    load_result = ti.xcom_pull(task_ids='load')
    run_id = context['run_id']

    validation_result = {{
        'success': True,
        'issues': [],
        'warnings': []
    }}

    # 변환 품질 체크
    avg_quality = transform_result.get('avg_quality_score', 0)
    if avg_quality < 0.5:
        validation_result['warnings'].append(f"Low average quality score: {{avg_quality}}")

    dropped_count = transform_result.get('dropped_count', 0)
    original_count = transform_result.get('original_count', 0)
    if original_count > 0 and dropped_count / original_count > 0.3:
        validation_result['warnings'].append(f"High drop rate: {{dropped_count}}/{{original_count}}")

    # 로드 에러 체크
    load_errors = load_result.get('errors', [])
    if load_errors:
        validation_result['issues'].extend(load_errors[:5])

    # 데이터 양 체크 (이전 실행과 비교)
    mongo = MongoService()
    try:
        recent_results = list(mongo.db.crawl_results.find(
            {{'source_id': SOURCE_ID, 'status': 'success'}},
            {{'record_count': 1}}
        ).sort('executed_at', -1).limit(5))

        if recent_results:
            avg_count = sum(r.get('record_count', 0) for r in recent_results) / len(recent_results)
            current_count = load_result.get('loaded', 0)

            if avg_count > 0 and current_count < avg_count * 0.5:
                validation_result['warnings'].append(
                    f"Record count significantly lower than average: {{current_count}} vs {{avg_count:.0f}}"
                )
    finally:
        mongo.close()

    logger.info(f"[{{run_id}}] Validation complete: {{len(validation_result['issues'])}} issues, {{len(validation_result['warnings'])}} warnings")

    return validation_result


def save_result(**context) -> str:
    """
    결과 저장
    """
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract')
    transform_result = ti.xcom_pull(task_ids='transform')
    load_result = ti.xcom_pull(task_ids='load')
    validation_result = ti.xcom_pull(task_ids='validate')
    run_id = context['run_id']

    mongo = MongoService()

    try:
        # 성공 여부 판단
        success = (
            extract_result.get('success') and
            transform_result.get('success') and
            load_result.get('success') and
            len(validation_result.get('issues', [])) == 0
        )

        result_data = {{
            'source_id': SOURCE_ID,
            'crawler_id': CRAWLER_ID,
            'run_id': run_id,
            'status': 'success' if success else 'partial',
            'data_category': DATA_CATEGORY.value,
            'record_count': load_result.get('loaded', 0),
            'duplicate_count': load_result.get('duplicates', 0),
            'quality_score': transform_result.get('avg_quality_score', 0),
            'execution_time_ms': extract_result.get('execution_time_ms', 0),
            'validation_warnings': validation_result.get('warnings', []),
            'executed_at': datetime.utcnow(),
            'metadata': {{
                'original_count': transform_result.get('original_count', 0),
                'dropped_count': transform_result.get('dropped_count', 0),
                'collection': load_result.get('collection')
            }}
        }}

        result_id = mongo.save_crawl_result(result_data)

        # 소스 상태 업데이트
        mongo.update_source_status(
            SOURCE_ID,
            'active',
            last_run=datetime.utcnow(),
            last_success=datetime.utcnow() if success else None
        )

        logger.info(f"[{{run_id}}] Result saved: {{result_id}}, status={{result_data['status']}}")

        return result_id

    finally:
        mongo.close()


def start_healing(**context) -> Dict[str, Any]:
    """
    자가 치유 프로세스 시작
    """
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract')
    run_id = context['run_id']

    if extract_result.get('success'):
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("Extraction successful, no healing needed")

    error_message = extract_result.get('error_message', 'Unknown error')
    stack_trace = extract_result.get('stack_trace', '')
    html_snapshot = extract_result.get('html_snapshot', '')

    logger.warning(f"[{{run_id}}] Starting healing process for error: {{error_message[:100]}}")

    mongo = MongoService()

    try:
        # 에러 분류
        try:
            exception = Exception(error_message)
            classification = ErrorHandler.classify_error(exception, html_snapshot)
        except Exception:
            classification = None

        error_code = classification.code.value if classification else 'E010'

        # 에러 로그 저장
        error_data = {{
            'source_id': SOURCE_ID,
            'crawler_id': CRAWLER_ID,
            'run_id': run_id,
            'error_code': error_code,
            'error_type': classification.error_type if classification else 'unknown',
            'message': error_message[:500],
            'stack_trace': stack_trace[:2000],
            'auto_recoverable': classification.auto_recoverable if classification else False,
            'html_snapshot': html_snapshot[:10000] if html_snapshot else None
        }}
        mongo.log_error(error_data)

        # 자가 치유 오케스트레이터 호출
        orchestrator = HealingOrchestrator(mongo)

        # 현재 크롤러 코드 가져오기
        crawler = mongo.get_crawler(CRAWLER_ID)
        current_code = crawler.get('code', '') if crawler else ''

        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            healing_result = loop.run_until_complete(orchestrator.run_healing_pipeline(
                source_id=SOURCE_ID,
                crawler_id=CRAWLER_ID,
                error_code=error_code,
                error_message=error_message,
                stack_trace=stack_trace,
                current_code=current_code,
                html_snapshot=html_snapshot,
                url=URL
            ))
        finally:
            loop.close()

        logger.info(f"[{{run_id}}] Healing result: {{healing_result.get('status')}} - {{healing_result.get('message')}}")

        # 새 코드가 생성되었으면 크롤러 업데이트
        if healing_result.get('new_code'):
            # 기존 코드 이력 저장
            if crawler:
                mongo.save_crawler_history(
                    crawler_id=CRAWLER_ID,
                    version=crawler.get('version', 1),
                    code=current_code,
                    change_reason='auto_fix',
                    change_detail=f"Self-healing for {{error_code}}",
                    changed_by='ai'
                )

            # 새 코드로 업데이트
            mongo.update_crawler_code(
                crawler_id=CRAWLER_ID,
                new_code=healing_result['new_code'],
                created_by='ai'
            )

            logger.info(f"[{{run_id}}] Crawler code updated by self-healing")

        # 소스 상태 업데이트
        mongo.update_source_status(
            SOURCE_ID,
            'error' if not healing_result.get('success') else 'active',
            last_run=datetime.utcnow(),
            increment_error=True
        )

        return healing_result

    finally:
        mongo.close()


def log_completion(**context):
    """
    완료 로깅
    """
    run_id = context['run_id']
    logger.info(f"[{{run_id}}] DAG execution completed for {{SOURCE_NAME}}")


# ============== DAG Definition ==============

with DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description=f'Advanced crawler for {{SOURCE_NAME}} ({{PAGE_TYPE}})',
    schedule_interval='{schedule}',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crawler', 'advanced', '{data_type}', '{page_type}'],
    max_active_runs=1,
    doc_md=__doc__
) as dag:

    # Tasks
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id='check_result',
        python_callable=check_extraction_result,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True
    )

    validate_task = PythonOperator(
        task_id='validate',
        python_callable=validate_quality,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_result',
        python_callable=save_result,
        provide_context=True
    )

    healing_task = PythonOperator(
        task_id='start_healing',
        python_callable=start_healing,
        provide_context=True
    )

    complete_task = PythonOperator(
        task_id='log_completion',
        python_callable=log_completion,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Dependencies
    extract_task >> branch_task

    # Success path
    branch_task >> transform_task >> load_task >> validate_task >> save_task >> complete_task

    # Failure path (healing)
    branch_task >> healing_task >> complete_task
'''

    return dag_template
