"""
Instant ETL Service - 즉시 크롤러 코드 생성 및 DAG 배포

source_manager DAG를 건너뛰고 API에서 직접:
1. GPT로 크롤러 코드 생성
2. DAG 파일 즉시 생성
3. MongoDB에 저장

소요 시간: 2분 이내
"""

import os
import re
import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from openai import OpenAI

logger = logging.getLogger(__name__)


@dataclass
class InstantETLResult:
    """Instant ETL 결과"""
    success: bool
    source_id: str
    crawler_id: Optional[str]
    dag_id: Optional[str]
    dag_path: Optional[str]
    crawler_code: Optional[str]
    generation_time_ms: int
    error: Optional[str] = None


class InstantETLService:
    """즉시 ETL 생성 서비스"""

    # 크롤러 코드 생성 프롬프트
    CRAWLER_PROMPT = '''다음 요구사항에 맞는 Python 크롤러 함수를 생성하세요.

URL: {url}
데이터 타입: {data_type}
추출 필드:
{fields}

요구사항:
1. 함수명: crawl_{source_id}()
2. requests와 BeautifulSoup 사용
3. User-Agent 헤더 포함
4. 타임아웃 30초
5. 반환: List[Dict] 형태
6. 에러 발생 시 빈 리스트 반환

코드만 출력하세요. 설명 없이 Python 코드만.'''

    def __init__(self):
        self.api_key = os.getenv('OPENAI_API_KEY')
        self.model = os.getenv('AI_MODEL', 'gpt-4o-mini')
        if self.api_key:
            client_kwargs = {"api_key": self.api_key}
            ai_base_url = os.getenv('AI_BASE_URL')
            if ai_base_url:
                client_kwargs["base_url"] = ai_base_url
            self.client = OpenAI(**client_kwargs)
        else:
            self.client = None
            logger.warning("OpenAI API key not set")

        # DAG 파일 경로 (환경변수 또는 기본값)
        self.dag_dir = os.getenv('AIRFLOW_DAG_DIR', '/opt/airflow/dags/dynamic_crawlers')

    async def create_instant_etl(
        self,
        source_id: str,
        url: str,
        fields: List[Dict],
        source_name: str,
        schedule: str,
        metadata: Optional[Dict] = None
    ) -> InstantETLResult:
        """
        즉시 ETL 생성

        1. GPT로 크롤러 코드 생성
        2. DAG 파일 즉시 작성
        3. 결과 반환
        """
        start_time = datetime.utcnow()

        try:
            # 1. 크롤러 코드 생성
            crawler_code = await self._generate_crawler_code(
                source_id=source_id,
                url=url,
                fields=fields
            )

            if not crawler_code:
                raise ValueError("크롤러 코드 생성 실패")

            # 2. DAG 파일 생성
            dag_id = f"crawler_{source_id.replace('-', '_')}"
            dag_content = self._generate_dag_content(
                dag_id=dag_id,
                source_id=source_id,
                source_name=source_name,
                url=url,
                schedule=schedule,
                crawler_code=crawler_code,
                fields=fields,
                metadata=metadata
            )

            # 3. DAG 파일 저장
            dag_path = await self._write_dag_file(dag_id, dag_content)

            generation_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

            return InstantETLResult(
                success=True,
                source_id=source_id,
                crawler_id=source_id,  # 같은 ID 사용
                dag_id=dag_id,
                dag_path=dag_path,
                crawler_code=crawler_code,
                generation_time_ms=generation_time
            )

        except Exception as e:
            logger.error(f"Instant ETL failed: {e}", exc_info=True)
            generation_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

            return InstantETLResult(
                success=False,
                source_id=source_id,
                crawler_id=None,
                dag_id=None,
                dag_path=None,
                crawler_code=None,
                generation_time_ms=generation_time,
                error=str(e)
            )

    async def _generate_crawler_code(
        self,
        source_id: str,
        url: str,
        fields: List[Dict]
    ) -> Optional[str]:
        """GPT로 크롤러 코드 생성"""
        if not self.client:
            # GPT 없이 템플릿 기반 생성
            return self._generate_template_code(source_id, url, fields)

        fields_str = "\n".join([
            f"  - {f['name']}: selector='{f.get('selector', '')}', type={f.get('data_type', 'string')}"
            for f in fields
        ])

        prompt = self.CRAWLER_PROMPT.format(
            url=url,
            data_type='html',
            fields=fields_str,
            source_id=source_id.replace('-', '_')
        )

        try:
            # 비동기 실행을 위해 executor 사용
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "Python 웹 스크래핑 전문가입니다. 간결하고 효율적인 코드를 작성합니다."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=2000,
                    temperature=0.1
                )
            )

            code = response.choices[0].message.content.strip()
            return self._clean_code(code)

        except Exception as e:
            logger.error(f"GPT code generation failed: {e}")
            # 폴백: 템플릿 기반 생성
            return self._generate_template_code(source_id, url, fields)

    def _generate_template_code(
        self,
        source_id: str,
        url: str,
        fields: List[Dict]
    ) -> str:
        """템플릿 기반 크롤러 코드 생성 (GPT 없이)"""
        safe_id = source_id.replace('-', '_')

        # 필드 추출 코드 생성
        field_extractions = []
        for f in fields:
            name = f['name']
            selector = f.get('selector', '')
            data_type = f.get('data_type', 'string')
            attribute = f.get('attribute', '')

            if attribute == 'href' or f.get('extraction_method') == 'href':
                extraction = f"elem.select_one('{selector}').get('href', '') if elem.select_one('{selector}') else ''"
            elif attribute:
                extraction = f"elem.select_one('{selector}').get('{attribute}', '') if elem.select_one('{selector}') else ''"
            else:
                extraction = f"elem.select_one('{selector}').get_text(strip=True) if elem.select_one('{selector}') else ''"

            field_extractions.append(f"                    '{name}': {extraction},")

        fields_code = "\n".join(field_extractions)

        # 첫 번째 필드의 셀렉터로 아이템 컨테이너 추정
        first_selector = fields[0].get('selector', 'div') if fields else 'div'
        # 셀렉터에서 부모 추정 (예: .news_tit -> 부모 찾기)
        item_selector = self._guess_item_selector(first_selector)

        code = f'''import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any

def crawl_{safe_id}() -> List[Dict[str, Any]]:
    """Auto-generated crawler for {url}"""
    url = "{url}"

    headers = {{
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    }}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'lxml')
        results = []

        # 아이템 컨테이너 찾기
        items = soup.select('{item_selector}')

        if not items:
            # 폴백: 첫 번째 필드 셀렉터로 직접 추출
            items = soup.select('{first_selector}')

        for elem in items[:100]:  # 최대 100개
            try:
                record = {{
{fields_code}
                }}
                # 최소 하나의 값이 있으면 추가
                if any(v for v in record.values()):
                    results.append(record)
            except Exception:
                continue

        return results

    except Exception as e:
        print(f"Crawl error: {{e}}")
        return []


if __name__ == "__main__":
    data = crawl_{safe_id}()
    print(f"Extracted {{len(data)}} records")
    for item in data[:3]:
        print(item)
'''
        return code

    def _guess_item_selector(self, field_selector: str) -> str:
        """필드 셀렉터에서 아이템 컨테이너 추정"""
        # 일반적인 리스트 아이템 패턴
        common_parents = ['li', 'tr', 'article', 'div.item', 'div.list-item', 'div.news-item']

        # 셀렉터 분석
        if 'table' in field_selector.lower() or 'td' in field_selector.lower():
            return 'tr'
        elif 'li' in field_selector.lower():
            return 'li'
        elif 'article' in field_selector.lower():
            return 'article'

        # 기본값: 셀렉터의 부모 추정
        # .news_tit -> .news_tit의 부모를 찾기 어려우므로 그냥 사용
        return field_selector

    def _clean_code(self, code: str) -> str:
        """마크다운 코드 블록 제거"""
        if '```python' in code:
            code = code.split('```python')[1].split('```')[0]
        elif '```' in code:
            code = code.split('```')[1].split('```')[0]
        return code.strip()

    def _generate_dag_content(
        self,
        dag_id: str,
        source_id: str,
        source_name: str,
        url: str,
        schedule: str,
        crawler_code: str,
        fields: List[Dict],
        metadata: Optional[Dict] = None
    ) -> str:
        """DAG 파일 내용 생성"""
        metadata = metadata or {}
        page_type = metadata.get('page_type', 'generic')

        # 데이터 카테고리 매핑
        category_mapping = {
            'news_list': 'NEWS_ARTICLE',
            'news_article': 'NEWS_ARTICLE',
            'financial_data': 'FINANCIAL_DATA',
            'stock_price': 'STOCK_PRICE',
            'exchange_rate': 'EXCHANGE_RATE',
            'market_index': 'MARKET_INDEX',
            'data_table': 'GENERIC',
        }
        data_category = category_mapping.get(page_type, 'GENERIC')

        # 코드 이스케이프
        escaped_code = crawler_code.replace('\\', '\\\\').replace("'''", "\\'\\'\\'")
        fields_json = json.dumps(fields, ensure_ascii=False)

        dag_content = f'''"""
Instant Auto-generated Crawler DAG for {source_name}

Source ID: {source_id}
URL: {url}
Generated at: {datetime.utcnow().isoformat()}

This DAG was generated instantly via /api/quick-add/create endpoint.
"""

import sys
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, '/opt/airflow/dags')

logger = logging.getLogger(__name__)

# ============== Configuration ==============
SOURCE_ID = "{source_id}"
SOURCE_NAME = "{source_name}"
URL = "{url}"
DATA_CATEGORY = "{data_category}"
FIELDS = {fields_json}

# ============== Embedded Crawler Code ==============
CRAWLER_CODE = \'\'\'
{escaped_code}
\'\'\'

# ============== DAG Definition ==============
default_args = {{
    'owner': 'instant-etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}}

with DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='Instant crawler for {source_name}',
    schedule_interval='{schedule}',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['instant', 'crawler', '{page_type}'],
) as dag:

    def extract_data(**context):
        """크롤러 실행"""
        try:
            # 크롤러 코드 실행
            local_vars = {{}}
            exec(CRAWLER_CODE, {{}}, local_vars)

            # crawl_ 함수 찾기
            crawl_func = None
            for name, obj in local_vars.items():
                if name.startswith('crawl_') and callable(obj):
                    crawl_func = obj
                    break

            if not crawl_func:
                raise ValueError("crawl_ 함수를 찾을 수 없습니다")

            # 크롤링 실행
            data = crawl_func()
            logger.info(f"Extracted {{len(data)}} records from {{URL}}")

            return {{
                'success': len(data) > 0,
                'data': data,
                'count': len(data),
                'error': None
            }}

        except Exception as e:
            logger.error(f"Extract failed: {{e}}")
            return {{
                'success': False,
                'data': [],
                'count': 0,
                'error': str(e)
            }}

    def check_result(**context):
        """결과 확인 및 분기"""
        ti = context['ti']
        result = ti.xcom_pull(task_ids='extract')

        if result and result.get('success') and result.get('count', 0) > 0:
            return 'save_result'
        return 'log_failure'

    def save_result(**context):
        """결과 저장"""
        try:
            from utils.mongo_service import MongoService

            ti = context['ti']
            result = ti.xcom_pull(task_ids='extract')

            if not result or not result.get('data'):
                logger.warning("No data to save")
                return

            mongo = MongoService()

            # 결과 저장
            mongo.save_crawl_result({{
                'source_id': SOURCE_ID,
                'run_id': context['run_id'],
                'status': 'success',
                'data': result['data'][:10],  # 샘플만 저장
                'record_count': result['count'],
                'executed_at': datetime.utcnow()
            }})

            # 소스 상태 업데이트
            mongo.update_source_status(SOURCE_ID, 'active', last_success=datetime.utcnow())

            logger.info(f"Saved {{result['count']}} records for {{SOURCE_ID}}")

        except Exception as e:
            logger.error(f"Save failed: {{e}}")

    def log_failure(**context):
        """실패 로깅"""
        ti = context['ti']
        result = ti.xcom_pull(task_ids='extract')
        error = result.get('error', 'Unknown error') if result else 'No result'
        logger.error(f"Crawl failed for {{SOURCE_ID}}: {{error}}")

    # Tasks
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    check = BranchPythonOperator(
        task_id='check_result',
        python_callable=check_result,
    )

    save = PythonOperator(
        task_id='save_result',
        python_callable=save_result,
    )

    failure = PythonOperator(
        task_id='log_failure',
        python_callable=log_failure,
    )

    done = EmptyOperator(
        task_id='done',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Dependencies
    extract >> check >> [save, failure] >> done
'''
        return dag_content

    async def _write_dag_file(self, dag_id: str, content: str) -> str:
        """DAG 파일 저장"""
        # 디렉토리 생성
        os.makedirs(self.dag_dir, exist_ok=True)

        file_path = os.path.join(self.dag_dir, f"{dag_id}.py")

        # 비동기 파일 쓰기
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._write_file_sync(file_path, content)
        )

        logger.info(f"DAG file written: {file_path}")
        return file_path

    def _write_file_sync(self, path: str, content: str):
        """동기 파일 쓰기"""
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
