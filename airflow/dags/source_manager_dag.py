"""
Source Manager DAG for registering and managing crawling sources.

This DAG handles:
1. Source registration from API triggers
2. Page structure analysis
3. Crawler code generation via GPT
4. Code validation and testing
5. Dynamic DAG creation for scheduled crawling
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

import requests
from bs4 import BeautifulSoup

# Import utility modules
import sys
sys.path.insert(0, '/opt/airflow/dags')
from utils.gpt_service import GPTService
from utils.mongo_service import MongoService
from utils.code_validator import CodeValidator

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'crawler-system',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def register_source(**context) -> str:
    """
    Register a new source from DAG run configuration.

    Expects dag_run.conf to contain:
    - name: Source name
    - url: Target URL
    - type: Data type (html, pdf, excel, csv)
    - fields: List of fields to extract
    - schedule: Cron expression
    """
    conf = context['dag_run'].conf or {}

    required_fields = ['name', 'url', 'type', 'fields', 'schedule']
    for field in required_fields:
        if field not in conf:
            raise ValueError(f"Missing required field: {field}")

    mongo = MongoService()

    try:
        # Check if source already exists
        existing = mongo.get_source_by_name(conf['name'])
        if existing:
            logger.info(f"Source '{conf['name']}' already exists, updating...")
            mongo.update_source(existing['_id'], conf)
            return existing['_id']

        # Create new source
        source_data = {
            'name': conf['name'],
            'url': conf['url'],
            'type': conf['type'],
            'fields': conf['fields'],
            'schedule': conf['schedule'],
            'status': 'inactive'  # Will be activated after crawler is ready
        }

        source_id = mongo.create_source(source_data)
        logger.info(f"Created source: {source_id}")

        return source_id

    finally:
        mongo.close()


def analyze_source(**context) -> Dict[str, Any]:
    """
    Analyze the source URL to understand its structure.

    Returns page structure information for code generation.
    """
    ti = context['ti']
    source_id = ti.xcom_pull(task_ids='register_source')

    mongo = MongoService()

    try:
        source = mongo.get_source(source_id)
        if not source:
            raise ValueError(f"Source not found: {source_id}")

        url = source['url']
        data_type = source['type']

        analysis_result = {
            'source_id': source_id,
            'url': url,
            'type': data_type,
            'fields': source['fields']
        }

        if data_type == 'html':
            # Fetch and analyze HTML structure
            response = requests.get(
                url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                timeout=30
            )
            response.raise_for_status()

            html_content = response.text
            analysis_result['html_sample'] = html_content[:10000]
            analysis_result['encoding'] = response.encoding

            # Basic structure analysis
            soup = BeautifulSoup(html_content, 'lxml')
            analysis_result['title'] = soup.title.string if soup.title else None
            analysis_result['has_tables'] = len(soup.find_all('table')) > 0
            analysis_result['has_forms'] = len(soup.find_all('form')) > 0

            # Try to use GPT for selector analysis
            try:
                gpt = GPTService()
                field_names = [f['name'] for f in source['fields']]
                structure_analysis = gpt.analyze_page_structure(
                    url, html_content, field_names
                )
                analysis_result['gpt_analysis'] = structure_analysis
            except Exception as e:
                logger.warning(f"GPT analysis failed: {e}")

        elif data_type in ['pdf', 'excel', 'csv']:
            # For file types, just store metadata
            analysis_result['is_file'] = True
            analysis_result['file_extension'] = url.split('.')[-1].lower()

        logger.info(f"Analysis complete for source {source_id}")
        return analysis_result

    finally:
        mongo.close()


def generate_code(**context) -> str:
    """
    Generate crawler code using GPT.

    Uses analysis results to create appropriate crawler code.
    """
    ti = context['ti']
    analysis = ti.xcom_pull(task_ids='analyze_source')

    source_id = analysis['source_id']
    url = analysis['url']
    data_type = analysis['type']
    fields = analysis['fields']

    gpt = GPTService()

    try:
        if data_type == 'html':
            html_sample = analysis.get('html_sample', '')

            # Use GPT analysis if available
            gpt_analysis = analysis.get('gpt_analysis', {})
            if gpt_analysis.get('fields'):
                # Update fields with GPT-suggested selectors
                for field in fields:
                    for gpt_field in gpt_analysis['fields']:
                        if field['name'] == gpt_field['name']:
                            field['selector'] = gpt_field.get('selector', field.get('selector'))
                            field['data_type'] = gpt_field.get('data_type', field.get('data_type'))

            code = gpt.generate_crawler_code(
                source_id=source_id,
                url=url,
                data_type=data_type,
                fields=fields,
                html_sample=html_sample
            )

        elif data_type == 'pdf':
            code = gpt.generate_pdf_crawler_code(
                source_id=source_id,
                url=url,
                fields=fields
            )

        elif data_type in ['excel', 'csv']:
            code = gpt.generate_excel_crawler_code(
                source_id=source_id,
                url=url,
                fields=fields
            )

        else:
            raise ValueError(f"Unsupported data type: {data_type}")

        logger.info(f"Generated code for source {source_id}")
        return code

    except Exception as e:
        logger.error(f"Code generation failed: {e}")
        raise


def validate_code(**context) -> Dict[str, Any]:
    """
    Validate generated crawler code.

    Performs syntax check, security scan, and basic execution test.
    """
    ti = context['ti']
    code = ti.xcom_pull(task_ids='generate_code')
    analysis = ti.xcom_pull(task_ids='analyze_source')

    source_id = analysis['source_id']
    data_type = analysis['type']

    validator = CodeValidator()
    validation_result = validator.full_validation(
        code=code,
        crawler_type=data_type,
        source_id=source_id
    )

    if not validation_result['valid']:
        error_msg = '; '.join(validation_result['errors'])
        logger.error(f"Code validation failed: {error_msg}")

        # Try to regenerate code if validation fails
        # This would require another GPT call with error context
        raise ValueError(f"Generated code failed validation: {error_msg}")

    if validation_result['warnings']:
        logger.warning(f"Validation warnings: {validation_result['warnings']}")

    validation_result['code'] = code
    return validation_result


def save_crawler(**context) -> str:
    """
    Save validated crawler code to MongoDB.
    """
    ti = context['ti']
    validation_result = ti.xcom_pull(task_ids='validate_code')
    analysis = ti.xcom_pull(task_ids='analyze_source')

    source_id = analysis['source_id']
    code = validation_result['code']

    mongo = MongoService()

    try:
        # Create DAG ID for this crawler
        dag_id = f"crawler_{source_id.replace('-', '_')}"

        crawler_data = {
            'source_id': source_id,
            'code': code,
            'version': 1,
            'status': 'testing',
            'dag_id': dag_id,
            'created_by': 'gpt',
            'gpt_prompt': 'auto-generated'
        }

        crawler_id = mongo.create_crawler(crawler_data)
        logger.info(f"Created crawler: {crawler_id}")

        return crawler_id

    finally:
        mongo.close()


def create_schedule(**context) -> str:
    """
    Create dynamic DAG for the crawler.

    Generates an ADVANCED DAG file with:
    - ETL Pipeline Integration
    - Self-Healing System
    - Data Quality Validation
    """
    ti = context['ti']
    crawler_id = ti.xcom_pull(task_ids='save_crawler')
    analysis = ti.xcom_pull(task_ids='analyze_source')
    validation_result = ti.xcom_pull(task_ids='validate_code')

    source_id = analysis['source_id']

    mongo = MongoService()

    try:
        source = mongo.get_source(source_id)
        crawler = mongo.get_crawler(crawler_id)

        if not source or not crawler:
            raise ValueError("Source or crawler not found")

        dag_id = crawler['dag_id']
        schedule = source['schedule']
        code = validation_result['code']

        # Use advanced DAG generator with ETL + Self-Healing
        from utils.advanced_dag_generator import generate_advanced_dag

        dag_content = generate_advanced_dag(
            dag_id=dag_id,
            source_id=source_id,
            crawler_id=crawler_id,
            source_name=source['name'],
            url=source['url'],
            schedule=schedule,
            crawler_code=code,
            data_type=source['type'],
            fields=source['fields'],
            metadata=source.get('metadata', {})
        )

        # Write DAG file
        dag_file_path = f"/opt/airflow/dags/dynamic_crawlers/{dag_id}.py"
        os.makedirs(os.path.dirname(dag_file_path), exist_ok=True)

        with open(dag_file_path, 'w') as f:
            f.write(dag_content)

        logger.info(f"Created DAG file: {dag_file_path}")

        # Activate crawler and source
        mongo.activate_crawler(crawler_id)
        mongo.update_source_status(source_id, 'active')

        return dag_id

    finally:
        mongo.close()


def _generate_dag_file(
    dag_id: str,
    source_id: str,
    crawler_id: str,
    source_name: str,
    schedule: str,
    crawler_code: str,
    data_type: str,
    fields: list,
    url: str
) -> str:
    """
    Generate dynamic DAG file content.
    """
    # Escape code for embedding
    escaped_code = crawler_code.replace('\\', '\\\\').replace("'''", "\\'\\'\\'")
    fields_json = json.dumps(fields)

    dag_template = f'''"""
Auto-generated crawler DAG for {source_name}.
Source ID: {source_id}
Crawler ID: {crawler_id}
Generated at: {datetime.utcnow().isoformat()}
"""

import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

sys.path.insert(0, '/opt/airflow/dags')
from utils.mongo_service import MongoService
from utils.gpt_service import GPTService
from utils.error_handler import ErrorHandler, ErrorCode

logger = logging.getLogger(__name__)

# Configuration
SOURCE_ID = "{source_id}"
CRAWLER_ID = "{crawler_id}"
SOURCE_NAME = "{source_name}"
URL = "{url}"
DATA_TYPE = "{data_type}"
FIELDS = {fields_json}

# Crawler code
CRAWLER_CODE = \'\'\'
{escaped_code}
\'\'\'

default_args = {{
    'owner': 'crawler-system',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}}


def execute_crawler(**context) -> Dict[str, Any]:
    """Execute the crawler code and return results."""
    import traceback

    # Execute the crawler code
    local_vars = {{}}
    try:
        exec(CRAWLER_CODE, {{}}, local_vars)
    except Exception as e:
        logger.error(f"Failed to load crawler code: {{e}}")
        raise

    # Find the crawl function
    crawl_func = None
    for name, obj in local_vars.items():
        if name.startswith('crawl_') and callable(obj):
            crawl_func = obj
            break

    if not crawl_func:
        raise ValueError("No crawl function found in code")

    # Execute crawl
    start_time = datetime.utcnow()
    try:
        result = crawl_func()
        execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

        return {{
            'success': True,
            'data': result,
            'record_count': len(result) if isinstance(result, list) else 1,
            'execution_time_ms': execution_time
        }}

    except Exception as e:
        execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        stack_trace = traceback.format_exc()

        return {{
            'success': False,
            'error_message': str(e),
            'stack_trace': stack_trace,
            'execution_time_ms': execution_time
        }}


def save_result(**context) -> str:
    """Save crawl result to MongoDB."""
    ti = context['ti']
    result = ti.xcom_pull(task_ids='execute_crawler')
    run_id = context['run_id']

    mongo = MongoService()
    try:
        result_data = {{
            'source_id': SOURCE_ID,
            'crawler_id': CRAWLER_ID,
            'run_id': run_id,
            'status': 'success' if result['success'] else 'failed',
            'data': result.get('data'),
            'record_count': result.get('record_count', 0),
            'error_message': result.get('error_message'),
            'execution_time_ms': result.get('execution_time_ms', 0)
        }}

        result_id = mongo.save_crawl_result(result_data)

        # Update source status
        if result['success']:
            mongo.update_source_status(
                SOURCE_ID,
                'active',
                last_run=datetime.utcnow(),
                last_success=datetime.utcnow()
            )
        else:
            mongo.update_source_status(
                SOURCE_ID,
                'error',
                last_run=datetime.utcnow(),
                increment_error=True
            )

        return result_id

    finally:
        mongo.close()


def handle_error(**context) -> Dict[str, Any]:
    """Handle crawl errors and attempt auto-recovery."""
    ti = context['ti']
    result = ti.xcom_pull(task_ids='execute_crawler')

    if result.get('success'):
        # No error to handle
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("No error to handle")

    error_message = result.get('error_message', '')
    stack_trace = result.get('stack_trace', '')

    # Classify error
    try:
        exception = Exception(error_message)
        classification = ErrorHandler.classify_error(exception)
    except Exception as e:
        logger.error(f"Error classification failed: {{e}}")
        classification = None

    if not classification:
        return {{'recoverable': False, 'reason': 'Classification failed'}}

    mongo = MongoService()
    try:
        # Log error
        error_data = ErrorHandler.create_error_log_data(
            classification=classification,
            source_id=SOURCE_ID,
            crawler_id=CRAWLER_ID,
            run_id=context['run_id']
        )
        mongo.log_error(error_data)

        return {{
            'recoverable': classification.auto_recoverable,
            'error_code': classification.code.value,
            'recovery_action': classification.recovery_action,
            'max_retries': classification.max_retries
        }}

    finally:
        mongo.close()


def auto_recover(**context) -> bool:
    """Attempt automatic recovery using GPT."""
    ti = context['ti']
    error_info = ti.xcom_pull(task_ids='handle_error')
    result = ti.xcom_pull(task_ids='execute_crawler')

    if not error_info.get('recoverable'):
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("Error not auto-recoverable")

    if 'gpt' not in error_info.get('recovery_action', '').lower():
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("Recovery action does not require GPT")

    mongo = MongoService()
    gpt = GPTService()

    try:
        # Get current crawler code
        crawler = mongo.get_crawler(CRAWLER_ID)
        current_code = crawler['code']

        # Generate fixed code
        fixed_code = gpt.fix_crawler_code(
            current_code=current_code,
            error_code=error_info['error_code'],
            error_message=result.get('error_message', ''),
            stack_trace=result.get('stack_trace', '')
        )

        # Save old version to history
        mongo.save_crawler_history(
            crawler_id=CRAWLER_ID,
            version=crawler['version'],
            code=current_code,
            change_reason='auto_fix',
            change_detail=f"Auto-fix for error {{error_info['error_code']}}",
            changed_by='gpt'
        )

        # Update crawler with fixed code
        mongo.update_crawler_code(
            crawler_id=CRAWLER_ID,
            new_code=fixed_code,
            created_by='gpt'
        )

        logger.info(f"Auto-recovery complete for crawler {{CRAWLER_ID}}")
        return True

    except Exception as e:
        logger.error(f"Auto-recovery failed: {{e}}")
        return False

    finally:
        mongo.close()


# Create DAG
with DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='Auto-generated crawler for {source_name}',
    schedule_interval='{schedule}',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crawler', 'auto-generated', '{data_type}'],
    max_active_runs=1
) as dag:

    execute_task = PythonOperator(
        task_id='execute_crawler',
        python_callable=execute_crawler,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_result',
        python_callable=save_result,
        provide_context=True
    )

    error_task = PythonOperator(
        task_id='handle_error',
        python_callable=handle_error,
        provide_context=True,
        trigger_rule='all_done'
    )

    recover_task = PythonOperator(
        task_id='auto_recover',
        python_callable=auto_recover,
        provide_context=True,
        trigger_rule='all_done'
    )

    execute_task >> save_task >> error_task >> recover_task
'''

    return dag_template


# Define the main DAG
with DAG(
    dag_id='source_manager',
    default_args=default_args,
    description='Manage crawling sources - register, analyze, generate code',
    schedule_interval=None,  # Triggered via API
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['management', 'source']
) as dag:

    register_task = PythonOperator(
        task_id='register_source',
        python_callable=register_source,
        provide_context=True
    )

    analyze_task = PythonOperator(
        task_id='analyze_source',
        python_callable=analyze_source,
        provide_context=True
    )

    generate_task = PythonOperator(
        task_id='generate_code',
        python_callable=generate_code,
        provide_context=True
    )

    validate_task = PythonOperator(
        task_id='validate_code',
        python_callable=validate_code,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_crawler',
        python_callable=save_crawler,
        provide_context=True
    )

    schedule_task = PythonOperator(
        task_id='create_schedule',
        python_callable=create_schedule,
        provide_context=True
    )

    # Define task dependencies
    register_task >> analyze_task >> generate_task >> validate_task >> save_task >> schedule_task
