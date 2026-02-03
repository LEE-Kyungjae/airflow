"""
Tests for Airflow DAGs.

These tests verify that DAGs can be loaded without errors
and that task dependencies are correctly configured.
"""

import pytest
from airflow.models import DagBag


@pytest.fixture
def dagbag():
    """Load all DAGs from the dags folder."""
    return DagBag(dag_folder='/opt/airflow/dags', include_examples=False)


def test_dagbag_import(dagbag):
    """Test that all DAGs can be imported without errors."""
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_source_manager_dag_loaded(dagbag):
    """Test that source_manager DAG is loaded."""
    assert 'source_manager' in dagbag.dags
    dag = dagbag.dags['source_manager']
    assert dag is not None


def test_history_manager_dag_loaded(dagbag):
    """Test that history_manager DAG is loaded."""
    assert 'history_manager' in dagbag.dags
    dag = dagbag.dags['history_manager']
    assert dag is not None


def test_source_manager_tasks(dagbag):
    """Test that source_manager DAG has expected tasks."""
    dag = dagbag.dags['source_manager']
    expected_tasks = [
        'register_source',
        'analyze_source',
        'generate_code',
        'validate_code',
        'save_crawler',
        'create_schedule'
    ]
    task_ids = [task.task_id for task in dag.tasks]

    for expected in expected_tasks:
        assert expected in task_ids, f"Missing task: {expected}"


def test_history_manager_tasks(dagbag):
    """Test that history_manager DAG has expected tasks."""
    dag = dagbag.dags['history_manager']
    expected_tasks = [
        'detect_changes',
        'backup_old_versions',
        'log_history',
        'cleanup_old_backups',
        'generate_report'
    ]
    task_ids = [task.task_id for task in dag.tasks]

    for expected in expected_tasks:
        assert expected in task_ids, f"Missing task: {expected}"


def test_source_manager_task_dependencies(dagbag):
    """Test task dependencies in source_manager DAG."""
    dag = dagbag.dags['source_manager']

    # Check that tasks are connected in the right order
    register = dag.get_task('register_source')
    analyze = dag.get_task('analyze_source')
    generate = dag.get_task('generate_code')
    validate = dag.get_task('validate_code')
    save = dag.get_task('save_crawler')
    schedule = dag.get_task('create_schedule')

    assert analyze in register.downstream_list
    assert generate in analyze.downstream_list
    assert validate in generate.downstream_list
    assert save in validate.downstream_list
    assert schedule in save.downstream_list
