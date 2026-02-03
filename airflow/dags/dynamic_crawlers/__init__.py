"""
Dynamic Crawlers Directory.

This directory contains auto-generated DAG files for individual crawlers.
Each file is created by the source_manager_dag when a new source is registered.

Files in this directory follow the naming convention:
    crawler_{source_id}.py

These DAGs are automatically picked up by Airflow's scheduler.
Do not manually edit files in this directory - changes may be overwritten
by the auto-recovery system.
"""
