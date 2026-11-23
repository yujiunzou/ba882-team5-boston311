"""
Boston 311 dbt Transformation Pipeline

Orchestrates dbt models, seeds, and tests using Astronomer Cosmos.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import (
    DbtDag,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.constants import TestBehavior

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt")
DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/dbt_venv/bin/dbt")

profile_config = ProfileConfig(
    profile_name="boston311",
    target_name=os.environ.get("DBT_TARGET", "dev"),
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

dbt_dag = DbtDag(
    dag_id="boston311_dbt",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    ),
    render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_ALL,
    ),
    operator_args={
        "append_env": True,
    },
    schedule=None,
    start_date=datetime(2026, 10, 19),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
    },
    tags=["boston311", "dbt", "transformation"],
)
