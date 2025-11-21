"""
Advanced example DAG demonstrating complex project_keys scenarios.

This DAG demonstrates more advanced use cases for project_keys including:
- Complex nested configurations
- Integration with dbt_vars
- Different configurations for different models
- Runtime environment-based configurations
"""
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Param

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import LoadMode
# from cosmos.profiles import PostgresUserPasswordProfileMapping  # Not needed for this example

# Path to the dbt project
DBT_PROJECT_PATH = Path(__file__).parent / "dbt" / "simple"

# Profile configuration - using profiles.yml file for simplicity
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

# Default DAG
with DAG(
    dag_id="example_project_keys_advanced",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["example", "project_keys", "advanced", "cosmos"],
    params={
        "environment": Param(
            default="dev",
            type="string",
            enum=["dev", "staging", "prod"],
            description="Deployment environment"
        ),
        "data_freshness_hours": Param(
            default=24,
            type="integer",
            description="Data freshness requirement in hours"
        ),
        "enable_tests": Param(
            default=True,
            type="boolean",
            description="Whether to enable dbt tests"
        ),
    },
) as dag:

    # Task Group 1: Staging models with environment-specific configuration
    staging_project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        project_keys={
            "name": "{{ dag.dag_id }}_staging",
            "version": "{{ ds_nodash }}.{{ params.environment }}",
            
            # Staging-specific model configurations
            "models.my_dbt_project.staging.materialized": "{{ 'table' if params.environment == 'prod' else 'view' }}",
            "models.my_dbt_project.staging.schema": "staging_{{ params.environment }}",
            "models.my_dbt_project.staging.tags": ["staging", "{{ params.environment }}"],
            
            # Performance configurations based on environment
            "models.my_dbt_project.staging.meta.cluster_by": "{{ 'created_at' if params.environment == 'prod' else none }}",
            "models.my_dbt_project.staging.meta.partition_by": "{{ 'date(created_at)' if params.environment == 'prod' else none }}",
            
            # Data quality configurations
            "models.my_dbt_project.staging.meta.freshness_hours": "{{ params.data_freshness_hours }}",
            "models.my_dbt_project.staging.meta.tests_enabled": "{{ params.enable_tests }}",
        },
        dbt_vars={
            "start_date": "{{ data_interval_start.strftime('%Y-%m-%d') }}",
            "end_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
            "environment": "{{ params.environment }}",
        }
    )

    staging_models = DbtTaskGroup(
        group_id="staging_models",
        project_config=staging_project_config,
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt",
        ),
        # select=["tag:staging"],  # Commented out for simplicity
        operator_args={
            "vars": {
                "execution_timestamp": "{{ ts }}",
            }
        }
    )

    # Task Group 2: Mart models with different configuration
    mart_project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        project_keys={
            "name": "{{ dag.dag_id }}_mart",
            "version": "{{ ds_nodash }}.{{ params.environment }}",
            
            # Mart-specific configurations
            "models.my_dbt_project.marts.materialized": "table",
            "models.my_dbt_project.marts.schema": "marts_{{ params.environment }}",
            "models.my_dbt_project.marts.tags": ["marts", "{{ params.environment }}", "business_critical"],
            
            # Always use performance optimizations for marts
            "models.my_dbt_project.marts.meta.cluster_by": "business_date",
            "models.my_dbt_project.marts.meta.partition_by": "date(business_date)",
            
            # Business metadata
            "models.my_dbt_project.marts.meta.owner": "analytics_team",
            "models.my_dbt_project.marts.meta.sla_hours": "{{ 4 if params.environment == 'prod' else 24 }}",
            "models.my_dbt_project.marts.meta.criticality": "{{ 'high' if params.environment == 'prod' else 'medium' }}",
        },
        dbt_vars={
            "start_date": "{{ data_interval_start.strftime('%Y-%m-%d') }}",
            "end_date": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
            "environment": "{{ params.environment }}",
            "business_date": "{{ ds }}",
        }
    )

    mart_models = DbtTaskGroup(
        group_id="mart_models",
        project_config=mart_project_config,
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt",
        ),
        # select=["tag:marts"],  # Commented out for simplicity
        operator_args={
            "full_refresh": "{{ params.environment == 'dev' }}",
        }
    )

    # Task Group 3: Tests with conditional configuration
    test_project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        project_keys={
            "name": "{{ dag.dag_id }}_tests",
            "version": "{{ ds_nodash }}.{{ params.environment }}",
            
            # Test-specific configurations
            "tests.severity": "{{ 'error' if params.environment == 'prod' else 'warn' }}",
            "tests.store_failures": "{{ params.environment == 'prod' }}",
            "tests.meta.alert_on_failure": "{{ params.environment == 'prod' }}",
            "tests.meta.max_failure_rate": "{{ 0.01 if params.environment == 'prod' else 0.05 }}",
        }
    )

    # Only run tests if enabled
    test_models = DbtTaskGroup(
        group_id="test_models",
        project_config=test_project_config,
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt",
        ),
        # select=["test_type:data"],  # Commented out for simplicity
        operator_args={
            "vars": {
                "test_execution_time": "{{ ts }}",
                "test_environment": "{{ params.environment }}",
            }
        }
    )

    # Set up dependencies
    staging_models >> mart_models >> test_models
