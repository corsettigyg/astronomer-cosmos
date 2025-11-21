Project Config
================

The ``cosmos.config.ProjectConfig`` allows you to specify information about where your dbt project is located and project
variables that should be used for rendering and execution. It takes the following arguments:

- ``dbt_project_path``: The full path to your dbt project. This directory should have a ``dbt_project.yml`` file
- ``models_relative_path``: The path to your models directory, relative to the ``dbt_project_path``. This defaults to
  ``models/``
- ``seeds_relative_path``: The path to your seeds directory, relative to the ``dbt_project_path``. This defaults to
  ``data/``
- ``snapshots_relative_path``: The path to your snapshots directory, relative to the ``dbt_project_path``. This defaults
  to ``snapshots/``
- ``manifest_path``: The absolute path to your manifests directory. This is only required if you're using Cosmos' manifest
  parsing mode. Along with supporting local paths for manifest parsing, starting with Cosmos 1.6.0, if you've
  Airflow >= 2.8.0, Cosmos also supports remote paths for manifest parsing(e.g. S3 URL). See :ref:`parsing-methods` for more details.
- ``project_name`` : The name of the project. If ``dbt_project_path`` is provided, the ``project_name`` defaults to the
  folder name containing ``dbt_project.yml``. If ``dbt_project_path`` is not provided, and ``manifest_path`` is provided,
  ``project_name`` is required as the name can not be inferred from ``dbt_project_path``
- ``dbt_vars``: (new in v1.3) A dictionary of dbt variables for the project rendering and execution. This argument overrides variables
  defined in the dbt_project.yml file. The dictionary of variables is dumped to a yaml string and passed to dbt commands
  as the --vars argument. Variables are only supported for rendering when using ``RenderConfig.LoadMode.DBT_LS`` and
  ``RenderConfig.LoadMode.CUSTOM`` load mode. Variables using `Airflow templating <https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#templates-reference>`_
  will only be rendered at execution time, not at render time.
- ``project_keys``: (new in v1.12) A dictionary of keys to dynamically replace in ``dbt_project.yml`` at runtime. This allows for dynamic
  modification of project configuration based on Airflow context. Keys can use dot notation for nested values
  (e.g., ``"models.my_project.materialized"``). Supports `Airflow templating <https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#templates-reference>`_
  for dynamic values. Unlike ``dbt_vars`` which are passed as command-line arguments, ``project_keys`` directly modify the
  ``dbt_project.yml`` file before dbt execution.
- ``env_vars``: (new in v1.3) A dictionary of environment variables used for rendering and execution. Rendering with
  env vars is only supported when using ``RenderConfig.LoadMode.DBT_LS`` load mode.
- ``install_dbt_deps``: (new in v1.9) Run dbt deps during DAG parsing and task execution if True (default).
- ``copy_dbt_packages``: (new in v1.10) Copy the dbt project ``dbt_packages`` instead of creating symbolic links, so Cosmos can run ``dbt deps`` incrementally.
- ``partial_parse``: (new in v1.4) If True, then attempt to use the ``partial_parse.msgpack`` if it exists. This is only used
  for the ``LoadMode.DBT_LS`` load mode, and for the ``ExecutionMode.LOCAL`` and ``ExecutionMode.VIRTUALENV``
  execution modes. Due to the way that dbt `partial parsing works <https://docs.getdbt.com/reference/parsing#known-limitations>`_, it does not work with Cosmos profile mapping classes. To benefit from this feature, users have to set the ``profiles_yml_filepath`` argument in ``ProfileConfig``.

Project Config Example
----------------------

.. code-block:: python

    from cosmos.config import ProjectConfig

    config = ProjectConfig(
        dbt_project_path="/path/to/dbt/project",
        models_relative_path="models",
        seeds_relative_path="data",
        snapshots_relative_path="snapshots",
        manifest_path="/path/to/manifests",
        env_vars={"MY_ENV_VAR": "my_env_value"},
        dbt_vars={
            "my_dbt_var": "my_value",
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
        project_keys={
            "name": "{{ dag.dag_id }}_project",
            "version": "{{ ds_nodash }}",
            "models.my_project.materialized": "{{ params.materialization }}",
            "models.my_project.schema": "{{ params.target_schema }}",
            "models.my_project.tags": ["{{ dag.dag_id }}", "production"],
        },
    )


Project Keys (Dynamic dbt_project.yml Configuration)
----------------------------------------------------

The ``project_keys`` parameter allows you to dynamically modify your ``dbt_project.yml`` file at runtime based on Airflow context.
This is particularly useful for:

- Environment-specific configurations
- Dynamic project naming based on DAG context
- Runtime materialization strategies
- Context-aware metadata and tags

Key Features
~~~~~~~~~~~~

**Dot Notation for Nested Keys**

You can use dot notation to modify nested configuration values:

.. code-block:: python

    project_keys = {
        "models.my_project.staging.materialized": "view",
        "models.my_project.marts.materialized": "table",
        "models.my_project.marts.meta.owner": "analytics_team"
    }

**Airflow Template Support**

``project_keys`` supports full Airflow templating, allowing you to use context variables:

.. code-block:: python

    project_keys = {
        # Dynamic project name based on DAG ID
        "name": "{{ dag.dag_id }}_project",
        
        # Version based on execution date
        "version": "{{ ds_nodash }}",
        
        # Environment-based configuration
        "models.my_project.materialized": "{{ 'table' if params.environment == 'prod' else 'view' }}",
        
        # Runtime metadata
        "models.my_project.meta.execution_date": "{{ ds }}",
        "models.my_project.meta.dag_run_id": "{{ run_id }}",
    }

**Complex Value Types**

``project_keys`` automatically converts string values to appropriate YAML types:

.. code-block:: python

    project_keys = {
        "models.my_project.enabled": "true",  # Becomes boolean True
        "models.my_project.threads": "4",     # Becomes integer 4
        "models.my_project.tags": '["tag1", "tag2"]',  # Becomes list ["tag1", "tag2"]
    }

Complete Example
~~~~~~~~~~~~~~~~

Here's a comprehensive example showing ``project_keys`` in action:

.. code-block:: python

    from cosmos import DbtDag, ProjectConfig, ProfileConfig
    from datetime import datetime

    project_config = ProjectConfig(
        dbt_project_path="/path/to/dbt/project",
        project_keys={
            # Dynamic project configuration
            "name": "{{ dag.dag_id }}_{{ params.environment }}",
            "version": "{{ ds_nodash }}.{{ params.environment }}",
            
            # Environment-specific model configurations
            "models.my_project.staging.materialized": "{{ 'table' if params.environment == 'prod' else 'view' }}",
            "models.my_project.staging.schema": "staging_{{ params.environment }}",
            "models.my_project.staging.tags": ["staging", "{{ params.environment }}"],
            
            # Performance configurations for production
            "models.my_project.marts.materialized": "table",
            "models.my_project.marts.meta.cluster_by": "{{ 'created_at' if params.environment == 'prod' else none }}",
            
            # Business metadata
            "models.my_project.marts.meta.owner": "analytics_team",
            "models.my_project.marts.meta.sla_hours": "{{ 4 if params.environment == 'prod' else 24 }}",
        }
    )

    dag = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        dag_id="dynamic_dbt_project",
        start_date=datetime(2023, 1, 1),
        params={
            "environment": "dev",
            "materialization": "view"
        }
    )

Comparison with dbt_vars
~~~~~~~~~~~~~~~~~~~~~~~~

While both ``dbt_vars`` and ``project_keys`` allow dynamic configuration, they work differently:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Feature
     - ``dbt_vars``
     - ``project_keys``
   * - **Scope**
     - Variables available in dbt models via ``{{ var('name') }}``
     - Direct modification of ``dbt_project.yml`` configuration
   * - **Implementation**
     - Passed as ``--vars`` command-line argument
     - Modifies ``dbt_project.yml`` file before execution
   * - **Use Cases**
     - Model logic, SQL parameters, feature flags
     - Project structure, materialization, schemas, metadata
   * - **Template Support**
     - ✅ Full Airflow templating
     - ✅ Full Airflow templating
   * - **Nested Configuration**
     - ❌ Flat key-value pairs only
     - ✅ Supports dot notation for nested keys

Best Practices
~~~~~~~~~~~~~~

1. **Use descriptive keys**: Make your ``project_keys`` self-documenting
2. **Leverage templating**: Take advantage of Airflow context for dynamic values
3. **Environment separation**: Use parameters to differentiate between environments
4. **Combine with dbt_vars**: Use both features together for comprehensive configuration
5. **Test thoroughly**: Validate your dynamic configurations in development first
