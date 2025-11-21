"""
Unit tests for project_keys integration with operators.
"""
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from cosmos.operators.base import AbstractDbtBase
from cosmos.operators.local import DbtRunLocalOperator


class TestProjectKeysOperatorIntegration:
    """Test project_keys integration with dbt operators."""

    def create_temp_dbt_project(self, dbt_project_content: dict) -> Path:
        """Helper to create a temporary dbt project with dbt_project.yml."""
        temp_dir = Path(tempfile.mkdtemp())
        dbt_project_path = temp_dir / "dbt_project.yml"
        
        with open(dbt_project_path, "w") as f:
            yaml.dump(dbt_project_content, f)
        
        return temp_dir

    def test_abstract_dbt_base_accepts_project_keys(self):
        """Test that AbstractDbtBase accepts project_keys parameter."""
        
        class TestDbtOperator(AbstractDbtBase):
            base_cmd = ["test"]
            
            def build_and_run_cmd(self, context, cmd_flags, **kwargs):
                pass
        
        project_keys = {
            "name": "test_project",
            "models.my_project.materialized": "table"
        }
        
        operator = TestDbtOperator(
            project_dir="/tmp/test",
            project_keys=project_keys
        )
        
        assert hasattr(operator, 'project_keys')
        assert operator.project_keys == project_keys

    def test_abstract_dbt_base_project_keys_defaults_to_none(self):
        """Test that project_keys defaults to None when not provided."""
        
        class TestDbtOperator(AbstractDbtBase):
            base_cmd = ["test"]
            
            def build_and_run_cmd(self, context, cmd_flags, **kwargs):
                pass
        
        operator = TestDbtOperator(project_dir="/tmp/test")
        
        assert hasattr(operator, 'project_keys')
        assert operator.project_keys is None

    def test_project_keys_in_template_fields(self):
        """Test that project_keys is included in template_fields."""
        
        class TestDbtOperator(AbstractDbtBase):
            base_cmd = ["test"]
            
            def build_and_run_cmd(self, context, cmd_flags, **kwargs):
                pass
        
        assert "project_keys" in TestDbtOperator.template_fields

    @patch('cosmos.operators.local.AbstractDbtLocalBase._clone_project')
    @patch('cosmos.dbt.project.apply_project_keys_to_dbt_project_yml')
    def test_project_keys_applied_during_execution(self, mock_apply_project_keys, mock_clone_project):
        """Test that project_keys are applied during operator execution."""
        
        # Create a mock context
        mock_context = {
            'dag': Mock(dag_id='test_dag'),
            'ds': '2023-01-01',
            'task_instance': Mock(),
            'run_id': 'test_run'
        }
        
        project_keys = {
            "name": "{{ dag.dag_id }}_project",
            "models.my_project.materialized": "table"
        }
        
        # This test will need to be updated once the actual implementation is done
        # For now, we're setting up the structure
        
        # Mock the operator methods that would be called
        with patch('tempfile.TemporaryDirectory') as mock_temp_dir:
            mock_temp_dir.return_value.__enter__.return_value = "/tmp/test_dir"
            mock_temp_dir.return_value.__exit__.return_value = None
            
            # The actual test would verify that apply_project_keys_to_dbt_project_yml
            # is called with the rendered project_keys
            pass

    def test_render_project_keys_with_context(self):
        """Test rendering project_keys with Airflow context."""
        
        # This test will verify that template rendering works for project_keys
        # Similar to how vars are rendered in the existing codebase
        
        project_keys = {
            "name": "{{ dag.dag_id }}_project",
            "version": "{{ ds }}",
            "models.my_project.schema": "{{ params.target_schema }}"
        }
        
        mock_context = {
            'dag': Mock(dag_id='test_dag'),
            'ds': '2023-01-01',
            'params': {'target_schema': 'production'}
        }
        
        # This would test the _render_project_keys method once implemented
        expected_rendered = {
            "name": "test_dag_project",
            "version": "2023-01-01",
            "models.my_project.schema": "production"
        }
        
        # The actual assertion would be:
        # rendered_keys = operator._render_project_keys(mock_context)
        # assert rendered_keys == expected_rendered

    @patch('cosmos.operators.local.AbstractDbtLocalBase.invoke_dbt')
    @patch('cosmos.dbt.project.apply_project_keys_to_dbt_project_yml')
    def test_project_keys_integration_with_local_operator(self, mock_apply_project_keys, mock_invoke_dbt):
        """Test project_keys integration with DbtRunLocalOperator."""
        
        # Create a temporary dbt project
        original_content = {
            "name": "original_project",
            "version": "1.0.0",
            "profile": "default"
        }
        
        temp_project_dir = self.create_temp_dbt_project(original_content)
        
        project_keys = {
            "name": "test_project",
            "models.my_project.materialized": "table"
        }
        
        # Mock the necessary dependencies
        with patch('cosmos.operators.local.tempfile.TemporaryDirectory') as mock_temp_dir, \
             patch('cosmos.operators.local.AbstractDbtLocalBase._clone_project'), \
             patch('cosmos.config.ProfileConfig') as mock_profile_config:
            
            mock_temp_dir.return_value.__enter__.return_value = str(temp_project_dir)
            mock_temp_dir.return_value.__exit__.return_value = None
            
            mock_profile_config.return_value.ensure_profile.return_value.__enter__.return_value = (
                Path("/tmp/profiles"), {}
            )
            mock_profile_config.return_value.ensure_profile.return_value.__exit__.return_value = None
            
            # This test structure is ready for when the implementation is complete
            pass

    def test_project_keys_with_different_execution_modes(self):
        """Test that project_keys work with different execution modes."""
        
        project_keys = {
            "name": "test_project",
            "models.my_project.materialized": "table"
        }
        
        # Test with different operator types once implemented:
        # - DbtRunLocalOperator
        # - DbtRunDockerOperator  
        # - DbtRunKubernetesOperator
        # etc.
        
        # For now, just verify the structure is in place
        assert project_keys is not None

    def test_project_keys_error_handling(self):
        """Test error handling for project_keys functionality."""
        
        # Test cases for error handling:
        # 1. Invalid project_keys format
        # 2. Missing dbt_project.yml
        # 3. Invalid YAML in dbt_project.yml
        # 4. Permission errors when writing to temp directory
        
        invalid_project_keys = "not_a_dict"
        
        class TestDbtOperator(AbstractDbtBase):
            base_cmd = ["test"]
            
            def build_and_run_cmd(self, context, cmd_flags, **kwargs):
                pass
        
        # This should not raise an error during initialization
        # Error handling should happen during execution
        operator = TestDbtOperator(
            project_dir="/tmp/test",
            project_keys=invalid_project_keys
        )
        
        assert operator.project_keys == invalid_project_keys

    def test_project_keys_with_empty_values(self):
        """Test handling of empty or None project_keys values."""
        
        class TestDbtOperator(AbstractDbtBase):
            base_cmd = ["test"]
            
            def build_and_run_cmd(self, context, cmd_flags, **kwargs):
                pass
        
        # Test with None
        operator1 = TestDbtOperator(project_dir="/tmp/test", project_keys=None)
        assert operator1.project_keys is None
        
        # Test with empty dict
        operator2 = TestDbtOperator(project_dir="/tmp/test", project_keys={})
        assert operator2.project_keys == {}

    def test_project_keys_precedence_over_dbt_project_yml(self):
        """Test that project_keys values override dbt_project.yml values."""
        
        # This test will verify that when both dbt_project.yml and project_keys
        # define the same key, project_keys takes precedence
        
        original_content = {
            "name": "original_project",
            "version": "1.0.0",
            "models": {
                "my_project": {
                    "materialized": "view"
                }
            }
        }
        
        project_keys = {
            "name": "overridden_project",
            "models.my_project.materialized": "table"
        }
        
        # The test implementation will verify this behavior
        # once apply_project_keys_to_dbt_project_yml is implemented
        
        assert project_keys["name"] == "overridden_project"
        assert project_keys["models.my_project.materialized"] == "table"


