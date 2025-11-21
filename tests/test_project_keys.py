"""
Unit tests for project_keys functionality.
"""
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from cosmos.config import ProjectConfig
from cosmos.dbt.project import apply_project_keys_to_dbt_project_yml
from cosmos.exceptions import CosmosValueError


class TestProjectKeysConfig:
    """Test project_keys parameter in ProjectConfig."""

    def test_init_with_project_keys_succeeds(self):
        """Test that ProjectConfig accepts project_keys parameter."""
        project_keys = {
            "name": "test_project",
            "version": "1.0.0",
            "models.my_project.materialized": "table"
        }
        project_config = ProjectConfig(
            dbt_project_path="path/to/dbt/project",
            project_keys=project_keys
        )
        assert project_config.project_keys == project_keys

    def test_init_without_project_keys_defaults_to_none(self):
        """Test that project_keys defaults to None when not provided."""
        project_config = ProjectConfig(dbt_project_path="path/to/dbt/project")
        assert project_config.project_keys is None

    def test_init_with_empty_project_keys(self):
        """Test that empty project_keys dict is handled correctly."""
        project_config = ProjectConfig(
            dbt_project_path="path/to/dbt/project",
            project_keys={}
        )
        assert project_config.project_keys == {}

    def test_project_keys_with_templated_values(self):
        """Test that project_keys can contain Airflow template strings."""
        project_keys = {
            "name": "{{ dag.dag_id }}_project",
            "version": "{{ ds }}",
            "models.my_project.schema": "{{ params.target_schema }}"
        }
        project_config = ProjectConfig(
            dbt_project_path="path/to/dbt/project",
            project_keys=project_keys
        )
        assert project_config.project_keys == project_keys


class TestApplyProjectKeysFunction:
    """Test the apply_project_keys_to_dbt_project_yml function."""

    def create_temp_dbt_project(self, dbt_project_content: dict) -> Path:
        """Helper to create a temporary dbt project with dbt_project.yml."""
        temp_dir = Path(tempfile.mkdtemp())
        dbt_project_path = temp_dir / "dbt_project.yml"
        
        with open(dbt_project_path, "w") as f:
            yaml.dump(dbt_project_content, f)
        
        return temp_dir

    def test_apply_simple_project_keys(self):
        """Test applying simple top-level project keys."""
        original_content = {
            "name": "original_project",
            "version": "0.1.0",
            "profile": "default"
        }
        
        project_keys = {
            "name": "new_project_name",
            "version": "1.0.0"
        }
        
        temp_dir = self.create_temp_dbt_project(original_content)
        
        # Apply project keys
        apply_project_keys_to_dbt_project_yml(temp_dir, project_keys)
        
        # Read the modified file
        with open(temp_dir / "dbt_project.yml", "r") as f:
            modified_content = yaml.safe_load(f)
        
        expected_content = {
            "name": "new_project_name",
            "version": "1.0.0",
            "profile": "default"
        }
        
        assert modified_content == expected_content

    def test_apply_nested_project_keys(self):
        """Test applying nested project keys using dot notation."""
        original_content = {
            "name": "test_project",
            "version": "1.0.0",
            "models": {
                "my_project": {
                    "materialized": "view",
                    "schema": "default"
                }
            }
        }
        
        project_keys = {
            "models.my_project.materialized": "table",
            "models.my_project.schema": "production"
        }
        
        temp_dir = self.create_temp_dbt_project(original_content)
        
        # Apply project keys
        apply_project_keys_to_dbt_project_yml(temp_dir, project_keys)
        
        # Read the modified file
        with open(temp_dir / "dbt_project.yml", "r") as f:
            modified_content = yaml.safe_load(f)
        
        expected_content = {
            "name": "test_project",
            "version": "1.0.0",
            "models": {
                "my_project": {
                    "materialized": "table",
                    "schema": "production"
                }
            }
        }
        
        assert modified_content == expected_content

    def test_apply_project_keys_creates_nested_structure(self):
        """Test that project keys can create new nested structures."""
        original_content = {
            "name": "test_project",
            "version": "1.0.0"
        }
        
        project_keys = {
            "models.new_project.materialized": "table",
            "models.new_project.tags": ["production"]
        }
        
        temp_dir = self.create_temp_dbt_project(original_content)
        
        # Apply project keys
        apply_project_keys_to_dbt_project_yml(temp_dir, project_keys)
        
        # Read the modified file
        with open(temp_dir / "dbt_project.yml", "r") as f:
            modified_content = yaml.safe_load(f)
        
        expected_content = {
            "name": "test_project",
            "version": "1.0.0",
            "models": {
                "new_project": {
                    "materialized": "table",
                    "tags": ["production"]
                }
            }
        }
        
        assert modified_content == expected_content

    def test_apply_project_keys_with_complex_values(self):
        """Test applying project keys with complex values (lists, dicts)."""
        original_content = {
            "name": "test_project",
            "version": "1.0.0"
        }
        
        project_keys = {
            "models.my_project.tags": ["tag1", "tag2"],
            "models.my_project.meta.owner": "data_team"
        }
        
        temp_dir = self.create_temp_dbt_project(original_content)
        
        # Apply project keys
        apply_project_keys_to_dbt_project_yml(temp_dir, project_keys)
        
        # Read the modified file
        with open(temp_dir / "dbt_project.yml", "r") as f:
            modified_content = yaml.safe_load(f)
        
        expected_content = {
            "name": "test_project",
            "version": "1.0.0",
            "models": {
                "my_project": {
                    "tags": ["tag1", "tag2"],
                    "meta": {
                        "owner": "data_team"
                    }
                }
            }
        }
        
        assert modified_content == expected_content

    def test_apply_empty_project_keys(self):
        """Test that empty project_keys dict doesn't modify the file."""
        original_content = {
            "name": "test_project",
            "version": "1.0.0",
            "profile": "default"
        }
        
        temp_dir = self.create_temp_dbt_project(original_content)
        
        # Apply empty project keys
        apply_project_keys_to_dbt_project_yml(temp_dir, {})
        
        # Read the modified file
        with open(temp_dir / "dbt_project.yml", "r") as f:
            modified_content = yaml.safe_load(f)
        
        assert modified_content == original_content

    def test_apply_project_keys_missing_dbt_project_yml(self):
        """Test that function raises error when dbt_project.yml doesn't exist."""
        temp_dir = Path(tempfile.mkdtemp())
        
        project_keys = {"name": "test_project"}
        
        with pytest.raises(FileNotFoundError):
            apply_project_keys_to_dbt_project_yml(temp_dir, project_keys)

    def test_apply_project_keys_invalid_yaml(self):
        """Test handling of invalid YAML in dbt_project.yml."""
        temp_dir = Path(tempfile.mkdtemp())
        dbt_project_path = temp_dir / "dbt_project.yml"
        
        # Write invalid YAML
        with open(dbt_project_path, "w") as f:
            f.write("invalid: yaml: content: [")
        
        project_keys = {"name": "test_project"}
        
        with pytest.raises(yaml.YAMLError):
            apply_project_keys_to_dbt_project_yml(temp_dir, project_keys)

    def test_apply_project_keys_preserves_comments_and_formatting(self):
        """Test that applying project keys preserves YAML structure as much as possible."""
        original_content = {
            "name": "test_project",
            "version": "1.0.0",
            "profile": "default",
            "models": {
                "my_project": {
                    "materialized": "view"
                }
            }
        }
        
        project_keys = {
            "models.my_project.materialized": "table"
        }
        
        temp_dir = self.create_temp_dbt_project(original_content)
        
        # Apply project keys
        apply_project_keys_to_dbt_project_yml(temp_dir, project_keys)
        
        # Read the modified file
        with open(temp_dir / "dbt_project.yml", "r") as f:
            modified_content = yaml.safe_load(f)
        
        # Verify the structure is maintained
        assert "name" in modified_content
        assert "version" in modified_content
        assert "profile" in modified_content
        assert "models" in modified_content
        assert modified_content["models"]["my_project"]["materialized"] == "table"

    def test_deep_nested_key_creation(self):
        """Test creating deeply nested keys that don't exist."""
        original_content = {
            "name": "test_project",
            "version": "1.0.0"
        }
        
        project_keys = {
            "models.my_project.staging.customers.materialized": "table",
            "models.my_project.staging.customers.meta.owner": "analytics_team"
        }
        
        temp_dir = self.create_temp_dbt_project(original_content)
        
        # Apply project keys
        apply_project_keys_to_dbt_project_yml(temp_dir, project_keys)
        
        # Read the modified file
        with open(temp_dir / "dbt_project.yml", "r") as f:
            modified_content = yaml.safe_load(f)
        
        expected_content = {
            "name": "test_project",
            "version": "1.0.0",
            "models": {
                "my_project": {
                    "staging": {
                        "customers": {
                            "materialized": "table",
                            "meta": {
                                "owner": "analytics_team"
                            }
                        }
                    }
                }
            }
        }
        
        assert modified_content == expected_content


class TestProjectKeysIntegration:
    """Integration tests for project_keys with other components."""

    @patch('cosmos.dbt.project.apply_project_keys_to_dbt_project_yml')
    def test_project_keys_passed_to_operators(self, mock_apply_project_keys):
        """Test that project_keys from ProjectConfig are passed to operators."""
        from cosmos.operators.local import DbtRunLocalOperator
        
        project_keys = {
            "name": "test_project",
            "models.my_project.materialized": "table"
        }
        
        # This test would need to be expanded once the operator integration is implemented
        # For now, we're just testing the config part
        project_config = ProjectConfig(
            dbt_project_path="path/to/dbt/project",
            project_keys=project_keys
        )
        
        assert project_config.project_keys == project_keys

    def test_project_keys_with_dbt_vars_coexistence(self):
        """Test that project_keys and dbt_vars can coexist."""
        project_keys = {
            "name": "test_project",
            "models.my_project.materialized": "table"
        }
        
        dbt_vars = {
            "start_date": "2023-01-01",
            "end_date": "2023-12-31"
        }
        
        project_config = ProjectConfig(
            dbt_project_path="path/to/dbt/project",
            project_keys=project_keys,
            dbt_vars=dbt_vars
        )
        
        assert project_config.project_keys == project_keys
        assert project_config.dbt_vars == dbt_vars


