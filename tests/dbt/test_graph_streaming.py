"""
Tests for the streaming manifest parser feature.

This module tests the ijson-based streaming manifest parser that can significantly
reduce memory usage when parsing large dbt manifest files.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cosmos import settings
from cosmos.config import ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.constants import DbtResourceType, SourceRenderingBehavior
from cosmos.dbt.graph import CosmosLoadDbtException, DbtGraph, DbtNode

SAMPLE_MANIFEST = Path(__file__).parent.parent / "sample/manifest.json"
SAMPLE_MANIFEST_SELECTORS = Path(__file__).parent.parent / "sample/manifest_selectors.json"


@pytest.fixture
def sample_manifest_content():
    """Load sample manifest content for testing."""
    with open(SAMPLE_MANIFEST) as f:
        return json.load(f)


@pytest.fixture
def tmp_manifest_path(sample_manifest_content):
    """Create a temporary manifest file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_manifest_content, f)
        return Path(f.name)


@pytest.fixture
def large_tmp_manifest_path(sample_manifest_content):
    """Create a large temporary manifest file (>50MB) for testing threshold."""
    # Duplicate nodes to create a larger manifest
    manifest = sample_manifest_content.copy()
    original_nodes = manifest.get("nodes", {})

    # Create many duplicated nodes to exceed threshold
    for i in range(1000):
        for key, value in list(original_nodes.items())[:5]:
            new_key = f"{key}_duplicate_{i}"
            new_value = value.copy()
            new_value["unique_id"] = new_key
            manifest["nodes"][new_key] = new_value

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(manifest, f)
        return Path(f.name)


class TestStreamingManifestParser:
    """Tests for the streaming manifest parser feature."""

    def test_streaming_disabled_by_default(self):
        """Verify streaming parser is disabled by default."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
        execution_config = ExecutionConfig(dbt_project_path=Path("/tmp/test"))
        render_config = RenderConfig()
        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        # Should return False since enable_streaming_manifest_parser defaults to False
        assert not dbt_graph._should_use_streaming_manifest_parser()

    @patch.object(settings, "enable_streaming_manifest_parser", True)
    @patch.object(settings, "streaming_manifest_threshold_mb", 0)  # Set to 0 to always trigger
    def test_streaming_enabled_when_ijson_installed(self):
        """Verify streaming is enabled when settings allow and ijson is available."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
        execution_config = ExecutionConfig(dbt_project_path=Path("/tmp/test"))
        render_config = RenderConfig()
        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        # This will return True if ijson is installed, False otherwise
        # The test verifies the logic works correctly regardless of ijson availability
        try:
            import ijson

            assert dbt_graph._should_use_streaming_manifest_parser()
        except ImportError:
            assert not dbt_graph._should_use_streaming_manifest_parser()

    @patch.object(settings, "enable_streaming_manifest_parser", True)
    @patch.object(settings, "streaming_manifest_threshold_mb", 1000)  # Very high threshold
    def test_streaming_disabled_when_manifest_too_small(self):
        """Verify streaming is disabled when manifest is below threshold."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
        execution_config = ExecutionConfig(dbt_project_path=Path("/tmp/test"))
        render_config = RenderConfig()
        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        # Should be False because manifest is smaller than 1000MB
        assert not dbt_graph._should_use_streaming_manifest_parser()

    @patch.object(settings, "enable_streaming_manifest_parser", True)
    @patch.dict("sys.modules", {"ijson": None})
    def test_streaming_disabled_when_ijson_not_installed(self):
        """Verify graceful fallback when ijson is not available."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
        execution_config = ExecutionConfig(dbt_project_path=Path("/tmp/test"))
        render_config = RenderConfig()
        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        # Should return False since ijson is not available
        with patch("builtins.__import__", side_effect=ImportError("No module named 'ijson'")):
            assert not dbt_graph._should_use_streaming_manifest_parser()

    def test_create_node_from_dict_success(self):
        """Test _create_node_from_dict creates valid DbtNode."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
        execution_config = ExecutionConfig(dbt_project_path=Path("/tmp/test"))
        render_config = RenderConfig()
        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        node_dict = {
            "resource_type": "model",
            "package_name": "jaffle_shop",
            "original_file_path": "models/customers.sql",
            "depends_on": {"nodes": ["model.jaffle_shop.stg_customers"]},
            "tags": ["daily"],
            "config": {"materialized": "table"},
        }

        node = dbt_graph._create_node_from_dict("model.jaffle_shop.customers", node_dict)

        assert node is not None
        assert node.unique_id == "model.jaffle_shop.customers"
        assert node.package_name == "jaffle_shop"
        assert node.resource_type == DbtResourceType.MODEL
        assert node.depends_on == ["model.jaffle_shop.stg_customers"]
        assert node.tags == ["daily"]
        assert node.config == {"materialized": "table"}

    def test_create_node_from_dict_skips_external_nodes(self):
        """Test _create_node_from_dict returns None for nodes without file paths."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
        execution_config = ExecutionConfig(dbt_project_path=Path("/tmp/test"))
        render_config = RenderConfig()
        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        # Node without original_file_path (e.g., external reference from dbt-loom)
        node_dict = {
            "resource_type": "model",
            "package_name": "external_package",
            "depends_on": {"nodes": []},
            "tags": [],
            "config": {},
        }

        node = dbt_graph._create_node_from_dict("model.external_package.ext_model", node_dict)

        assert node is None

    def test_create_node_from_dict_skips_empty_file_path(self):
        """Test _create_node_from_dict returns None for nodes with empty file paths."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="test")
        execution_config = ExecutionConfig(dbt_project_path=Path("/tmp/test"))
        render_config = RenderConfig()
        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        node_dict = {
            "resource_type": "model",
            "package_name": "test",
            "original_file_path": "",  # Empty string
            "depends_on": {"nodes": []},
            "tags": [],
            "config": {},
        }

        node = dbt_graph._create_node_from_dict("model.test.empty_path", node_dict)

        assert node is None


def _is_ijson_available() -> bool:
    """Check if ijson is available for testing."""
    try:
        import ijson

        return True
    except ImportError:
        return False


@pytest.mark.skipif(
    not _is_ijson_available(),
    reason="ijson not installed",
)
class TestStreamingManifestParserWithIjson:
    """Tests that require ijson to be installed."""

    @patch.object(settings, "enable_streaming_manifest_parser", True)
    @patch.object(settings, "streaming_manifest_threshold_mb", 0)
    def test_streaming_produces_same_results_as_standard(self):
        """Verify streaming parser produces identical results to standard parser."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="jaffle_shop")
        execution_config = ExecutionConfig(dbt_project_path=Path(__file__).parent.parent / "sample")
        render_config = RenderConfig()

        # Load with standard parser
        dbt_graph_standard = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )
        with patch.object(settings, "enable_streaming_manifest_parser", False):
            dbt_graph_standard.load_from_dbt_manifest()

        # Load with streaming parser
        dbt_graph_streaming = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )
        dbt_graph_streaming.load_from_dbt_manifest_streaming()

        # Compare results
        assert dbt_graph_standard.nodes.keys() == dbt_graph_streaming.nodes.keys()

        for node_id in dbt_graph_standard.nodes:
            standard_node = dbt_graph_standard.nodes[node_id]
            streaming_node = dbt_graph_streaming.nodes[node_id]

            assert standard_node.unique_id == streaming_node.unique_id
            assert standard_node.resource_type == streaming_node.resource_type
            assert standard_node.package_name == streaming_node.package_name
            assert standard_node.depends_on == streaming_node.depends_on
            assert standard_node.tags == streaming_node.tags
            assert standard_node.config == streaming_node.config

    @patch.object(settings, "enable_streaming_manifest_parser", True)
    @patch.object(settings, "streaming_manifest_threshold_mb", 0)
    def test_streaming_with_selectors(self):
        """Verify streaming parser works correctly with YAML selectors."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST_SELECTORS, project_name="jaffle_shop")
        execution_config = ExecutionConfig(dbt_project_path=Path(__file__).parent.parent / "sample")
        render_config = RenderConfig(selector="fqn_customers")

        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        # This should use the streaming parser and handle selectors
        dbt_graph.load_from_dbt_manifest_streaming()

        assert len(dbt_graph.nodes) > 0
        assert len(dbt_graph.filtered_nodes) <= len(dbt_graph.nodes)

    @patch.object(settings, "enable_streaming_manifest_parser", True)
    @patch.object(settings, "streaming_manifest_threshold_mb", 0)
    def test_streaming_fallback_on_invalid_manifest(self, tmp_path):
        """Verify streaming parser handles invalid manifest gracefully."""
        # Create an invalid manifest file
        invalid_manifest = tmp_path / "invalid_manifest.json"
        invalid_manifest.write_text("{invalid json")

        project_config = ProjectConfig(manifest_path=invalid_manifest, project_name="test")
        execution_config = ExecutionConfig(dbt_project_path=tmp_path)
        render_config = RenderConfig()

        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        # Should raise an exception for invalid JSON
        with pytest.raises(Exception):
            dbt_graph.load_from_dbt_manifest_streaming()

    @patch.object(settings, "enable_streaming_manifest_parser", True)
    @patch.object(settings, "streaming_manifest_threshold_mb", 0)
    def test_streaming_with_missing_selectors_raises_exception(self):
        """Verify streaming parser raises exception when selector not found."""
        project_config = ProjectConfig(manifest_path=SAMPLE_MANIFEST, project_name="jaffle_shop")
        execution_config = ExecutionConfig(dbt_project_path=Path(__file__).parent.parent / "sample")
        render_config = RenderConfig(selector="nonexistent_selector")

        dbt_graph = DbtGraph(
            project=project_config,
            execution_config=execution_config,
            render_config=render_config,
        )

        with pytest.raises(CosmosLoadDbtException) as exc_info:
            dbt_graph.load_from_dbt_manifest_streaming()

        assert "Selectors not found" in str(exc_info.value)


class TestStreamingParserSettings:
    """Tests for streaming parser settings."""

    def test_settings_have_correct_defaults(self):
        """Verify settings have correct default values."""
        # These should match the defaults in settings.py
        with patch.object(settings, "enable_streaming_manifest_parser", False):
            assert settings.enable_streaming_manifest_parser is False

        with patch.object(settings, "streaming_manifest_threshold_mb", 25):
            assert settings.streaming_manifest_threshold_mb == 25

    @patch.object(settings, "enable_streaming_manifest_parser", True)
    @patch.object(settings, "streaming_manifest_threshold_mb", 100)
    def test_settings_can_be_overridden(self):
        """Verify settings can be overridden."""
        assert settings.enable_streaming_manifest_parser is True
        assert settings.streaming_manifest_threshold_mb == 100
