import os
from pathlib import Path
import pytest
from unittest.mock import patch, MagicMock

from dagster import PipesSubprocessClient
from dagster_meltano import MeltanoResource
from dagster_meltano.exceptions import MeltanoCommandError

MELTANO_PROJECT_TEST_PATH = str(Path(__file__).parent / "meltano_test_project")


def test_execute_command_success():
    """Test that execute_command works with PipesSubprocessClient for successful commands."""
    resource = MeltanoResource(project_dir=MELTANO_PROJECT_TEST_PATH)
    
    # Mock the PipesSubprocessClient
    with patch('dagster._utils.subprocess_utils.PipesSubprocessClient') as mock_client_class:
        # Setup the mock
        mock_client = MagicMock()
        mock_client.get_output_lines.return_value = ["line1", "line2", "line3"]
        mock_client.wait.return_value = 0  # Success exit code
        mock_client_class.return_value = mock_client
        
        # Call the method
        output = resource.execute_command("test command", {})
        
        # Verify the client was created correctly
        mock_client_class.assert_called_once()
        
        # Verify the output was processed correctly
        assert output == "line1\nline2\nline3"
        
        # Verify get_output_lines was called
        mock_client.get_output_lines.assert_called_once()
        
        # Verify wait was called
        mock_client.wait.assert_called_once()


def test_execute_command_failure():
    """Test that execute_command raises MeltanoCommandError for failed commands."""
    resource = MeltanoResource(project_dir=MELTANO_PROJECT_TEST_PATH)
    
    # Mock the PipesSubprocessClient
    with patch('dagster._utils.subprocess_utils.PipesSubprocessClient') as mock_client_class:
        # Setup the mock
        mock_client = MagicMock()
        mock_client.get_output_lines.return_value = ["error line 1", "error line 2"]
        mock_client.wait.return_value = 1  # Error exit code
        mock_client_class.return_value = mock_client
        
        # Call the method and expect an exception
        with pytest.raises(MeltanoCommandError) as excinfo:
            resource.execute_command("failing command", {})
        
        # Verify the exception message
        assert "Command 'failing command' failed with exit code 1" in str(excinfo.value)
        
        # Verify the client was created correctly
        mock_client_class.assert_called_once()
        
        # Verify get_output_lines was called
        mock_client.get_output_lines.assert_called_once()
        
        # Verify wait was called
        mock_client.wait.assert_called_once()


def test_environment_variables():
    """Test that environment variables are correctly passed to PipesSubprocessClient."""
    resource = MeltanoResource(project_dir=MELTANO_PROJECT_TEST_PATH)
    
    # Create test environment
    test_env = {"TEST_VAR": "test_value", "ANOTHER_VAR": "another_value"}
    
    # Mock the PipesSubprocessClient
    with patch('dagster._utils.subprocess_utils.PipesSubprocessClient') as mock_client_class:
        # Setup the mock
        mock_client = MagicMock()
        mock_client.get_output_lines.return_value = []
        mock_client.wait.return_value = 0
        mock_client_class.return_value = mock_client
        
        # Call the method
        resource.execute_command("test command", test_env)
        
        # Get the args passed to the constructor
        args, kwargs = mock_client_class.call_args
        
        # Verify environment variables were merged correctly
        env = kwargs.get('env', {})
        assert env.get("TEST_VAR") == "test_value"
        assert env.get("ANOTHER_VAR") == "another_value"
        
        # Verify default environment variables are included
        assert "MELTANO_CLI_LOG_CONFIG" in env
        assert "DBT_USE_COLORS" in env
        assert env.get("DBT_USE_COLORS") == "false"
        assert env.get("NO_COLOR") == "1"


def test_execute_command_with_logger():
    """Test that execute_command correctly logs output with the provided logger."""
    resource = MeltanoResource(project_dir=MELTANO_PROJECT_TEST_PATH)
    
    # Create a mock logger
    mock_logger = MagicMock()
    
    # Mock the PipesSubprocessClient
    with patch('dagster._utils.subprocess_utils.PipesSubprocessClient') as mock_client_class:
        # Setup the mock
        mock_client = MagicMock()
        mock_client.get_output_lines.return_value = ["line1", "line2"]
        mock_client.wait.return_value = 0
        mock_client_class.return_value = mock_client
        
        # Call the method with the logger
        resource.execute_command("test command", {}, logger=mock_logger)
        
        # Verify logger.info was called for each line
        assert mock_logger.info.call_count >= 3  # Command + 2 output lines
        
        # Verify it was called with each line
        mock_logger.info.assert_any_call("line1")
        mock_logger.info.assert_any_call("line2") 