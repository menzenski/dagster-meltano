"""
Example of how to use dagster-meltano with the modern Definitions pattern.

This file demonstrates different ways to use dagster-meltano:
1. Using create_meltano_definitions to automatically create Definitions from a Meltano project
2. Manually creating Definitions with custom jobs that use the MeltanoResource
"""

import os
from pathlib import Path

from dagster import (
    Definitions,
    job,
    op,
    IOManager,
    io_manager,
    AssetKey,
    asset,
    with_resources,
)

from dagster_meltano import (
    MeltanoResource,
    meltano_run_op,
    meltano_command_op,
    create_meltano_definitions,
)

# Set up project paths
MELTANO_PROJECT_DIR = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())

# Example 1: Create Definitions directly from Meltano project
# This is the simplest way to use dagster-meltano
meltano_defs = create_meltano_definitions(
    meltano_project_dir=MELTANO_PROJECT_DIR,
    retries=1,
)

# Example 2: Create custom jobs using MeltanoResource
# This gives you more control over your pipeline

# Create a custom job that runs multiple Meltano commands in sequence
@job
def custom_meltano_pipeline():
    """A job that runs a sequence of Meltano commands."""
    # First install Meltano plugins
    install_result = meltano_command_op("install")()
    
    # Then run a tap-to-target ELT job
    tap_result = meltano_run_op("tap-csv target-jsonl")(after=install_result)
    
    # Then run another command that depends on the previous step
    meltano_run_op("tap-csv:sample target-jsonl")(after=tap_result)

# Example of adding environment variables to a job
@op
def get_env_vars():
    """Returns environment variables to inject into Meltano run."""
    return {
        "TAP_CSV_FILES": str(Path(MELTANO_PROJECT_DIR) / "data/*.csv"),
        "TARGET_JSONL_DESTINATION_PATH": str(Path(MELTANO_PROJECT_DIR) / "output"),
    }

@job
def custom_meltano_pipeline_with_env():
    """A job that runs Meltano with custom environment variables."""
    env_vars = get_env_vars()
    meltano_run_op("tap-csv target-jsonl")(env=env_vars)

# Create custom Definitions with our jobs
custom_defs = Definitions(
    jobs=[custom_meltano_pipeline, custom_meltano_pipeline_with_env],
    resources={
        "meltano": MeltanoResource(
            project_dir=MELTANO_PROJECT_DIR,
            retries=2,
        )
    },
)

# Example 3: Combining auto-loaded jobs with custom jobs
# Get jobs and schedules from Meltano project
defs_from_project = create_meltano_definitions(
    meltano_project_dir=MELTANO_PROJECT_DIR,
)

# Combine with custom jobs
combined_defs = Definitions(
    jobs=[custom_meltano_pipeline, custom_meltano_pipeline_with_env, *defs_from_project.jobs.values()],
    schedules=defs_from_project.schedules,
    resources={
        "meltano": MeltanoResource(
            project_dir=MELTANO_PROJECT_DIR,
            retries=1,
        )
    },
)

# Use this if you want to run this file directly with dagster
defs = combined_defs 