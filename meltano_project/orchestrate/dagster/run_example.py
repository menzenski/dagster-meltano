#!/usr/bin/env python3
"""
Example script to run Meltano jobs using the Dagster CLI.

This script demonstrates how to execute jobs from the example.py file
using the Dagster Python API.
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from dagster import execute_job

from orchestrate.dagster.example import (
    custom_meltano_pipeline,
    custom_meltano_pipeline_with_env,
    meltano_defs,
)

def run_custom_pipeline():
    """Run the custom Meltano pipeline."""
    print("Running custom_meltano_pipeline...")
    result = execute_job(
        custom_meltano_pipeline,
        resources={
            "meltano": {
                "project_dir": str(project_root),
            }
        }
    )
    print(f"Pipeline result: {result.success}")
    return result


def run_custom_pipeline_with_env():
    """Run the custom Meltano pipeline with environment variables."""
    print("Running custom_meltano_pipeline_with_env...")
    result = execute_job(
        custom_meltano_pipeline_with_env,
        resources={
            "meltano": {
                "project_dir": str(project_root),
            }
        }
    )
    print(f"Pipeline result: {result.success}")
    return result


def run_meltano_job(job_name):
    """Run a specific Meltano job from the definitions."""
    print(f"Running Meltano job: {job_name}...")
    job = meltano_defs.get_job(job_name)
    if not job:
        print(f"Job '{job_name}' not found in Meltano definitions.")
        return None
    
    result = execute_job(job)
    print(f"Job '{job_name}' result: {result.success}")
    return result


if __name__ == "__main__":
    # Get job name from command line arguments
    if len(sys.argv) > 1:
        job_name = sys.argv[1]
        if job_name == "custom":
            run_custom_pipeline()
        elif job_name == "custom_env":
            run_custom_pipeline_with_env()
        else:
            run_meltano_job(job_name)
    else:
        print("Available options:")
        print("  custom - Run the custom Meltano pipeline")
        print("  custom_env - Run the custom pipeline with environment variables")
        print("  <job_name> - Run a specific Meltano job")
        
        # List available Meltano jobs
        print("\nAvailable Meltano jobs:")
        for job_name in meltano_defs.jobs:
            print(f"  {job_name}") 