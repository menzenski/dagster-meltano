import re
from dagster import JobDefinition, ScheduleDefinition

from dagster_meltano import load_jobs_from_meltano_project

from pathlib import Path

import os
import sys
import pytest
from dagster_meltano import (
    create_meltano_definitions,
    MeltanoResource
)

MELTANO_PROJECT_TEST_PATH = Path(__file__).parent / "meltano_test_project"
MELTANO_TEST_PROJECT_DIR = str(Path(__file__).parent / "meltano_test_project")
MELTANO_PROJECT_DIR = str(Path(__file__).parent.parent / "meltano_project")


def test_job_and_schedule_returned():
    """
    Check if the generator function correctly returns the jobs and schedules.
    """
    (job, schedule) = load_jobs_from_meltano_project(
        MELTANO_PROJECT_TEST_PATH,
    )

    assert isinstance(job, JobDefinition)
    assert isinstance(schedule, ScheduleDefinition)

    assert job.name == "smoke_job"
    assert schedule.name == "daily_smoke_job"


def test_job():
    """
    Run the job using Dagster and check for failures.
    """
    (job, _schedule) = load_jobs_from_meltano_project(MELTANO_PROJECT_TEST_PATH)

    job_response = job.execute_in_process()

    assert job_response.success
    assert (
        type(job_response.output_for_node("tap_smoke_test_target_jsonl", "logs")) == str
    )


def test_schedule():
    """
    Make sure the generated cron schedule has the correct interval
    """
    (_job, schedule) = load_jobs_from_meltano_project(MELTANO_PROJECT_TEST_PATH)

    assert schedule.cron_schedule == "0 0 * * *"


def test_load_jobs_from_simple_project():
    """
    Check if we can load jobs from a simple Meltano project.
    """
    jobs = load_jobs_from_meltano_project(
        meltano_project_dir=MELTANO_TEST_PROJECT_DIR
    )

    # Check if we have jobs
    assert len(jobs) > 0

    # Check if we have the right types
    for job in jobs:
        assert isinstance(job, (JobDefinition, ScheduleDefinition))


def test_create_meltano_definitions_from_simple_project():
    """
    Check if we can create Definitions from a simple Meltano project.
    """
    defs = create_meltano_definitions(
        meltano_project_dir=MELTANO_TEST_PROJECT_DIR,
        retries=1
    )
    
    # Check if we have the MeltanoResource
    assert defs.resources.get("meltano") is not None
    assert isinstance(defs.resources.get("meltano"), MeltanoResource)
    
    # Check if we have jobs
    assert len(defs.jobs) > 0
    
    # Check job properties
    for job_name, job in defs.jobs.items():
        assert isinstance(job, JobDefinition)
        assert job.resource_defs.get("meltano") is not None


def test_create_meltano_definitions_from_project_directory():
    """
    Check if we can create Definitions from the actual meltano_project directory.
    """
    # Skip if meltano_project directory doesn't exist
    if not Path(MELTANO_PROJECT_DIR).exists():
        pytest.skip(f"Meltano project directory {MELTANO_PROJECT_DIR} does not exist")
    
    defs = create_meltano_definitions(
        meltano_project_dir=MELTANO_PROJECT_DIR,
        retries=1
    )
    
    # Check if we have the MeltanoResource
    assert defs.resources.get("meltano") is not None
    assert isinstance(defs.resources.get("meltano"), MeltanoResource)
    
    # The project should have at least one job or schedule
    total_items = len(defs.jobs) + len(defs.schedules)
    assert total_items > 0, "Expected at least one job or schedule in the Meltano project"
