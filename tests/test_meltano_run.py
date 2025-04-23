import os
import subprocess
import pytest
from dagster import job, op, Definitions

from dagster_meltano import (
    meltano_resource, 
    meltano_run_op, 
    MeltanoResource,
    create_meltano_definitions
)

from pathlib import Path

from dagster_meltano.exceptions import MeltanoCommandError

MELTANO_PROJECT_TEST_PATH = str(Path(__file__).parent / "meltano_test_project")


@job(resource_defs={"meltano": meltano_resource})
def meltano_run_job():
    meltano_run_op("tap-smoke-test target-jsonl")()


@job(resource_defs={"meltano": meltano_resource})
def meltano_run_job_with_env_op():
    @op
    def inject_env():
        return {"MELTANO_ENVIRONMENT": "non_existing_env"}

    injected_env = inject_env()
    meltano_run_op("tap-smoke-test target-jsonl")(env=injected_env)


# Modern Definitions-based pattern
def create_test_definitions():
    @job
    def modern_meltano_run_job():
        meltano_run_op("tap-smoke-test target-jsonl")()
    
    return Definitions(
        jobs=[modern_meltano_run_job],
        resources={
            "meltano": MeltanoResource(project_dir=MELTANO_PROJECT_TEST_PATH)
        }
    )


def test_meltano_run():
    """
    Check if we can run abitrary `meltano run` commands.
    """
    job_response = meltano_run_job.execute_in_process(
        {
            "resources": {
                "meltano": {
                    "config": {
                        "project_dir": MELTANO_PROJECT_TEST_PATH,
                    },
                }
            }
        }
    )

    assert job_response.success


def test_meltano_run_using_env_root():
    """
    Check if we can run abitrary `meltano run` commands, with the project
    root defined using an env variable.
    """
    os.environ["MELTANO_PROJECT_ROOT"] = MELTANO_PROJECT_TEST_PATH
    job_response = meltano_run_job.execute_in_process()

    assert job_response.success


def test_meltano_run_with_definitions():
    """
    Check if we can run Meltano commands using the modern Definitions pattern.
    """
    defs = create_test_definitions()
    job_response = defs.get_job("modern_meltano_run_job").execute_in_process()
    
    assert job_response.success


def test_create_meltano_definitions():
    """
    Check if we can create Definitions from a Meltano project.
    """
    defs = create_meltano_definitions(meltano_project_dir=MELTANO_PROJECT_TEST_PATH)
    
    # Assert we have definitions with jobs and resources
    assert defs.resources.get("meltano") is not None
    
    # Jobs are loaded from the Meltano project
    assert len(defs.jobs) > 0


def test_meltano_run_injecting_env():
    """
    Check if we can inject environment variables into the `meltano run` command.
    We test this by injecting a non existing Meltano environment, which should
    cause the command to fail.
    """
    with pytest.raises(MeltanoCommandError):
        meltano_run_job.execute_in_process(
            {
                "resources": {
                    "meltano": {
                        "config": {
                            "project_dir": MELTANO_PROJECT_TEST_PATH,
                        },
                    }
                },
                "ops": {
                    "tap_smoke_test_target_jsonl": {
                        "config": {
                            "env": {
                                "MELTANO_ENVIRONMENT": "non_existing_env",
                            }
                        }
                    }
                },
            }
        )


def test_meltano_run_injecting_env_input():
    """
    Check if we can inject environment variables from the op input into the `meltano run` command.
    We test this by injecting a non existing Meltano environment, which should
    cause the command to fail.
    """
    with pytest.raises(MeltanoCommandError):
        meltano_run_job.execute_in_process(
            {
                "resources": {
                    "meltano": {
                        "config": {
                            "project_dir": MELTANO_PROJECT_TEST_PATH,
                        },
                    }
                },
                "ops": {
                    "tap_smoke_test_target_jsonl": {
                        "inputs": {
                            "env": {
                                "MELTANO_ENVIRONMENT": "non_existing_env",
                            }
                        }
                    }
                },
            }
        )


def test_meltano_run_injecting_env_using_op():
    """
    Check if we can inject environment variables from an op into the `meltano run` command.
    We test this by injecting a non existing Meltano environment, which should
    cause the command to fail.
    """
    with pytest.raises(MeltanoCommandError):
        meltano_run_job_with_env_op.execute_in_process(
            {
                "resources": {
                    "meltano": {
                        "config": {
                            "project_dir": MELTANO_PROJECT_TEST_PATH,
                        },
                    }
                },
            }
        )
