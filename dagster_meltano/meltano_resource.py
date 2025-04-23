import asyncio
import json
import logging
import os
from functools import cached_property
from pathlib import Path
from typing import Any, Union, Generator, Dict, List

import dagster as dg
from pydantic import Field

from dagster_meltano.exceptions import MeltanoCommandError
from dagster_meltano.job import Job
from dagster_meltano.schedule import Schedule
from dagster_meltano.utils import Singleton


class MeltanoResource(dg.ConfigurableResource, metaclass=Singleton):
    """A resource that corresponds to a Meltano project."""
    
    project_dir: str = Field(
        default_factory=lambda: os.getenv("MELTANO_PROJECT_ROOT", os.getcwd()),
        description="The path to the Meltano project."
    )
    meltano_bin: str = Field(
        default="meltano", 
        description="The path to the Meltano binary."
    )
    retries: int = Field(
        default=0, 
        description="The number of times to retry a failed job."
    )
    
    @property
    def default_env(self) -> Dict[str, str]:
        """The default environment to use when running Meltano commands.

        Returns:
            Dict[str, str]: The environment variables.
        """
        return {
            "MELTANO_CLI_LOG_CONFIG": str(Path(__file__).parent / "logging.yaml"),
            "DBT_USE_COLORS": "false",
            "NO_COLOR": "1",
            **os.environ.copy(),
        }

    def execute_command(
        self,
        command: str,
        env: Dict[str, str],
        logger: Union[logging.Logger, dg.DagsterLogManager, None] = None,
    ) -> str:
        """Execute a Meltano command.

        Args:
            command: The Meltano command to execute.
            env: The environment variables to inject when executing the command.
            logger: The logger to use.

        Returns:
            str: The output of the command.
        """
        if logger is None:
            logger = dg.get_dagster_logger()
            
        full_command = f"{self.meltano_bin} {command}"
        merged_env = {**self.default_env, **env}
        
        logger.info(f"Executing command: {full_command}")

        client: dg.PipesSubprocessClient = dg.PipesSubprocessClient(
            full_command,
            env=merged_env,
            cwd=self.project_dir,
            write_unicode_logs=True,
        )
        
        output = []
        for line in client.get_output_lines():
            output.append(line)
            logger.info(line)
            
        exit_code = client.wait()
        
        if exit_code != 0:
            raise MeltanoCommandError(
                f"Command '{command}' failed with exit code {exit_code}"
            )

        return "\n".join(output)

    async def load_json_from_cli(self, command: List[str]) -> Dict[str, Any]:
        """Use the Meltano CLI to load JSON data.
        Use asyncio to run multiple commands concurrently.

        Args:
            command: The Meltano command to execute.

        Returns:
            Dict[str, Any]: The processed JSON data.
        """
        # Create the subprocess, redirect the standard output into a pipe
        proc = await asyncio.create_subprocess_exec(
            self.meltano_bin,
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.project_dir,
        )

        # Wait for the subprocess to finish
        stdout, stderr = await proc.communicate()

        # Try to load the output as JSON
        try:
            return json.loads(stdout)
        except json.decoder.JSONDecodeError:
            raise ValueError(f"Could not process json: {stdout} {stderr}")

    async def gather_meltano_yaml_information(self):
        jobs, schedules = await asyncio.gather(
            self.load_json_from_cli(["job", "list", "--format=json"]),
            self.load_json_from_cli(["schedule", "list", "--format=json"]),
        )

        return jobs, schedules

    @cached_property
    def meltano_yaml(self) -> Dict[str, Any]:
        """Asynchronously load the Meltano jobs and schedules.

        Returns:
            Dict[str, Any]: The Meltano jobs and schedules.
        """
        jobs, schedules = asyncio.run(self.gather_meltano_yaml_information())
        return {"jobs": jobs["jobs"], "schedules": schedules["schedules"]}

    @cached_property
    def meltano_jobs(self) -> List[Job]:
        meltano_job_list = self.meltano_yaml["jobs"]
        return [
            Job(
                meltano_job=meltano_job,
                retries=self.retries,
            )
            for meltano_job in meltano_job_list
        ]

    @cached_property
    def meltano_schedules(self) -> List[Schedule]:
        meltano_schedule_list = self.meltano_yaml["schedules"]["job"]
        schedule_list: List[Schedule] = [
            Schedule(meltano_schedule) for meltano_schedule in meltano_schedule_list
        ]
        return schedule_list

    @property
    def meltano_job_schedules(self) -> Dict[str, Schedule]:
        return {schedule.job_name: schedule for schedule in self.meltano_schedules}

    @property
    def jobs(self) -> Generator[Dict[str, Any], None, None]:
        for meltano_job in self.meltano_jobs:
            yield meltano_job.dagster_job

        for meltano_schedule in self.meltano_schedules:
            yield meltano_schedule.dagster_schedule


# Legacy resource definition for backward compatibility
@dg.resource(
    description="A resource that corresponds to a Meltano project.",
    config_schema={
        "project_dir": Field(
            str,
            description="The path to the Meltano project.",
            default_value=os.getenv("MELTANO_PROJECT_ROOT", os.getcwd()),
            is_required=False,
        ),
        "retries": Field(
            int,
            description="The number of times to retry a failed job.",
            default_value=0,
            is_required=False,
        ),
    },
)
def meltano_resource(init_context):
    project_dir = init_context.resource_config["project_dir"]
    retries = init_context.resource_config["retries"]

    return MeltanoResource(
        project_dir=project_dir,
        retries=retries,
    )


if __name__ == "__main__":
    meltano_resource = MeltanoResource("/workspace/meltano")
    print(list(meltano_resource.jobs))
    print(meltano_resource.jobs)
