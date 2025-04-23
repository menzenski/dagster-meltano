from typing import Dict, List, Optional, Union, Any

import dagster as dg

from dagster_meltano.meltano_resource import MeltanoResource


def load_jobs_from_meltano_project(
    meltano_project_dir: Optional[str] = None,
    retries: int = 0,
) -> List[Union[dg.JobDefinition, dg.ScheduleDefinition]]:
    """Generate dagster jobs and schedules for all jobs and schedules defined in the Meltano project.

    Args:
        meltano_project_dir: The location of the Meltano project. Defaults to os.getenv("MELTANO_PROJECT_ROOT").
        retries: The number of retries to attempt if the Meltano CLI fails to run. Defaults to 0.

    Returns:
        List[Union[dg.JobDefinition, dg.ScheduleDefinition]]: Returns a list of either Dagster JobDefinitions or ScheduleDefinitions
    """
    meltano_resource = MeltanoResource(
        project_dir=meltano_project_dir,
        meltano_bin="meltano",
        retries=retries,
    )

    meltano_jobs = meltano_resource.jobs

    return list(meltano_jobs)


def create_meltano_definitions(
    meltano_project_dir: Optional[str] = None,
    retries: int = 0,
    resource_defs: Optional[Dict[str, Any]] = None,
) -> dg.Definitions:
    """Creates a Dagster Definitions object from a Meltano project.
    
    Args:
        meltano_project_dir: The location of the Meltano project. Defaults to os.getenv("MELTANO_PROJECT_ROOT").
        retries: The number of retries to attempt if the Meltano CLI fails to run. Defaults to 0.
        resource_defs: Additional resource definitions to include in the Definitions object.
        
    Returns:
        dg.Definitions: A Dagster Definitions object containing all jobs and schedules from the Meltano project.
    """
    jobs_and_schedules = load_jobs_from_meltano_project(
        meltano_project_dir=meltano_project_dir,
        retries=retries,
    )
    
    # Separate jobs and schedules
    jobs = [item for item in jobs_and_schedules if isinstance(item, dg.JobDefinition)]
    schedules = [item for item in jobs_and_schedules if isinstance(item, dg.ScheduleDefinition)]
    
    # Include MeltanoResource in resource_defs
    all_resources = {
        "meltano": MeltanoResource(
            project_dir=meltano_project_dir,
            retries=retries,
        )
    }
    
    # Add any additional resources
    if resource_defs:
        all_resources.update(resource_defs)
    
    return dg.Definitions(
        jobs=jobs,
        schedules=schedules,
        resources=all_resources,
    )


def load_assets_from_meltano_project(
    meltano_project_dir: str,
) -> List[dg.AssetsDefinition]:
    """This function generates all Assets it can find in the supplied Meltano project.
    This currently includes the taps and dbt assets.

    Args:
        meltano_project_dir: The location of the Meltano project.

    Returns:
        List[AssetsDefinition]: Returns a list of all Meltano assets
    """
    # meltano_resource = MeltanoResource(meltano_project_dir)
    meltano_assets: List[dg.AssetsDefinition] = []
    # meltano_assets = [extractor.asset for extractor in meltano_resource.extractors]

    # if dbt_project_dir:
    #     dbt_assets = load_assets_from_dbt_project(
    #         project_dir=dbt_project_dir,
    #         profiles_dir=dbt_profiles_dir,
    #         target_dir=dbt_target_dir,
    #         use_build_command=dbt_use_build_command,
    #         node_info_to_group_fn=generate_dbt_group_name,
    #     )
    #     meltano_assets += dbt_assets

    return meltano_assets


if __name__ == "__main__":
    load_jobs_from_meltano_project("/workspace/meltano")
