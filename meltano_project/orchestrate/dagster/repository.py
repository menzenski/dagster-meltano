import os

from dagster import Definitions

from dagster_meltano import create_meltano_definitions

MELTANO_PROJECT_DIR = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())
MELTANO_BIN = os.getenv("MELTANO_BIN", "meltano")

# Create Definitions with all Meltano jobs and schedules
defs = create_meltano_definitions(
    meltano_project_dir=MELTANO_PROJECT_DIR,
    retries=1,
)
