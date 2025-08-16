from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import dbtlearn_dbt_assets
from .project import dbtlearn_project
from .schedules import schedules

defs = Definitions(
    assets=[dbtlearn_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=dbtlearn_project),
    },
)