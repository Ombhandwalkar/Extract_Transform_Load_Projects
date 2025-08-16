from pathlib import Path

from dagster_dbt import DbtProject

dbtlearn_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "dbtlearn").resolve(),
   # packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
    profiles_dir="C:/Users/ombha/.dbt" 
)
dbtlearn_project.prepare_if_dev()