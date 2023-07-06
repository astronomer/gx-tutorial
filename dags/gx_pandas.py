"""
## Run a Great Expectations validation suite on a pandas DataFrame

This DAG runs the Great Expectations validation suite `strawberry_suite` 
located at `include/great_expectations/expectations/strawberry_suite.json`
on a pandas DataFrame.
"""

from pendulum import datetime
from airflow.decorators import dag
import pandas as pd
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
)
def gx_pandas():
    GreatExpectationsOperator(
        task_id="gx_validate_pg",
        data_context_root_dir="include/great_expectations",
        data_asset_name="strawberries",
        dataframe_to_validate=pd.DataFrame(
            {
                "id": ["001", "002", "003", "004", "005"],
                "name": [
                    "Strawberry Order 1",
                    "Strawberry Order 2",
                    "Strawberry Order 3",
                    "Strawberry Order 4",
                    "Strawberry Order 5",
                ],
                "amount": [10, 5, 8, 3, 12],
            }
        ),
        execution_engine="PandasExecutionEngine",
        expectation_suite_name="strawberry_suite",
        return_json_dict=True,
    )


gx_pandas()
