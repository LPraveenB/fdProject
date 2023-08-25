from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from ghost_calc_pipelines.components.de_pipeline.src.helper.helper import Helper
import json
from urllib.parse import urlparse
import logging
import math
from google.cloud import storage
import re
from google.cloud import dataproc_v1 as dataproc
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator
)
from airflow.models import Variable


class E2EHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_e2e_validator_args(self) -> list:
        """

        Args:
            bucket_name:
            load_date:
            error_path:
            task_name:

        Returns:

        """

        pyspark_args = ['--bucket_name',
                        self.get_env_variable("dev-e2e-validator", "base_bucket", "base_path"),
                        '--load_date', '',
                        '--error_path',
                        self.get_env_variable("dev-e2e-validator", "error_path", "base_bucket", "base_path"),
                        '--merge_path',
                        self.get_env_variable("dev-e2e-validator", "merge_path", "base_bucket", "base_path"),

                        ]

        return pyspark_args

    def submit_dataproc_job(self, batch_id, context):

        batch_config = {
            "pyspark_batch": {
                "main_python_file_uri": self.get_env_variable("dev-e2e-validator", "main_python_file_uri"),
                "args": self.get_e2e_validator_args()
            },
            "runtime_config": {
                "properties": self.get_env_variable("dev-e2e-validator", "spark_properties")
            },
            "environment_config": {
                "execution_config": {
                    "service_account": self.get_env_variable("dev-env-config", "service_account"),
                    "subnetwork_uri": self.get_env_variable("dev-env-config", "subnetwork_uri")
                },
            }
        }

        print(" printing batch config ********* ")
        print(batch_config)

        run_batch = DataprocCreateBatchOperator(
            task_id="e2-evalidator" + batch_id,
            project_id="dollar-tree-project-369709",
            region="us-west1",
            batch=batch_config,
            batch_id="e2-evalidator-" + batch_id,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return run_batch.execute(context)

