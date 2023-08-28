from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from ghost_calc_pipelines.components.de_pipeline.src.helper.helper import Helper
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.models import Variable


class E2EHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_e2e_validator_args(self) -> list:
        """

        Args:


        Returns:

        """

        pyspark_args = ['--bucket_name',
                        self.get_env_variable("dev-e2e-validator", "base_bucket", "base_path"),
                        '--load_date',
                        Variable.get(key="run_date"),
                        '--error_path',
                        self.get_env_variable("dev-e2e-validator", "error_path", "base_bucket", "base_path"),
                        '--merge_path',
                        self.get_env_variable("dev-e2e-validator", "merge_path", "base_bucket", "base_path"),

                        ]

        return pyspark_args

    def submit_manual_dataproc(self, context):
        cluster_create_task = DataprocCreateClusterOperator(
            task_id="e2e-validator",
            project_id="dollar-tree-project-369709",
            region="us-west1",
            cluster_name="e2e-validator-cluster",
            num_workers=2,  # Specify the number of worker nodes
            worker_machine_type="n1-standard-4",  # Specify the machine type

        )

        spark_job_task = DataprocSubmitJobOperator(
            task_id="run_e2e_job",
            main="gs://vertex-scripts/de-scripts/e2e-validator/e2eValidationComponent.py",
            project_id="dollar-tree-project-369709",
            region="us-west1",
            cluster_name="e2e-validator-cluster",
            arguments=self.get_e2e_validator_args()
        )

        cluster_delete_task = DataprocDeleteClusterOperator(
            task_id="delete_dataproc_cluster",
            project_id="dollar-tree-project-369709",
            region="us-west1",
            cluster_name="e2e-validator-cluster"
        )

    # Define the task dependencies
        cluster_create_task >> spark_job_task >> cluster_delete_task

    context = {Variable.get(key="run_date")}
# Call the function to run the Spark job dynamically
    submit_manual_dataproc(context)
