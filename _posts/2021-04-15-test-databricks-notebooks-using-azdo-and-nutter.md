---
layout: post
title:  "Test Databricks Notebooks Using Azure Devops and Nutter"
date:   2021-04-15 11:07:24 +0530
categories: jekyll update
---

This article covers how we performed end-to-end integration testing in a recent project that involved Data Engineering and Data Science notebooks on Databricks. To begin with, let's walk through the development approach that was followed.

## Development approach used for Data Engineering

### VSCode and Dev Containers

VSCode development containers are used to write reusable business logic such as common data validations, data transformations etc in python. Code is then packaged as wheel package and installed as Databricks notebook-scoped library. Python packages are unit tested locally.

### Databricks Notebooks

Databricks notebooks are used to orchestrate data engineering code as a whole. The orchestration code performs following tasks:

- Install the python package developed in the step above as a Databricks notebook-scoped library. When you install a notebook-scoped library, only the current notebook and any jobs associated with that notebook have access to that library. Other notebooks attached to the same cluster are not affected.
- Detect incremental files in ADLS Gen2 using Auto Loader feature of Databricks.
- Read data from the detected incremental files in ADLS Gen2
- Transform data using the business logic present in the python package installed as notebook-scoped library
- Save transformed data to ADLS Gen2

The orchestration code cannot be unit tested locally since it requires access to data in ADLS Gen2 and uses Databricks specific features such as Auto Loader to detect incremental files. Instead, integration testing of Databricks notebooks are performed using [Nutter](https://github.com/microsoft/nutter).

## End-to-end Integration Testing

End-to-end integration testing of Data Engineering and Data science notebooks are performed using [Nutter framework](https://github.com/microsoft/nutter) using an Azure Devops pipeline. The integration test pipeline performs all the pre requisites for integration testing such as copy notebooks to Databricks workspace, copy configuration files to DBFS, set Databricks cluster environment variables, create MLflow experiments on Databricks etc before running the Nutter test notebook.

End-to-end integration testing uses the following:

- A [Nutter](https://github.com/microsoft/nutter) test notebook in Databricks workspace that runs Data Engineering and Data Science pipelines and performs assertion on the results.
- An integration test configuration file template that contains input parameters for Data Engineering and Data Science Databricks notebooks, configurations for integration testing such as test directory name, test data path in ADLS Gen2 etc. The template can be updated with variables created during runtime (such as MLflow experiment ID) to create a final integration test configuration file which will be used by the Nutter test notebook.
- Azure Devops variable group that contains Azure environment specific variables such as Databricks cluster ID, host, token, workspace path, DBFS path etc.
- Azure Key Vault can also be used to store secrets such as Databricks token, Personal Access Token to publish and consume Python packages to and from Azure Artifact Feed etc. The secrets are retrieved in the Azure Devops pipeline using the `AzureKeyVault@1` task.

## Nutter Test Notebook

Nutter framework is used to test Databricks notebooks. A test notebook "test_e2e_integration.py" is created in Databricks. The test notebook contains the following sections:
- Install nutter as a notebook-scoped library

```python
# MAGIC %pip install nutter
```

- Import NutterFixture base class in test notebook and implement it using a test fixture.

```python
from runtime.nutterfixture import NutterFixture
class TestE2EIntegration(NutterFixture):
    ...
```

- before_all(): To perform multiple assertions, a before_all() method is implemented without a run() method. In this pattern, the before_all method runs both the Data Engineering as well Data Science notebooks. There are no run methods.
- assertion_<testname>(): The assertion methods simply assert against what was done in before_all. We can have an assertion method for Data Engineering and Data Science each.

```python
# COMMAND ----------


from runtime.nutterfixture import NutterFixture

class TestE2EIntegration(NutterFixture):
    """Define test suite for end to end integration testing."""

    def __init__(self, data_engineering_params, data_science_params):
        """Initialize TestE2EIntegration."""
        self.data_engineering_params = data_engineering_params.copy()
        self.data_science_params = data_science_params.copy()
        super().__init__()
    
    def before_all(self):
        """Arrange: Setup test directory structure and data."""

        # Drop and create test directory
        dbutils.fs.rm(self.data_engineering_params["test_directory"], True)
        dbutils.fs.mkdirs(self.data_engineering_params["test_directory"])

        # Copy test data for all datasets
        dbutils.fs.cp(self.data_engineering_params["test_data_path"], self.data_engineering_params["test_directory"], recurse=True)

        # Run Data Engineering notebooks
        print("Running Data Engineering notebooks")
        dbutils.notebook.run(self.data_engineering_params["notebook"], 0, self.data_engineering_params["input_params"])

         # Run Data Science notebooks
        print("Running Data Science notebooks")
        self.result = dbutils.notebook.run(self.data_science_params["notebook"], 0, self.data_science_params["input_params"])
        result = json.loads(self.result)
        self.mlflow_run_id = result["mlflow_run_id"]

    def assertion_data_engineering(self):
        """Assert data engineering logic."""

    def assertion_data_science(self):
        """Assert data science logic."""

# COMMAND ----------

integration_test_config_path = Path("/dbfs/FileStore/libraries/configs/integration_test_config.yaml")

try:
    with integration_test_config_path.open("r") as stream:
        config = yaml.safe_load(stream)
        data_engineering_params = config["data_engineering"]
        data_science_params = config["data_science"]

    e2e_test = TestE2EIntegration(data_engineering_params, data_science_params)
    # Run tests
    result = e2e_test.execute_tests()
    # Return test results to the Nutter CLI
    result.exit(dbutils)

except Exception:
    dbutils.fs.rm(data_engineering_params["test_directory"], True)
    assert False
```

## End-to-end Integration Test Pipeline

The end-to-end integration test Azure Devops pipeline performs the following activities:

- Create a build version for the python package using Build ID of the pipeline. The python wheel package will be generated using this version.

```bash
variables:
  - group: integration-test
  - name: packageVersion
    value: 0.1.0-dev.$(Build.BuildId)
```

- Run linting and unit tests for the reusable code in python package.
- Publish code coverage and unit test results.
- Build wheel package using the version generated in step 1.
- Publish Data engineering and Data Science Databricks notebooks, corresponding configuration files and python package to Artifact Feed.

> The dependencies for the python package are also downloaded to Artifact Feed. This is not visible during the first time upload of the python package. However, when the python package is consumed from the Artifact Feed, the dependencies are automatically downloaded to the Artifact Feed. From here on, anytime the python package is consumed from the Artifact Feed, all the dependencies are downloaded from Azure Artifact python feed rather than the public pypi server.

- Download published artifact from Artifact Feed.
- Deploy Data Engineering, Data Science and Nutter Test notebooks to Databricks workspace

```bash
databricks workspace import_dir --overwrite "${source_dir}" "${destination_dir}"
```

or

```bash
databricks workspace import --language PYTHON  --overwrite "${source_file}" "${destination_file}"
```

- Deploy configuration files to DBFS

```bash
databricks fs cp --overwrite "${source_file}" "${destination_file}"
```

- Create MLflow experiment for integration testing

```python
import mlflow
experiment_path = databricks_workspace_path + "/" + experiment_name
experiment = mlflow.get_experiment_by_name(experiment_path)
if experiment is None:
    experiment_id = mlflow.create_experiment(experiment_path)
    experiment = mlflow.get_experiment(experiment_id)
    print("Mlflow experiment created successfully")
else:
    print("Mlflow experiment already exists")
```

> The MLflow experiment ID generated is injected into the integration test configuration template to generate the final configuration file.

```bash
sed "s@\$experimentIDPlaceholder@$mlflowExperimentID@g" integration_tests/integration_test_config.template.yaml > integration_tests/integration_test_config.yaml
```

- Add environment variables (if required) on the Databricks cluster used for integration testing.

```bash
updated_cluster_info=$(databricks clusters get --cluster-id "$dataBricksClusterId"| \
        jq --arg app_insights_key "$instrumentation_key" \
        --arg feed_url "$feed_index_url" \
        -c '.spark_env_vars += {"APPINSIGHTS_INSTRUMENTATION_KEY":  $app_insights_key, "PIP_INDEX_URL": $feed_url}')
databricks clusters edit --json "$updated_cluster_info"
```

- If test data exists in source directory, copy test data to ADLS Gen2 test directory.

- Install nutter and run integration test notebook through nutter CLI

```bash
- bash: |
    # Install nutter
    pip install nutter

    # Execute Nutter test notebook
    nutter run $DATABRICKS_WORKSPACE_BASE_PATH/tests/ $DATABRICKS_CLUSTER_ID --recursive --junit_report --timeout 7200
  displayName: Run Integration Tests
  env:
      DATABRICKS_HOST: $(databricksHost)
      DATABRICKS_TOKEN: $(databricksToken)
      DATABRICKS_CLUSTER_ID: $(databricksClusterId)
      DATABRICKS_WORKSPACE_BASE_PATH: ${{ parameters.workspaceBasePath }}
```

- Publish integration test results
