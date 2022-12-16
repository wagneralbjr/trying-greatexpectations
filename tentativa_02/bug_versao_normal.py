import os
from pyspark.sql import SparkSession
from ruamel import yaml

import great_expectations as gx
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
    FilesystemStoreBackendDefaults,
)


import logging

logging.basicConfig(level=logging.ERROR)

spark = SparkSession.builder.appName("a").getOrCreate()
# spark = gx.core.util.get_or_create_spark_application()

# root_directory = " /home/wagner/Documents/trying-greatexpectations/data/enem.parquet"
# root_directory = os.path.join( os.getcwd() , "great_expectations" )


# data_context_config = DataContextConfig(
#     store_backend_defaults=FilesystemStoreBackendDefaults(
#         root_directory=root_directory
#     ),
# )
# context = BaseDataContext(project_config=data_context_config)
store_backend_defaults = InMemoryStoreBackendDefaults()
data_context_config = DataContextConfig(
    store_backend_defaults=store_backend_defaults,
    checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
)


context = BaseDataContext(project_config=data_context_config)

# aqui o contexto foi criado, configurado pra acessar da memória.


# file_path = os.path.join(
#     os.getcwd(),
#     "data/enem.parquet")

file_path = "/home/wagner/Documents/trying-greatexpectations/data/enem.parquet"


df = spark.read.format("parquet").load(file_path)

DATASOURCE_NAME = "enem_dataframe"

datasource_config = {
    "name": DATASOURCE_NAME,
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",
        "force_reuse_spark_context": "true",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["batch_id"],
        }
    },
}

context.test_yaml_config(yaml.dump(datasource_config))
context.add_datasource(**datasource_config)


batch_request = RuntimeBatchRequest(
    datasource_name=DATASOURCE_NAME,
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="enem_data_asset",  # This can be anything that identifies this data_asset for you
    batch_identifiers={"batch_id": "default_identifier"},
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# context.create_expectation_suite(
#     expectation_suite_name="test_suite", overwrite_existing=True
# )

validator = context.get_validator(
    batch_request= batch_request,
    create_expectation_suite_with_name="tryingout"
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
a = validator.head()
print(a)
