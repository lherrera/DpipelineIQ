from pyspark import pipelines as dp
from pyspark.sql.functions import col
from includes.publishing_confs import INT_NAME_PREFIX

target_table_name = INT_NAME_PREFIX+"usecases_with_prepared_context"
dp.create_streaming_table(target_table_name, comment="Filtered and preprocessed subset of use case data for pipelineIQ before AI inputs")

dp.create_auto_cdc_flow(
  target = target_table_name,
  source = "vw_usecases_with_prepared_context",
  keys = ["usecase_id"],
  sequence_by = col("last_modified_date"),
  stored_as_scd_type = 1
)
