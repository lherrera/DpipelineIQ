"""
Minimal configuration module for PIQ schema paths from Spark configs.
"""

from dataclasses import dataclass
from pyspark.sql import SparkSession
from typing import Literal

@dataclass
class PIQSchemaConfigs:
    """Schema configuration for PIQ internal and output targets."""
    internal_target: str
    outputs_target: str


def _load_from_spark() -> PIQSchemaConfigs:
    """Load schema configs from Spark configuration."""
    spark = SparkSession.getActiveSession()
    
    if spark is None:
        raise RuntimeError("No active Spark session found. Please initialize Spark first.")
    
    internal_target = spark.conf.get('piq_schemas.internal_target')
    outputs_target = spark.conf.get('piq_schemas.output_target')


    assert internal_target and outputs_target, "Both internal_target and outputs_target must be provided."
    return PIQSchemaConfigs(
        internal_target=internal_target,
        outputs_target=outputs_target
    )

# Import this
# Todo - when we have more than one schema to use, we can separate internal operation tables from the outputs used for dashboarding.
# For now, i use this prefix workaround
piq_configs = _load_from_spark()
INT_NAME_PREFIX = piq_configs.internal_target + '__'
OUT_NAME_PREFIX = piq_configs.outputs_target + '__'