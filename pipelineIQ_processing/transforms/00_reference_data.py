from pyspark import pipelines as dp
from includes.publishing_confs import INT_NAME_PREFIX
from includes.reference_data import SLIPPAGE_CATEGORIES, ACCELERATION_CATEGORIES


@dp.table(name=INT_NAME_PREFIX+'slippage_categories', comment='Categories and definitions for slippage classification of use cases used by PipelineIQ')
def ref_slip_categories():
    return spark.createDataFrame(SLIPPAGE_CATEGORIES, 'type string, description string')

@dp.table(name=INT_NAME_PREFIX+'acceleration_categories', comment='Categories and definitions for acceleration classification of use cases used by PipelineIQ.')    
def ref_accel_categories():
    return spark.createDataFrame(ACCELERATION_CATEGORIES, 'type string, description string')