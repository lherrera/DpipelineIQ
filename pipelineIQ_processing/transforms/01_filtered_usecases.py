from pyspark import pipelines as dp
from pyspark.sql import functions as F
from includes.publishing_confs import INT_NAME_PREFIX

filter_configs = {
    "min_monthly_dbus": 5000,
    "excluded_stages": ["U1", "Lost", "Closed", "Disqualified"],
    "target_live_date_min_months": F.current_date(),
    "target_live_date_max_months": F.add_months(F.current_date(), 6),
    "last_modified_min_months": -6,
}


@dp.temporary_view(
    name="vw_filtered_usecases",
    comment="Filtered use cases applying business logic rules",
)
def filtered_usecases():
    """
    Apply business filters and transformations to source data.
    Removes low-value use cases and applies NULL handling logic.
    """
    return (
        spark.readStream.table("main.gtm_data.core_usecase")
        # Apply business filters
        .filter(
            F.col("estimated_monthly_dollar_dbus")
            >= filter_configs.get("min_monthly_dbus")
        )
        .filter(~F.col("stage").isin(filter_configs.get("excluded_stages")))
        .filter(
            F.col("target_live_date").between(
                filter_configs.get("target_live_date_min_months"),
                filter_configs.get("target_live_date_max_months"),
            )
        )
        .filter(
            F.col("last_modified_date")
            >= F.add_months(
                F.current_date(), filter_configs.get("last_modified_min_months")
            )
        )
        # NULL handling transformations
        .withColumn(
            "field_manager",
            F.when(F.col("field_manager").isNull(), "FLM not assigned").otherwise(
                F.col("field_manager")
            ),
        )
        .withColumn(
            "account_solution_architect",
            F.when(
                F.col("account_solution_architect").isNull(), "SA not assigned"
            ).otherwise(F.col("account_solution_architect")),
        )
        .withColumn(
            "sales_region",
            F.when(F.col("sales_region").isNotNull(), F.col("sales_region"))
            .when(F.col("business_unit").isNotNull(), F.col("business_unit"))
            .otherwise("No assigned"),
        )
        .withColumn("num_of_blockers", F.coalesce(F.col("num_of_blockers"), F.lit(0)))
    )
