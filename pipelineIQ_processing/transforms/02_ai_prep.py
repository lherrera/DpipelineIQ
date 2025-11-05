from pyspark import pipelines as dp
from pyspark.sql import functions as F
from includes.publishing_confs import INT_NAME_PREFIX

input_df_name = "vw_filtered_usecases"
@dp.temporary_view(name="vw_usecases_with_prepared_context")
def prepare_context_for_ai():
    """In this function we pre-process string columns for the ai operations in the next file. 4 Steps (Booleans, Cleanse, add bools to df, add inputs to df)"""
    input_df = spark.readStream.table(input_df_name)

    # 1 Define the boolean check conditions
    # Check if any of the fields have non-null, non-empty values
    has_account_context = (
        (
            F.col("sdr_bdr_notes").isNotNull()
            & (F.length(F.trim(F.col("sdr_bdr_notes"))) > 0)
        )
        | (
            F.col("business_impact").isNotNull()
            & (F.length(F.trim(F.col("business_impact"))) > 0)
        )
        | (
            F.col("business_outcome_description").isNotNull()
            & (F.length(F.trim(F.col("business_outcome_description"))) > 0)
        )
        | (
            F.col("ae_authority").isNotNull()
            & (F.length(F.trim(F.col("ae_authority"))) > 0)
        )
        | (
            F.col("ae_money_budget").isNotNull()
            & (F.length(F.trim(F.col("ae_money_budget"))) > 0)
        )
    )
    has_usecase_metadata = (
        F.col("usecase_name").isNotNull()
        & (F.length(F.trim(F.col("usecase_name"))) > 0)
    ) | (
        F.col("usecase_description").isNotNull()
        & (F.length(F.trim(F.col("usecase_description"))) > 0)
    )
    has_demand_plan = (
      F.col("demand_plan_stage_next_steps").isNotNull() & 
      (F.length(F.trim(F.col("demand_plan_stage_next_steps"))) > 0
    ))
    has_implementation_notes = (
      F.col("implementation_notes").isNotNull() & 
      (F.length(F.trim(F.col("implementation_notes"))) > 0
    ))
    has_blockers = F.col('num_of_blockers') > 0
    has_blocker_details = (F.col('last_modified_blocker_category').isNotNull() & (F.length(F.trim(F.col('last_modified_blocker_category'))) > 0)) & (F.col('last_modified_blocker_comment').isNotNull() & (F.length(F.trim(F.col('last_modified_blocker_comment'))) > 0))
 


    # 2 Build the concatenated inputs for ai_summarize
    # Remove HTML tags from sdr_bdr_notes and take first 2000 chars
    cleaned_notes = F.regexp_replace(F.col("sdr_bdr_notes"), "<[^>]+>", "")
    truncated_notes = F.substring(cleaned_notes, 1, 2000)

    account_context_input = F.concat(
        F.coalesce(F.trim(truncated_notes), F.lit("No context provided. ")),
        F.lit("Business Impact: "),
        F.coalesce(
            F.trim(F.col("business_impact")), F.lit("No business impact provided. ")
        ),
        F.lit("Business Outcome: "),
        F.coalesce(
            F.trim(F.col("business_outcome_description")),
            F.lit("No business outcome provided. "),
        ),
        F.lit("Main Contact Type: "),
        F.coalesce(F.trim(F.col("ae_authority")), F.lit("Not specified.")),
        F.lit("Funding Status: "),
        F.coalesce(F.trim(F.col("ae_money_budget")), F.lit("Not specified.")),
    )
    usecase_oneliner_input = F.concat(
        F.lit("Account Context: "),
        F.coalesce(F.trim(F.col("piq_input_context")), F.lit("No account context")),
        F.lit("Use Case Name: "),
        F.coalesce(F.trim(F.col("usecase_name")), F.lit("Use Case name not provided.")),
        F.lit(". Description: "),
        F.coalesce(
            F.trim(F.col("usecase_description")), F.lit("No description provided.")
        ),
    )
    next_steps_input = F.trim(F.col("demand_plan_stage_next_steps"))
    implementation_notes_input = F.trim(F.col("implementation_notes"))
    # Shares the 'has_usecase_metadata' flag with usecase_oneline 
    usecase_classify_types_input = F.concat(
        F.lit("Use Case Name: "),
        F.trim(F.col("usecase_name")), 
        F.lit(". Description: "),
        F.trim(F.col("usecase_description")),
        F.lit(".")
    )
    business_usecase_classification_input = F.concat(
      F.lit("Use Case Name: "),
      F.coalesce(F.trim(F.col("usecase_name")), F.lit('Unknown')),
      F.lit(". Description: "),
      F.coalesce(F.trim(F.col("usecase_description")), F.lit('No description')),
      F.lit(".")
    )
    blocker_count_input = F.concat(
      F.lit('There are '),
      F.col('num_of_blockers').cast('string'),
       F.lit(' blockers.')
       )
    confidence_context_input =  F.concat(
      F.lit('Context:\n'),
      F.lit('Use Case: '),
      F.coalesce(F.trim(F.col("usecase_name")), F.lit('Unknown'))
    )

    # 3 Add all flags
    df_with_flags = input_df.withColumns(
        {
            "piq_has_account_context": has_account_context,
            "piq_has_usecase": has_usecase_metadata,
            "piq_has_demand_plan": has_demand_plan,
            "piq_has_implementation_notes": has_implementation_notes,
            "piq_has_blockers": has_blockers,
            "piq_has_blocker_details": has_blocker_details
        }
    )
    # 4 Add all inputs
    df_with_ai_inputs = df_with_flags.withColumns(
        {
            "piq_input_context": account_context_input,
            "piq_input_usecase": usecase_oneliner_input,
            "piq_input_next_steps": next_steps_input,
            "piq_input_implementation_notes": implementation_notes_input,
            "piq_input_usecase_classification": usecase_classify_types_input,
            "piq_input_business_usecase_classification":
              business_usecase_classification_input,
            "piq_input_blocker_counts": blocker_count_input
        }
    )

    return df_with_ai_inputs

