# account_context_prompt =   'Summarize the following context and business impact into a concise paragraph (max 4 sentences). Focus on the main challenge, expected business value, the main type of contact (e.g., Executive sponsor, technical decision maker, Economic decision maker, etc), and whether funding is available or approved. Context: ';
# use_case_oneliner_prompt = 
# F.lit("Generate a short, clear one-line summary (max 50 words) describing the core business objective of this use case. "),
# Categories :
# business use case prompt :   'Given a use case name and description, return ONLY a concise business use case label (e.g. Fraud Detection, Inventory Forecasting, Customer Segmentation). Output exactly one short phrase, max 5 words, no punctuation or explanation. Usecase Name: ';

from pyspark import pipelines as dp
import pyspark.sql.functions as F
from includes.publishing_confs import INT_NAME_PREFIX, OUT_NAME_PREFIX

ai_prompts = {
  "account_context": "Summarize the following context and business impact into a concise paragraph (max 4 sentences). Focus on the main challenge, expected business value, the main type of contact (e.g., Executive sponsor, technical decision maker, Economic decision maker, etc), and whether funding is available or approved. Context: ",
  "usecase_oneliner": "Generate a short, clear one-line summary (max 50 words) describing the core business objective of this use case: ",
  "usecase_next_steps": "Summarize the following next steps into a short, action-oriented sentence focusing on immediate priorities: ",
  "implementation_notes": 'Summarize the following implementation notes into a short, action-oriented paragraph focusing on key steps, milestones, and ownership: ',
  "usecase_types": [
        'Machine Learning', 'Generative AI', 'Data Warehousing', 'Migration AI', 'Migration Unity Catalog',
        'Migration DWH', 'Migration ETL', 'Migration Streaming', 'Migration BI', 
        'BI', 'ETL', 'Governance', 'Streaming', 'Ingestion', 'Platform', 'Other'
      ],
  "business_usecase":  "Given a use case name and description, return ONLY a concise business use case label (e.g. Fraud Detection, Inventory Forecasting, Customer Segmentation). Output exactly one short phrase, max 5 words, no punctuation or explanation. ",
  "blockers_summary": "Briefly summarize the main theme or issue based on the latest blocker comment: ",
  "confidence_advanced_context_instructions": """
  You are a Databricks Solutions Architect evaluating a customer use case and scoring its likelihood to reach production and generate sustained usage.\n
  Rate the use case using these MEDDPICC-based dimensions (0-10 each):
    1. Pain - Clear, urgent business problem?
    2. Champion - Identified internal advocate?
    3. Implementation Plan - Concrete next steps and ownership?
    4. Decision Process - Clear decision path?
    5. Urgency - Time pressure or deadline?
    6. Competition Awareness - Known competitors?
    7. Measurable Impact - Defined success metrics?
    8. Major Blockers - Significant obstacles (higher score = **more blockers**).
  Rules:
    - Base scores strictly on available evidence — do not speculate or imagine missing info.
    - If information is missing, assign a low score (≤3) for that dimension.
    - Use these weightings: Pain 25%, Champion 20%, Implementation Plan 20%, Decision Process 10%, Urgency 10%, Competition Awareness 5%, Measurable Impact 2%, Major Blockers -8% (deductive).
    - Compute the total confidence_score as: (Pain×0.25 + Champion×0.20 + ImplementationPlan×0.20 + DecisionProcess×0.10 + Urgency×0.10 + CompetitionAwareness×0.05 + MeasurableImpact×0.02 - MajorBlockers×0.08) × 10.
    - A perfect use case (10s across positive dimensions, 0 blockers) scores 100.
    - The total confidence_score must be between 0 and 100, rounded to the nearest multiple of 10.
  """,
  "confidence_advanced_output_instructions":"""
  Return output in **this exact format** (no text before or after): 
  CONFIDENCE_SCORE: [0-100 number]\n
  LEVEL: [High || Medium || Low]\n
  RATIONALE: [Brief explanation, 2-3 sentences max]\n
  DIMENSION_SCORES:\n
    Pain=x,
    Champion=x,
    ImplementationPlan=x,
    DecisionProcess=x,
    Urgency=x,
    CompetitionAwareness=x,
    MajorBlockers=x,
    MeasurableImpact=x
  """
}

account_context_sql_expr = f"""
ai_summarize(
      CONCAT(
        '{ai_prompts.get('account_context')}',
        piq_input_context
      ),
      100
    )
"""
usecase_oneliner_sql_expr = f"""
ai_summarize(
    concat(
      '{ai_prompts.get('usecase_oneliner')}',
      piq_input_usecase
    ),
    25
  )
"""
usecase_next_steps_sql_expr = f"""
ai_summarize(
    concat(
      '{ai_prompts.get('usecase_next_steps')}',
      piq_input_next_steps
    ),
    100
    )
    """
implementation_notes_sql_expr = f"""
ai_summarize(
    concat(
      '{ai_prompts.get('implementation_notes')}',
      piq_input_implementation_notes
    ),
    100
    )
    """
usecase_types_sql_exr = f"""ai_classify(
  piq_input_usecase_classification,
  array({', '.join([f"'{t}'" for t in ai_prompts.get('usecase_types')])})
    )
  """
business_usecase_sql_expr = f"""coalesce(
  ai_query(
    'databricks-gpt-oss-20b', 
    concat(
      '{ai_prompts.get('business_usecase')}',
      piq_input_business_usecase_classification
      ),
    named_struct('max_tokens', 20, 'temperature', 0.0, 'failOnError', false)
    ),
    "Unknown business use case"
  )
"""
blockers_summary_sql_expr = f"""
  ai_summarize(
  CONCAT(
    piq_input_blocker_counts,
    '{ai_prompts.get('blockers_summary')}',
    coalesce(last_modified_blocker_comment, 'No blocker comment provided.'),
    "Category: ",
    coalesce(last_modified_blocker_category, 'Unknown')
  ),
  50
)
"""

def build_confidence_context_from_ai_results(account_context_col_name: str, one_liner_col_name: str, next_steps_col_name: str, implementation_notes_col_name: str, blockers_summary_col_name: str, competition_string_col_name: str):
  """Helper function to build the final AI query which depends on previous inputs."""
  confidence_context_col = F.concat(
    F.lit("Context:\n"),
    F.coalesce(F.col(account_context_col_name), F.lit('None')),
    F.lit("\nUse Case:\n"),
  F.coalesce(F.col(one_liner_col_name), F.lit('None')),
  F.lit("\nNext Steps:\n"),
  F.coalesce(F.col(next_steps_col_name), F.lit('None')),
  F.lit("\nImplementation Notes:\n"),
  F.coalesce(F.col(implementation_notes_col_name), F.lit('None')),
  F.lit("\nBlockers:\n"),
  F.coalesce(F.col(blockers_summary_col_name), F.lit('None')),
  F.lit("\nCompetition:\n"),
  F.coalesce(F.col(competition_string_col_name), F.lit('None')),
  F.lit("\n\n")
  )
  return confidence_context_col

  
input_table_name = INT_NAME_PREFIX+"usecases_with_prepared_context"
@dp.table(name= INT_NAME_PREFIX+"ai_analysed_usecases", comment='PipelineIQ AI Analysis part 1 - Evaluating use cases')
def ai_evaluate_use_cases():

  input_df = spark.readStream.table(input_table_name)
  ai_generated_columns_set1 = {
    "piq_out_context": F.when(F.col('piq_has_account_context'), F.expr(account_context_sql_expr)).otherwise('No context or business impact provided.'),
    "piq_out_oneliner": F.when(F.col('piq_has_usecase'), F.expr(usecase_oneliner_sql_expr)).otherwise('No use case name or description provided.'),
    "piq_out_next_steps_summary": F.when(F.col('piq_has_demand_plan'), F.expr(usecase_next_steps_sql_expr)).otherwise('No next steps provided'),
    "piq_out_implementation_notes_summary": F.when(F.col('piq_has_implementation_notes'), F.expr(implementation_notes_sql_expr)).otherwise('There is no implementation plan or not specified in Salesforce'),
    "piq_out_usecase_type": F.when(F.col('piq_has_usecase'), F.expr(usecase_types_sql_exr)).otherwise('Other'),
    "piq_out_business_usecase_type": F.expr(business_usecase_sql_expr),
    # next=. usecases_with_types
    "piq_out_blockers_summary": F.when(F.col('piq_has_blockers') & F.col('piq_has_blocker_details'), F.expr(blockers_summary_sql_expr)).otherwise('No blockers or missing details to summarise.'),
  }
  ai_augmented_df = (input_df.withColumns(ai_generated_columns_set1).withColumn('piq_input_confidence_context', build_confidence_context_from_ai_results('piq_out_context', 'piq_out_oneliner', 'piq_out_next_steps_summary', 'piq_out_implementation_notes_summary', 'piq_out_blockers_summary', 'competition_string')))
  # build in pipeline as it depends on other AI fields
  ai_confidence_sql_expr = f"""
  coalesce(
    ai_query(
      'databricks-gpt-oss-20b',
      concat(
        '{ai_prompts.get('confidence_advanced_context_instructions')}',
        piq_input_confidence_context,
        '{ai_prompts.get('confidence_advanced_output_instructions')}'
        ),
      named_struct('max_tokens', 200, 'temperature', 0.0, 'top_p', 0.1, 'failOnError', false)
    ),
    "CONFIDENCE_SCORE: 0\nLEVEL: Low\nRATIONALE: No information provided.\nDIMENSION_SCORES: Pain=0, Champion=0, ImplementationPlan=0, DecisionProcess=0, Urgency=0, CompetitionAwareness=0, MajorBlockers=0, MeasurableImpact=0"
  )
  """
  ai_augmented_with_confidence = ai_augmented_df.withColumn('piq_out_ai_confidence_score_advanced', F.expr(ai_confidence_sql_expr))
  fields_to_extract = {
    "piq_out_confidence_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'CONFIDENCE_SCORE:\\s*(\\d+)', 1).cast('int'),
    "piq_out_confidence_level": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'LEVEL:\\s*(High|Medium|Low)', 1),
    "piq_out_rationale_for_confidence_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'RATIONALE:\\s*(.+?)\\s*DIMENSION_SCORES:', 1),
    "piq_out_pain_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'Pain=(\\d+)', 1).cast('int'),
    "piq_out_champion_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'Champion=(\\d+)', 1).cast('int'),
    "piq_out_implementationplan_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'ImplementationPlan=(\\d+)', 1).cast('int'),
    "piq_out_decisionprocess_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'DecisionProcess=(\\d+)', 1).cast('int'),
    "piq_out_urgency_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'Urgency=(\\d+)', 1).cast('int'),
    "piq_out_competitionawareness_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'CompetitionAwareness=(\\d+)', 1).cast('int'),
    "piq_out_majorblockers_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'MajorBlockers=(\\d+)', 1).cast('int'),
    "piq_out_measurableimpact_score": F.regexp_extract('piq_out_ai_confidence_score_advanced', 'MeasurableImpact=(\\d+)', 1).cast('int')
  }
  ai_augmented_with_confidence_and_scores = ai_augmented_with_confidence.withColumns(fields_to_extract)

  return(ai_augmented_with_confidence_and_scores.withColumn(   "piq_out_confidence_level_normalized", F.when(F.col('piq_out_confidence_score') >= 75, 'High').when(F.col('piq_out_confidence_score') >= 45, 'Medium').when(F.col('piq_out_confidence_score').isNotNull(), 'Low').otherwise('Not computed')))