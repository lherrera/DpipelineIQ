from pyspark import pipelines as dp
import pyspark.sql.functions as F
from includes.publishing_confs import INT_NAME_PREFIX, OUT_NAME_PREFIX
from includes.reference_data import SLIPPAGE_CATEGORIES, ACCELERATION_CATEGORIES


next_best_action_sql_expr = """
  COALESCE(
    ai_query(
      'databricks-gpt-oss-20b',
      CONCAT(
        'You are a Databricks Solutions Architect helping an account team decide the next best action for a customer account and a specific use case.\\n',
        'Inputs:\\n',
        'Account context: ', COALESCE(piq_out_context, 'None'), '\\n',
        'Use Case: ', COALESCE(piq_out_oneliner, 'None'), '\\n',
        'Next steps summary: ', COALESCE(piq_out_next_steps_summary, 'None'), '\\n',
        'Implementation notes: ', COALESCE(piq_out_implementation_notes_summary, 'None'), '\\n',
        'Blockers: ', COALESCE(piq_out_blockers_summary, 'None'), '\\n',
        'Competition: ', COALESCE(competition_string, 'None'), '\\n',
        'MEDDPICC dimension scores: Pain=', COALESCE(CAST(piq_out_pain_score AS STRING), 'None'),
        ', Champion=', COALESCE(CAST(piq_out_champion_score AS STRING), 'None'),
        ', ImplementationPlan=', COALESCE(CAST(piq_out_implementationplan_score AS STRING), 'None'),
        ', DecisionProcess=', COALESCE(CAST(piq_out_decisionprocess_score AS STRING), 'None'),
        ', Urgency=', COALESCE(CAST(piq_out_urgency_score AS STRING), 'None'),
        ', CompetitionAwareness=', COALESCE(CAST(piq_out_competitionawareness_score AS STRING), 'None'),
        ', MajorBlockers=', COALESCE(CAST(piq_out_majorblockers_score AS STRING), 'None'),
        ', MeasurableImpact=', COALESCE(CAST(piq_out_measurableimpact_score AS STRING), 'None'), '\\n',
        'Overall confidence level: ', COALESCE(piq_out_confidence_level_normalized, 'None'), '\\n\\n',
        'Rules:\\n',
        '1. Base your recommendation strictly on the evidence provided — do not assume missing information.\\n',
        '2. Prioritize actions that maximize the likelihood of a successful go-live and reduce blockers.\\n',
        '3. If Pain, Champion, or Implementation Plan is low (<5/10), suggest actions to strengthen these dimensions first.\\n',
        '4. If Major Blockers > 5/10, suggest actions to mitigate or remove the blockers.\\n',
        '5. If Urgency is high (>7/10), prioritize time-sensitive actions.\\n',
        '6. Provide one concise, actionable next step, written as if for the account team to execute within the next week.\\n',
        '7. Keep your answer short — 1–2 sentences max.\\n',
        '8. Be deterministic: always follow the rules exactly and do not vary wording unnecessarily.\\n\\n',
        'Return output in this exact format (no extra text):\\n\\n',
        'NEXT_BEST_ACTION: [Short, actionable recommendation for the account team]\\n',
        'RATIONALE: [Brief explanation linking the action to the MEDDPICC scores and blockers, ≤2 sentences]'
      ),
      named_struct('max_tokens', 200, 'temperature', 0.0, 'top_p', 0.1, 'failOnError', false)
    ),
    'NEXT_BEST_ACTION: Review MEDDPICC evidence and address missing information.\\nRATIONALE: No sufficient context or scores provided to generate a specific action.'
  ) AS piq_out_next_best_action_recommendation
"""

slippage_map = dict(SLIPPAGE_CATEGORIES)
acceleration_map = dict(ACCELERATION_CATEGORIES)

slippage_sql_expr = """coalesce(
ai_query(
  'databricks-gpt-oss-20b',
  concat(
    'You are a Databricks Senior Solutions Architect. Classify the PRIMARY reason this use case is likely to slip. Choose exactly ONE from: Technical, Business, Stakeholder, Budget, Project Timelines, Data, Integration, Partner, Hyperscaler, Competition, External dependencies, Other\\n',
    'Your decision rules are:\\n',
    ' - Technical → If slippage stems from missing features, technical blockers, or unresolved bugs\\n',
    ' - Business → If slippage is due to shifting business priorities, unclear value, or deprioritization\\n',
    ' - Stakeholder → If slippage results from lack of stakeholder buy-in, engagement, or sponsor turnover\\n',
    ' - Budget → If slippage is caused by funding gaps, delayed approvals, or budget freezes\\n',
    ' - Project Timelines → If slippage is driven by unrealistic schedules, missed milestones, or dependencies\\n',
    ' - Data → If slippage comes from data issues (access, quality, privacy, or integration)\\n',
    ' - Integration → If slippage is due to system connectivity, API, or tooling integration problems\\n',
    ' - Partner → If slippage originates from partner-side limitations, bandwidth, or enablement gaps\\n',
    ' - Hyperscaler → If slippage stems from hyperscaler constraints (region, quota, roadmap, or policies)\\n',
    ' - Competition → If slippage is triggered by market shifts or competitive reprioritization\\n',
    ' - External dependencies → If slippage comes from external third parties or customer-side dependencies\\n',
    ' - Other → If the cause does not fit any of the above\\n',
    'Return exactly ONE of those category names. No punctuation or explanation.\\n',
    'Inputs: ',
    'Confidence Score Rationale: ',
    COALESCE(LEFT(piq_out_rationale_for_confidence_score, 200), 'N/A'),
    '\\nNext Step: ',
    COALESCE(LEFT(piq_out_next_best_action_recommendation, 200), 'N/A')
  ),
  named_struct('max_tokens', 200, 'temperature', 0.0, 'top_p', 0.1, 'failOnError', false)
  ),
  'Unable to determine slippage category'
)"""

accel_sql_expr = """coalesce(
  ai_query(
    'databricks-gpt-oss-20b',
    concat(
      'You are a Databricks Senior Solutions Architect. Classify the PRIMARY reason this use case could be accelerated. Choose exactly ONE from: Technical, Business, Stakeholder, Budget, Project Timelines, Data, Integration, Partner, Hyperscaler, Competition, External dependencies, Other\\n',
      'Your decision rules are:\\n',
      ' - Technical → If acceleration comes from technical enablers, feature availability, or resolved blockers\\n',
      ' - Business → If acceleration is driven by increased business priority, clear value, or urgency\\n',
      ' - Stakeholder → If acceleration results from strong stakeholder buy-in, engagement, or executive sponsorship\\n',
      ' - Budget → If acceleration is enabled by secured funding, approved budgets, or financial incentives\\n',
      ' - Project Timelines → If acceleration is possible through realistic schedules, met milestones, or resolved dependencies\\n',
      ' - Data → If acceleration comes from resolved data issues (access, quality, privacy, or integration)\\n',
      ' - Integration → If acceleration is enabled by successful system connectivity, API, or tooling integration\\n',
      ' - Partner → If acceleration originates from partner readiness, bandwidth, or enablement success\\n',
      ' - Hyperscaler → If acceleration stems from hyperscaler support (region availability, quota, roadmap alignment)\\n',
      ' - Competition → If acceleration is triggered by competitive advantage or market timing\\n',
      ' - External dependencies → If acceleration comes from resolved external third party or customer-side dependencies\\n',
      ' - Other → If the acceleration factor does not fit any of the above\\n',
      'Return exactly one of the labels above — no punctuation or explanation.\\n',
      'Inputs: ',
      'Confidence Score Rationale: ',
      COALESCE(LEFT(piq_out_rationale_for_confidence_score, 200), 'N/A'),
      '\\nNext Step: ',
      COALESCE(LEFT(piq_out_next_best_action_recommendation, 200), 'N/A')
    ),
    named_struct('max_tokens', 200, 'temperature', 0.0, 'top_p', 0.1, 'failOnError', false)
    ),
  'Unable to determine acceleration category'
)"""

@dp.table(name=OUT_NAME_PREFIX+'pipelineIQ', comment='PipelineIQ: Use case pipeline with AI-enriched fields.')
def create_pipelineiq_recommendations():
  in_df = spark.readStream.table(INT_NAME_PREFIX+"ai_analysed_usecases")
  add_nba_and_bools_df = in_df.withColumn(
    'piq_out_next_best_action_recommendation',
    F.expr(next_best_action_sql_expr)
  ).withColumn(
    'piq_out_likely_to_slip',
    F.when(F.col('piq_out_confidence_level_normalized') == 'Medium', True)
      .otherwise(False)
  ).withColumn(
    'piq_out_can_be_accelerated', F.when(F.col('piq_out_confidence_level_normalized').isin('High', 'Medium'), True)
      .otherwise(False)
  )
  add_slippage_and_accel_df = add_nba_and_bools_df.withColumns({
    'piq_out_slippage_category': F.when(F.col('piq_out_likely_to_slip'), F.expr(slippage_sql_expr)).otherwise('N/A'), 
    'piq_out_accel_category': F.when(F.col('piq_out_can_be_accelerated'), F.expr(accel_sql_expr)).otherwise('N/A')})
  return add_slippage_and_accel_df