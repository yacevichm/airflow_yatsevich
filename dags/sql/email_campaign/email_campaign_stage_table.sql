SELECT * FROM `{{ params.email_campaign_stage_table }}`
UNION DISTINCT
SELECT
    SAFE_CAST(TRIM(email_communication_id) as INT64) as email_communication_id,
    TRIM(communication_name) as communication_name,
    TRIM(campaign_name) as campaign_name,
    SAFE_CAST(start_date as TIMESTAMP) as start_date,
    SAFE_CAST(end_date as TIMESTAMP) as end_date,
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
    {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
FROM `{{ params.email_campaign_work_table }}`
WHERE email_communication_id IS NOT NULL AND communication_name IS NOT NULL
AND communication_name !='';