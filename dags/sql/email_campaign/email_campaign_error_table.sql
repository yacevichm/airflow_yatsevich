SELECT * FROM `{{ params.email_campaign_error_table }}`
UNION DISTINCT
SELECT * EXCEPT(email_communication_id_chk) FROM
(
    SELECT
        SAFE_CAST(TRIM(email_communication_id) as INT64) as email_communication_id_chk,
        TRIM(email_communication_id) as email_communication_id,
        TRIM(communication_name) as communication_name,
        TRIM(campaign_name) as campaign_name,
        start_date,
        end_date,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
        {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
    FROM `{{ params.email_campaign_work_table }}`
)
WHERE email_communication_id_chk is null or communication_name is null
or communication_name ='';
