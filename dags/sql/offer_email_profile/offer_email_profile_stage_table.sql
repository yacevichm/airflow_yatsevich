SELECT
    object_id,
    object_type,
    profile_id,
    profile_type,
    file_load_timestamp,
    batch_id
FROM `{{ params.offer_email_profile_stage_table }}`
UNION DISTINCT
SELECT
    SAFE_CAST(TRIM(object_id) as INT64) as object_id,
    TRIM(object_type),
    SAFE_CAST(TRIM(profile_id) as INT64) as profile_id,
    TRIM(profile_type),
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as  file_load_timestamp,
    {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
--FROM `company_name-ddh-lle`.company_name_dev_dp_customer_db.work_loyalty_offer_email_profile_tst
FROM `{{ params.offer_email_profile_work_table }}`
WHERE object_id IS NOT NULL and profile_id IS NOT NULL and object_type IS NOT NULL and profile_type IS NOT NULL
    and object_type IN ('N','O') and profile_type IN ('E','I');
