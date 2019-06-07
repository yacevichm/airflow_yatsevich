SELECT
    object_id,
    object_type,
    list_id,
    list_type,
    file_load_timestamp,
    batch_id
FROM `{{ params.offer_email_list_stage_table }}`
-- FROM `company_name-ddh-lle`.company_name_dev_dp_customer_db.stage_loyalty_offer_email_list_tst
UNION DISTINCT
SELECT
    SAFE_CAST(TRIM(object_id) as INT64) as object_id,
    TRIM(object_type),
    SAFE_CAST(TRIM(list_id) as INT64) as list_id,
    TRIM(list_type),
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
    {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
--FROM `company_name-ddh-lle`.company_name_dev_dp_customer_db.work_loyalty_offer_email_list_tst
FROM `{{ params.offer_email_list_work_table }}`
WHERE object_id IS NOT NULL and object_type IS NOT NULL and list_id IS NOT NULL and list_type IS NOT NULL
    and object_type IN ('O','N') and list_type IN ('I','E');