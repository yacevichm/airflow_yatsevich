SELECT
    object_id,
    object_type,
    list_id,
    list_type,
    file_load_timestamp,
    batch_id
FROM `{{ params.offer_email_list_error_table }}`
UNION DISTINCT
SELECT * EXCEPT(object_id_check, list_id_check) FROM (
    SELECT
        SAFE_CAST(TRIM(object_id) as INT64) as object_id_check,
        SAFE_CAST(TRIM(list_id) as INT64) as list_id_check,
        TRIM(object_id) as object_id,
        TRIM(object_type) as object_type,
        TRIM(list_id) as list_id,
        TRIM(list_type) as list_type,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
        {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
    FROM `{{ params.offer_email_list_work_table }}`
)
WHERE object_id_check IS NULL or object_type IS NULL or list_id_check IS NULL or list_type IS NULL
    or object_type NOT IN ('O','N') or list_type NOT IN ('I','E')