SELECT
    object_id,
    object_type,
    profile_id,
    profile_type,
    file_load_timestamp,
    batch_id
FROM `{{ params.offer_email_profile_error_table }}`
UNION DISTINCT
SELECT * EXCEPT(object_id_check, profile_id_check) FROM (
    SELECT
        SAFE_CAST(TRIM(object_id) as INT64) as object_id_check,
        SAFE_CAST(TRIM(profile_id) as INT64) as profile_id_check,
        TRIM(object_id) as object_id,
        TRIM(object_type) as object_type,
        TRIM(profile_id) as profile_id,
        TRIM(profile_type) as profile_type,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
        {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
    FROM `{{ params.offer_email_profile_work_table }}`
)
WHERE object_id_check IS NULL or object_type IS NULL or profile_id_check IS NULL or profile_type IS NULL
    or object_type NOT IN ('O','N') or profile_type NOT IN ('I','E');