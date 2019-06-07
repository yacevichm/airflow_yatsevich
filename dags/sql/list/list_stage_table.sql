SELECT
    list_id,
    list_name,
    list_description,
    internal_note,
    file_load_timestamp,
    batch_id
FROM `{{ params.list_stage_table }}`
UNION DISTINCT
SELECT
    SAFE_CAST(TRIM(list_id) as INT64) as list_id,
    TRIM(list_name) as list_name,
    TRIM(list_description) as list_description,
    TRIM(internal_note) as internal_note,
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
    {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
FROM `{{ params.list_work_table }}`
WHERE list_id IS NOT NULL and list_name IS NOT NULL and list_name != '';