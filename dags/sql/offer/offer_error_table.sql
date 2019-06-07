SELECT * FROM `{{ params.offer_error_table }}`
UNION DISTINCT
SELECT * EXCEPT(offer_id_chk, redemption_limit_chk) FROM
(
    SELECT
        SAFE_CAST(TRIM(offer_id) as INT64) as offer_id_chk,
        TRIM(offer_id) as offer_id,
        TRIM(offer_name) as offer_name,
        TRIM(offer_description) as offer_description,
        TRIM(offer_headline) as offer_headline,
        TRIM(internal_note) as internal_note,
        TRIM(terms_and_conditions) as terms_and_conditions,
        publish_start_date as publish_start_date,
        publish_end_date as publish_end_date,
        qualification_start_date as qualification_start_date,
        qualification_end_date as qualification_end_date,
        SAFE_CAST(TRIM(redemption_limit) as INT64) as redemption_limit_chk,
        TRIM(redemption_limit) as redemption_limit,
        TRIM(event_id) as event_id,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
        {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
    FROM `{{ params.offer_work_table }}`
)
WHERE offer_id_chk is null or offer_name is null
    or offer_description is null or offer_headline is null
    or qualification_start_date is null or qualification_end_date is null
    or offer_name = ''
    or offer_description = '' or offer_headline = ''
    or qualification_start_date = '' or qualification_end_date = '';