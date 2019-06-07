SELECT * FROM `{{ params.offer_stage_table }}`
UNION DISTINCT
SELECT
    SAFE_CAST(TRIM(offer_id) as INT64) as offer_id,
    TRIM(offer_name) as offer_name,
    TRIM(offer_description) as offer_description,
    TRIM(offer_headline) as offer_headline,
    TRIM(internal_note) as internal_note,
    TRIM(terms_and_conditions) as terms_and_conditions,
    SAFE_CAST(publish_start_date as TIMESTAMP) as publish_start_date,
    SAFE_CAST(publish_end_date as TIMESTAMP) as publish_end_date,
    SAFE_CAST(qualification_start_date as TIMESTAMP) as qualification_start_date,
    SAFE_CAST(qualification_end_date as TIMESTAMP) as qualification_end_date,
    SAFE_CAST(TRIM(redemption_limit) as INT64) as redemption_limit,
    TRIM(event_id) as event_id,
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
    {{ ti.xcom_pull( key = ti.run_id ) }} as batch_id
FROM `{{ params.offer_work_table }}`
WHERE offer_id is not null and offer_name is not null
and offer_description is not null and offer_headline is not null
and qualification_start_date is not null and qualification_end_date is not null
and offer_name !=''
and offer_description !='' and offer_headline !=''
and qualification_start_date !='' and qualification_end_date !='';