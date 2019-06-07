with full_outer as (
    select
        s.list_id as s_list_id,
        s.list_name as s_list_name,
        s.list_description as s_list_description,
        s.internal_note as s_internal_note,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
        {{ ti.xcom_pull( key = ti.run_id ) }} as s_batch_id,
        g.lst_id as g_lst_id,
        g.lst_nm as g_lst_nm,
        g.lst_desc as g_lst_desc,
        g.intr_note_txt as g_intr_note_txt,
        g.eff_dte,
        g.expir_dte,
        g.btch_id as g_btch_id,
        g.fil_lod_tmst as g_fil_lod_tmst
    from (select * from `{{ params.list_stage_table }}`  where batch_id={{ ti.xcom_pull( key = ti.run_id ) }})  s
    full outer join (
    select * from `{{ params.list_gold_table }}`
        where (expir_dte is null or expir_dte=PARSE_DATE('%Y-%m-%d',  '9999-12-31'))) g
    on g.lst_id = s.list_id
    and g.lst_nm = s.list_name
    and g.lst_desc = s.list_description
    and g.intr_note_txt = s.internal_note
),


end_dte_not_null as (
    select
        lst_id,
        lst_nm,
        lst_desc,
        intr_note_txt,
        eff_dte,
        expir_dte,
        fil_lod_tmst,
        btch_id
    from `{{ params.list_gold_table }}`
    where expir_dte != PARSE_DATE('%Y-%m-%d',  '9999-12-31')
),


stage_only as
(
    select
        s_list_id as lst_id,
        s_list_name as lst_nm,
        s_list_description as lst_desc,
        s_internal_note as intr_note_txt,
        current_date() as eff_dte,
        PARSE_DATE('%Y-%m-%d',  '9999-12-31') as expir_dte,
        file_load_timestamp as fil_lod_tmst,
        s_batch_id as btch_id
    from full_outer
    where g_lst_id is null
),


gold_only as
(
    select
        g_lst_id,
        g_lst_nm,
        g_lst_desc,
        g_intr_note_txt,
        eff_dte,
        current_date() as expir_dte,
        g_fil_lod_tmst,
        g_btch_id
    from full_outer
    where s_list_id is null
),


common as
(
    select
        g_lst_id,
        g_lst_nm,
        g_lst_desc,
        g_intr_note_txt,
        eff_dte,
        expir_dte,
        g_fil_lod_tmst,
        g_btch_id
    from full_outer
    where s_list_id is not null and g_lst_id is not null
),


final AS (
select * from stage_only
UNION DISTINCT
select * from gold_only
UNION DISTINCT
select * from common
UNION DISTINCT
select * from end_dte_not_null
)

SELECT * FROM final;



