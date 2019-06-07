with full_outer as (
    select
        s.object_id as s_obj_ref_id,
        s.object_type as s_obj_ref_typ_cde,
        s.profile_id as s_prfl_id,
        s.profile_type as s_prfl_incln_typ_cde,
        FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
        {{ ti.xcom_pull( key = ti.run_id ) }} as s_batch_id,
        g.obj_ref_id as g_obj_ref_id,
        g.obj_ref_typ_cde as g_obj_ref_typ_cde,
        g.prfl_id as g_prfl_id,
        g.prfl_incln_typ_cde as g_prfl_incln_typ_cde,
        g.eff_dte,
        g.expir_dte,
        g.btch_id as g_btch_id,
        g.fil_lod_tmst as g_fil_lod_tmst
    from ( select * from `{{ params.offer_email_profile_stage_table }}`
        where batch_id={{ ti.xcom_pull( key = ti.run_id ) }}) s
    full outer join (select * from `{{ params.offer_email_profile_gold_table }}`
        where (expir_dte is null or expir_dte='9999-12-31'))  g  on g.obj_ref_id = s.object_id
        and g.obj_ref_typ_cde = s.object_type
        and g.prfl_id = s.profile_id
        and g.prfl_incln_typ_cde = s.profile_type
),

stage_only as (
    select
        s_obj_ref_id as obj_ref_id,
        s_obj_ref_typ_cde as obj_ref_typ_cde,
        s_prfl_id as prfl_id,
        s_prfl_incln_typ_cde as prfl_incln_typ_cde,
        current_date() as eff_dte,
        PARSE_DATE('%Y-%m-%d',  '9999-12-31') as expir_dte,
        file_load_timestamp as fil_lod_tmst,
        s_batch_id as btch_id
    from full_outer
        where g_obj_ref_id is null
),

gold_only as (
    select
        g_obj_ref_id,
        g_obj_ref_typ_cde,
        g_prfl_id,
        g_prfl_incln_typ_cde,
        eff_dte,
        current_date() as expir_dte,
        g_fil_lod_tmst,
        g_btch_id
    from full_outer
        where s_obj_ref_id is null
),

common as (
    select
        g_obj_ref_id,
        g_obj_ref_typ_cde,
        g_prfl_id,
        g_prfl_incln_typ_cde,
        eff_dte,
        expir_dte,
        g_fil_lod_tmst,
        g_btch_id
    from full_outer
        where g_obj_ref_id is not null and s_obj_ref_id is not null
),


expir_dte_is_not_default as (
    select
        obj_ref_id,
        obj_ref_typ_cde,
        prfl_id,
        prfl_incln_typ_cde,
        eff_dte,
        expir_dte,
        fil_lod_tmst,
        btch_id
    from `{{ params.offer_email_profile_gold_table }}`
        where expir_dte!='9999-12-31'
),

final as (
    select * from stage_only
    UNION DISTINCT
    select * from gold_only
    UNION DISTINCT
    select * from common
    UNION DISTINCT
    select * from expir_dte_is_not_default
)

SELECT * FROM final;