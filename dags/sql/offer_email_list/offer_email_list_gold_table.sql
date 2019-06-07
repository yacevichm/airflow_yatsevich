with full_outer as (
select s.object_id as s_object_id,
s.object_type as s_object_type,
s.list_id as s_list_id,
s.list_type as s_list_type,
FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
{{ ti.xcom_pull( key = ti.run_id ) }} as s_batch_id,
g.obj_ref_id as g_obj_ref_id,
g.obj_ref_typ_cde as g_obj_ref_typ_cde,
g.lst_id as g_lst_id,
g.lst_incln_typ_cde as g_lst_incln_typ_cde,
g.eff_dte,
g.expir_dte,
g.fil_lod_tmst as g_fil_lod_tmst,
g.btch_id as g_btch_id
from (select * from `{{ params.offer_email_list_stage_table }}` where batch_id={{ ti.xcom_pull( key = ti.run_id ) }})  s
full outer join (select * from `{{ params.offer_email_list_gold_table }}`
    where (expir_dte is null or expir_dte='9999-12-31')) g on g.obj_ref_id = s.object_id
and g.obj_ref_typ_cde = s.object_type
and g.lst_id = s.list_id
and g.lst_incln_typ_cde = s.list_type
),

expir_dte_not_null as (
select
obj_ref_id,
obj_ref_typ_cde,
lst_id,
lst_incln_typ_cde,
eff_dte,
expir_dte,
fil_lod_tmst,
btch_id
from `{{ params.offer_email_list_gold_table }}`
where expir_dte != '9999-12-31'
),

stage_only as
(
select
s_object_id as obj_ref_id,
s_object_type as obj_ref_typ_cde,
s_list_id as lst_id,
s_list_type as lst_incln_typ_cde,
current_date() as eff_dte,
PARSE_DATE('%Y-%m-%d',  '9999-12-31') as expir_dte,
--'9999-12-31' as expir_dte,
file_load_timestamp as fil_lod_tmst,
s_batch_id as btch_id
from full_outer
where g_obj_ref_id is null
),

gold_only as
(
select
g_obj_ref_id,
g_obj_ref_typ_cde,
g_lst_id,
g_lst_incln_typ_cde,
eff_dte,
current_date() as expir_dte,
g_fil_lod_tmst,
g_btch_id
from full_outer
where s_object_id is null
),

common as (
select
g_obj_ref_id,
g_obj_ref_typ_cde,
g_lst_id,
g_lst_incln_typ_cde,
eff_dte,
expir_dte,
g_fil_lod_tmst,
g_btch_id
from full_outer
where s_object_id is not null and g_obj_ref_id is not null
),

final AS (
select * from stage_only
UNION DISTINCT
select * from gold_only
UNION DISTINCT
select * from common
UNION DISTINCT
select * from expir_dte_not_null
)

SELECT * FROM final;



