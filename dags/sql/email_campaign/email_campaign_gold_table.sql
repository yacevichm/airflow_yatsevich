with full_outer as (
select s.email_communication_id as s_email_communication_id,
s.communication_name as s_communication_name,
s.campaign_name as s_campaign_name,
s.start_date as s_start_date,
s.end_date as s_end_date,
FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as file_load_timestamp,
{{ ti.xcom_pull( key = ti.run_id ) }} as s_batch_id,
g.email_com_id as g_email_com_id,
g.email_com_nm as g_email_com_nm,
g.email_cmpgn_nm as g_email_cmpgn_nm,
g.email_com_strt_tmst as g_email_com_strt_tmst,
g.email_com_end_tmst as g_email_com_end_tmst,
g.email_cmpgn_stat_desc as g_email_cmpgn_stat_desc,
g.eff_dte,
g.expir_dte,
g.fil_lod_tmst as g_fil_lod_tmst,
g.btch_id as g_btch_id
from (select * from `{{ params.email_campaign_stage_table }}` where batch_id={{ ti.xcom_pull( key = ti.run_id ) }})  s
full outer join (select * from `{{ params.email_campaign_gold_table }}` where (expir_dte is null or expir_dte=PARSE_DATE('%Y-%m-%d',  '9999-12-31'))) g
on g.email_com_id = s.email_communication_id
and g.email_com_nm = s.communication_name
and g.email_cmpgn_nm = s.campaign_name
and CAST(g.email_com_strt_tmst as TIMESTAMP) = CAST(s.start_date as TIMESTAMP)
and CAST(g.email_com_end_tmst as TIMESTAMP) = CAST(s.end_date as TIMESTAMP)
),


end_dte_not_null as (
select
email_com_id,
email_com_nm,
email_cmpgn_nm,
email_com_strt_tmst,
email_com_end_tmst,
email_cmpgn_stat_desc,
eff_dte,
expir_dte,
fil_lod_tmst,
btch_id
from `{{ params.email_campaign_gold_table }}`
where expir_dte != PARSE_DATE('%Y-%m-%d',  '9999-12-31')
),


stage_only as
(
select
s_email_communication_id as email_com_id,
s_communication_name as email_com_nm,
s_campaign_name as email_cmpgn_nm,
s_start_date as email_com_strt_tmst,
s_end_date as email_com_end_tmst,
'Live' as email_cmpgn_stat_desc,
current_date() as eff_dte,
PARSE_DATE('%Y-%m-%d',  '9999-12-31') as expir_dte,
file_load_timestamp as fil_lod_tmst,
s_batch_id as btch_id
from full_outer
where g_email_com_id is null
),

gold_only as
(
select
g_email_com_id,
g_email_com_nm,
g_email_cmpgn_nm,
g_email_com_strt_tmst,
g_email_com_end_tmst,
'Completed' as email_cmpgn_stat_desc,
eff_dte,
current_date() as expir_dte,
g_fil_lod_tmst,
g_btch_id
from full_outer
where s_email_communication_id is null
),

common as
(
select
g_email_com_id,
g_email_com_nm,
g_email_cmpgn_nm,
g_email_com_strt_tmst,
g_email_com_end_tmst,
g_email_cmpgn_stat_desc,
eff_dte,
expir_dte,
g_fil_lod_tmst,
g_btch_id
from full_outer
where s_email_communication_id is not null and g_email_com_id is not null
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