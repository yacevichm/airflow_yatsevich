with full_outer as
(
	select
		s.offer_id as s_offer_id,
		s.offer_name as s_offer_name,
		s.offer_description as s_offer_description,
		s.offer_headline as s_offer_headline,
		s.internal_note as s_internal_note,
		s.terms_and_conditions as s_terms_and_conditions,
		s.publish_start_date as s_publish_start_date,
		s.publish_end_date as s_publish_end_date,
		s.qualification_start_date as s_qualification_start_date,
		s.qualification_end_date as s_qualification_end_date,
		s.redemption_limit as s_redemption_limit,
		FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", CURRENT_TIMESTAMP()) as s_file_load_timestamp,
        {{ ti.xcom_pull( key = ti.run_id ) }} as s_batch_id,
		g.ofr_id as g_offer_id,
		g.ofr_nm as g_offer_name,
		g.ofr_desc as g_offer_description,
		g.ofr_hdlne_txt as g_offer_headline,
		g.intr_note_txt as g_internal_note,
		g.trms_and_cnd_txt as g_terms_and_conditions,
		g.publish_strt_tmst as g_publish_start_date,
		g.publish_end_tmst as g_publish_end_date,
		g.qlfn_strt_tmst as g_qualification_start_date,
		g.qlfn_end_tmst as g_qualification_end_date,
		g.redmn_lmt_qty as g_redemption_limit,
		g.ofr_stat_desc as g_ofr_stat_desc,
		g.eff_dte,
		g.expir_dte,
		g.btch_id as g_batch_id,
		g.fil_lod_tmst as g_fil_lod_tmst
	from
		(
			select
				offer_id,
				offer_name,
				offer_description,
				offer_headline,
				internal_note,
				terms_and_conditions,
				publish_start_date,
				publish_end_date,
				qualification_start_date,
				qualification_end_date,
				redemption_limit,
				file_load_timestamp,
				batch_id
			from `{{ params.offer_stage_table }}`
			where batch_id={{ ti.xcom_pull( key = ti.run_id ) }}
		)s
		full outer join
		(
			select
				ofr_id,
				ofr_nm,
				ofr_desc,
				ofr_hdlne_txt,
				intr_note_txt,
				trms_and_cnd_txt,
				publish_strt_tmst,
				publish_end_tmst,
				qlfn_strt_tmst,
				qlfn_end_tmst,
				redmn_lmt_qty,
				ofr_stat_desc,
				eff_dte,
				expir_dte,
				fil_lod_tmst,
				btch_id
			from `{{ params.offer_gold_table }}`
			where (expir_dte is null or expir_dte=PARSE_DATE('%Y-%m-%d',  '9999-12-31'))
		)g
			on  g.ofr_id = s.offer_id and
				g.ofr_nm = s.offer_name and
				g.ofr_desc = s.offer_description and
				g.ofr_hdlne_txt = s.offer_headline and
				g.intr_note_txt = s.internal_note and
				g.trms_and_cnd_txt = s.terms_and_conditions and
				g.publish_strt_tmst = s.publish_start_date and
				g.publish_end_tmst = s.publish_end_date and
				g.qlfn_strt_tmst = s.qualification_start_date and
				g.qlfn_end_tmst = s.qualification_end_date and
				g.redmn_lmt_qty = s.redemption_limit
),

expir_dte_not_default as
(
	select
	    ofr_id,
		ofr_nm,
		ofr_desc,
		ofr_hdlne_txt,
		intr_note_txt,
		trms_and_cnd_txt,
		publish_strt_tmst,
		publish_end_tmst,
		qlfn_strt_tmst,
		qlfn_end_tmst,
		redmn_lmt_qty,
		ofr_stat_desc,
		eff_dte,
		expir_dte,
		fil_lod_tmst,
		btch_id
	from `{{ params.offer_gold_table }}`
	where expir_dte != PARSE_DATE('%Y-%m-%d',  '9999-12-31')
),

stage_only as
(
	select
		s_offer_id as ofr_id,
		s_offer_name as ofr_nm,
		s_offer_description as ofr_desc,
		s_offer_headline as ofr_hdlne_txt,
		s_internal_note as intr_note_txt,
		s_terms_and_conditions as trms_and_cnd_txt,
		s_publish_start_date as publish_strt_tmst,
		s_publish_end_date as publish_end_tmst,
		s_qualification_start_date as qlfn_strt_tmst,
		s_qualification_end_date as qlfn_end_tmst,
		s_redemption_limit as redmn_lmt_qty,
		'Live' as ofr_stat_desc,
		current_date() as eff_dte,
		PARSE_DATE('%Y-%m-%d',  '9999-12-31') as expir_dte,
		s_file_load_timestamp as fil_lod_tmst,
		s_batch_id as btch_id
	from full_outer
	where g_offer_id is null
),

gold_only as
(
	select
		g_offer_id,
		g_offer_name,
		g_offer_description,
		g_offer_headline,
		g_internal_note,
		g_terms_and_conditions,
		g_publish_start_date,
		g_publish_end_date,
		g_qualification_start_date,
		g_qualification_end_date,
		g_redemption_limit,
		'Completed' as ofr_stat_desc,
		eff_dte,
		current_date() as expir_dte,
		g_fil_lod_tmst,
		g_batch_id
	from full_outer
	where s_offer_id is null
),

common as
(
	select
		g_offer_id,
		g_offer_name,
		g_offer_description,
		g_offer_headline,
		g_internal_note,
		g_terms_and_conditions,
		g_publish_start_date,
		g_publish_end_date,
		g_qualification_start_date,
		g_qualification_end_date,
		g_redemption_limit,
		g_ofr_stat_desc,
		eff_dte,
		expir_dte,
		g_fil_lod_tmst,
		g_batch_id
	from full_outer
	where s_offer_id is not null and g_offer_id is not null
),

final AS (
select * from stage_only
UNION DISTINCT
select * from gold_only
UNION DISTINCT
select * from common
UNION DISTINCT
select * from expir_dte_not_default
)

SELECT * FROM final;