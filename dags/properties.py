# this is default properties file for local runs
import calendar
from datetime import datetime, timedelta

application_install_dir = '/tmp/'
config_home = "%s/config" % application_install_dir
app_version = '1.0.0-SNAPSHOT'
run_as_user = 'airflow'
queue_name = 'default'

alert_email = 'yatsevichm@gmail.com'

bigquery_conn_id = 'bigquery_default'
google_cloud_conn_id = 'google_cloud_default'

now = datetime.now()
unix_timestamp = calendar.timegm(now.utctimetuple())
date_time_now = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

batch_id = 1

inbound_bucket = "company_name-dev-infra-staging"
inbound_dir = 'loyalty/inbound'

failure_emails = 'bigdatadevops@company_name.com'
notiffy_emails = 'bigdatadevops@company_name.com'

project_id = 'company_name-ddh-lle'
dataset_id = 'company_name_dev_dp_customer_db'

move_object = False

#########################################################################################
#                        MIS_OfferAndEmailListExtract configs                           #
#########################################################################################

offer_email_list_work_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.work_loyalty_offer_email_list_tst'
offer_email_list_stage_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_offer_email_list_tst'
offer_email_list_error_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_offer_email_list_error_tst'
offer_email_list_gold_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.bq_gold_hct_llty_ofr_email_lst_tst'

offer_email_list_work_dir = 'loyalty/work/offer_email_list_work'
offer_email_list_file_prefix = 'Kohls_OfferAndEmailListExtract'

offer_email_list_work_schema = [{"name": "object_id", "type": "STRING", "mode": "NULLABLE"},
                                {"name": "object_type", "type": "STRING", "mode": "NULLABLE"},
                                {"name": "list_id", "type": "STRING", "mode": "NULLABLE"},
                                {"name": "list_type", "type": "STRING", "mode": "NULLABLE"}]

raw_loyalty_offer_email_list_shema = [{"name": "object_id", "type": "INTEGER", "mode": "NULLABLE"},
                                      {"name": "object_type", "type": "STRING", "mode": "NULLABLE"},
                                      {"name": "list_id", "type": "INTEGER", "mode": "NULLABLE"},
                                      {"name": "list_type", "type": "STRING", "mode": "NULLABLE"},
                                      {"name": "file_load_timestamp", "type": "STRING", "mode": "NULLABLE"},
                                      {"name": "batch_id", "type": "INTEGER", "mode": "NULLABLE"}]

#########################################################################################


#########################################################################################
#                        MIS_OfferAndEmailProfileExtract configs                        #
#########################################################################################

offer_email_profile_work_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.work_loyalty_offer_email_profile_tst'
offer_email_profile_stage_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_offer_email_profile_tst'
offer_email_profile_error_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_offer_email_profile_error_tst'
offer_email_profile_gold_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.bq_gold_hct_llty_ofr_email_prfl_tst'

offer_email_profile_work_dir = 'loyalty/work/offer_email_profile_work'
offer_email_profile_file_prefix = 'Kohls_OfferAndEmailProfileExtract'

offer_email_profile_work_schema = [{"name": "object_id", "type": "STRING", "mode": "NULLABLE"},
                                   {"name": "object_type", "type": "STRING", "mode": "NULLABLE"},
                                   {"name": "profile_id", "type": "STRING", "mode": "NULLABLE"},
                                   {"name": "profile_type", "type": "STRING", "mode": "NULLABLE"}]

#########################################################################################


#########################################################################################
#                             MIS_OfferExtract configs                                  #
#########################################################################################

offer_work_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.work_loyalty_offer_tst'
offer_stage_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_offer_tst'
offer_error_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_offer_error_tst'
offer_gold_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.bq_gold_hct_llty_ofr_tst'

offer_work_dir = 'loyalty/work/offer_work'
offer_file_prefix = 'Kohls_OfferExtract'

offer_work_schema = [{"name": "offer_id", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "offer_name", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "offer_description", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "offer_headline", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "internal_note", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "terms_and_conditions", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "publish_start_date", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "publish_end_date", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "qualification_start_date", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "qualification_end_date", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "redemption_limit", "type": "STRING", "mode": "NULLABLE"},
                     {"name": "event_id", "type": "STRING", "mode": "NULLABLE"}]

#########################################################################################


#########################################################################################
#                              MIS_ListExtract configs                                  #
#########################################################################################

list_work_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.work_loyalty_list_tst'
list_stage_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_list_tst'
list_error_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_list_error_tst'
list_gold_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.bq_gold_hct_llty_lst_tst'

list_work_dir = 'loyalty/work/list_work'
list_file_prefix = 'Kohls_ListExtract'

list_work_schema = [{"name": "list_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "list_name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "list_description", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "internal_note", "type": "STRING", "mode": "NULLABLE"}]

#########################################################################################

#########################################################################################
#                              Shopper_pii configs                                  #
#########################################################################################

shopper_tibco_file_pattern = 'LL_ShopperExport_test*'
shopper_copy_source_objects_path = 'tibco/loyalty/inbound/LL_ShopperExport_test*'
shopper_destination_objects_path = 'tibco/loyalty/inbound/work/{{ ti.xcom_pull( key = ti.run_id ) }}/LL_ShopperExport_test'
shopper_upload_objects_path = 'tibco/loyalty/inbound/work/{{ ti.xcom_pull( key = ti.run_id ) }}/LL_ShopperExport_test*'
shopper_work_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.work_loyalty_shopper_{{ ti.xcom_pull( key = ti.run_id ) }}'
shopper_stage_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.raw_loyalty_shopper'
shopper_stage_error_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.raw_loyalty_shopper_error'
shopper_gold_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.gold_hct_llty_shopr'
shopper_stage_table_id = 'raw_loyalty_shopper'
shopper_stage_table_error_id = 'raw_loyalty_shopper_error'

shopper_set_loyalty_member_expiry_id = 'set_loyalty_member_expiry'
shopper_set_shopper_status_expiry_id = 'set_shopper_status_expiry'
shopper_set_tier_name_expiry_id = 'set_tier_name_expiry'

#########################################################################################



#########################################################################################
#                             MIS_EmailCampaignExtract configs                          #
#########################################################################################

email_campaign_work_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.work_loyalty_email_campaign_tst'
email_campaign_stage_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_email_campaign_tst'
email_campaign_error_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.stage_loyalty_email_campaign_error_tst'
email_campaign_gold_table = 'company_name-ddh-lle.company_name_dev_dp_customer_db.bq_gold_hct_llty_email_cmpgn_tst'

email_campaign_work_dir = 'loyalty/work/email_campaign_work'
email_campaign_file_prefix = 'Kohls_EmailCampaignExtract'

email_campaign_work_schema = [
    {"name": "email_communication_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "communication_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "start_date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "end_date", "type": "STRING", "mode": "NULLABLE"},
]

#########################################################################################

default_args = {
    'owner': run_as_user,
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 3),
    'email': [alert_email],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'use_legacy_sql': False,
    'run_as_user': run_as_user,
    'write_disposition': 'WRITE_TRUNCATE',
    'bigquery_conn_id': bigquery_conn_id
}

default_params = {
    # config params
    'application_install_dir': application_install_dir,
    'app_version': app_version,
}

default_spark_conf = {
    "spark.yarn.report.interval": "60000",
}
