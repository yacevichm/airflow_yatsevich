from airflow.models import Variable


def generate_params(properties):
    batch_id = '''{{ ti.xcom_pull( key = ti.run_id ) }}'''

    params = {
        # GENERAL CONFIG
        'batch_id': Variable.get('BATCH_ID', default_var=batch_id),
        'inbound_bucket': Variable.get('inbound_bucket', default_var=properties.inbound_bucket),
        'inbound_dir': Variable.get('inbound_dir', default_var=properties.inbound_dir),
        'google_cloud_conn_id': Variable.get('google_cloud_conn_id', default_var=properties.google_cloud_conn_id),
        'bigquery_conn_id': Variable.get('bigquery_conn_id', default_var=properties.bigquery_conn_id),
        'run_as_user': Variable.get('run_as_user', default_var=properties.run_as_user),
        'project_id' : Variable.get('project_id', default_var=properties.project_id),
        'dataset_id' : Variable.get('dataset_id', default_var=properties.dataset_id),
        'move_object': Variable.get('move_object', default_var=properties.move_object),

        # OFFER_EMAIL_LIST
        'offer_email_list_work_dir': Variable.get('offer_email_list_work_dir',
                                                  default_var=properties.offer_email_list_work_dir),
        'offer_email_list_file_prefix': Variable.get('offer_email_list_file_prefix',
                                                     default_var=properties.offer_email_list_file_prefix),
        'offer_email_list_work_schema': Variable.get('offer_email_list_work_schema',
                                                     default_var=properties.offer_email_list_work_schema),
        'offer_email_list_work_table': Variable.get('offer_email_list_work_table',
                                                    default_var=properties.offer_email_list_work_table),
        'offer_email_list_stage_table': Variable.get('offer_email_list_stage_table',
                                                     default_var=properties.offer_email_list_stage_table),
        'offer_email_list_error_table': Variable.get('offer_email_list_error_table',
                                                     default_var=properties.offer_email_list_error_table),
        'offer_email_list_gold_table': Variable.get('offer_email_list_gold_table',
                                                    default_var=properties.offer_email_list_gold_table),

        # OFFER_EMAIL_PROFILE
        'offer_email_profile_work_dir': Variable.get('offer_email_profile_work_dir',
                                                     default_var=properties.offer_email_profile_work_dir),
        'offer_email_profile_file_prefix': Variable.get('offer_email_profile_file_prefix',
                                                        default_var=properties.offer_email_profile_file_prefix),
        'offer_email_profile_work_schema': Variable.get('offer_email_profile_work_schema',
                                                        default_var=properties.offer_email_profile_work_schema),
        'offer_email_profile_work_table': Variable.get('offer_email_profile_work_table',
                                                       default_var=properties.offer_email_profile_work_table),
        'offer_email_profile_stage_table': Variable.get('offer_email_profile_stage_table',
                                                        default_var=properties.offer_email_profile_stage_table),
        'offer_email_profile_error_table': Variable.get('offer_email_profile_error_table',
                                                        default_var=properties.offer_email_profile_error_table),
        'offer_email_profile_gold_table': Variable.get('offer_email_profile_gold_table',
                                                       default_var=properties.offer_email_profile_gold_table),

        # OFFER
        'offer_work_dir': Variable.get('offer_work_dir',
                                       default_var=properties.offer_work_dir),
        'offer_file_prefix': Variable.get('offer_file_prefix',
                                          default_var=properties.offer_file_prefix),
        'offer_work_schema': Variable.get('offer_work_schema',
                                          default_var=properties.offer_work_schema),
        'offer_work_table': Variable.get('offer_work_table',
                                         default_var=properties.offer_work_table),
        'offer_stage_table': Variable.get('offer_stage_table',
                                          default_var=properties.offer_stage_table),
        'offer_error_table': Variable.get('offer_error_table',
                                          default_var=properties.offer_error_table),
        'offer_gold_table': Variable.get('offer_gold_table',
                                         default_var=properties.offer_gold_table),

        # LIST
        'list_work_dir': Variable.get('list_work_dir',
                                      default_var=properties.list_work_dir),
        'list_file_prefix': Variable.get('list_file_prefix',
                                         default_var=properties.list_file_prefix),
        'list_work_schema': Variable.get('list_work_schema',
                                         default_var=properties.list_work_schema),
        'list_work_table': Variable.get('list_work_table',
                                        default_var=properties.list_work_table),
        'list_stage_table': Variable.get('list_stage_table',
                                         default_var=properties.list_stage_table),
        'list_error_table': Variable.get('list_error_table',
                                         default_var=properties.list_error_table),
        'list_gold_table': Variable.get('list_gold_table',
                                        default_var=properties.list_gold_table),

        # EMAIL_CAMPAIGN
        'email_campaign_work_dir': Variable.get('email_campaign_work_dir',
                                                default_var=properties.email_campaign_work_dir),
        'email_campaign_file_prefix': Variable.get('email_campaign_file_prefix',
                                                   default_var=properties.email_campaign_file_prefix),
        'email_campaign_work_schema': Variable.get('email_campaign_work_schema',
                                                   default_var=properties.email_campaign_work_schema),
        'email_campaign_work_table': Variable.get('email_campaign_work_table',
                                                  default_var=properties.email_campaign_work_table),
        'email_campaign_stage_table': Variable.get('email_campaign_stage_table',
                                                   default_var=properties.email_campaign_stage_table),
        'email_campaign_error_table': Variable.get('email_campaign_error_table',
                                                   default_var=properties.email_campaign_error_table),
        'email_campaign_gold_table': Variable.get('email_campaign_gold_table',
                                                  default_var=properties.email_campaign_gold_table),

    }

    return params
