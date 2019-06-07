from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from utils.gs import get_bucket_from_name, get_path_without_bucket_from_name
from google.cloud.bigquery import CreateDisposition, SourceFormat, WriteDisposition


def generate_dag(branch_name, params, dag):
    with dag:
        batch_id = params['batch_id']

        work_folder = params['%s_work_dir' % branch_name]
        inbound_bucket = params['inbound_bucket']
        inbound_dir = params['inbound_dir']
        offer_email_list_file_prefix = params['%s_file_prefix' % branch_name]

        inbound_full_path = 'gs://%s/%s' % (inbound_bucket, inbound_dir)


        move_files_to_work_directory = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id='move_%s_files_to_work_directory' % branch_name.lower(),
            source_bucket=inbound_bucket,
            source_object='%s/%s*.csv' % (inbound_dir, offer_email_list_file_prefix),
            destination_bucket=inbound_bucket,
            destination_object='%s/%s/%s' % (work_folder, batch_id, offer_email_list_file_prefix),
            google_cloud_storage_conn_id=params['google_cloud_conn_id'],
            dag=dag,
            move_object=params['move_object']
        )

        upload_to_bq_task = GoogleCloudStorageToBigQueryOperator(
            task_id='upload_%s_to_bq' % branch_name.lower(),
            bucket=get_bucket_from_name(inbound_full_path),
            source_objects=['%s/%s/*' % (work_folder, batch_id)],
            destination_project_dataset_table=params['%s_work_table' % branch_name],
            skip_leading_rows=0,
            source_format=SourceFormat.CSV,
            field_delimiter=',',
            schema_fields=params['%s_work_schema' % branch_name],
            create_disposition=CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            bigquery_conn_id=params['bigquery_conn_id'],
            google_cloud_storage_conn_id=params['google_cloud_conn_id'],
            autodetect=False,
            allow_jagged_rows=True,
            ignore_unknown_values=True,
            dag=dag
        )

        update_stage_table = BigQueryOperator(
            task_id='update_%s_stage_table' % branch_name.lower(),
            sql='sql/%s/%s_stage_table.sql' % (branch_name.lower(), branch_name.lower()),
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            create_disposition=CreateDisposition.CREATE_IF_NEEDED,
            destination_dataset_table=params['%s_stage_table' % branch_name],
            params=params,
            dag=dag
        )

        update_error_table = BigQueryOperator(
            task_id='update_%s_error_table' % branch_name.lower(),
            sql='sql/%s/%s_error_table.sql' % (branch_name.lower(), branch_name.lower()),
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            create_disposition=CreateDisposition.CREATE_IF_NEEDED,
            destination_dataset_table=params['%s_error_table' % branch_name],
            params=params,
            dag=dag
        )

        update_gold_table = BigQueryOperator(
            task_id='update_%s_gold_table' % branch_name.lower(),
            sql='sql/%s/%s_gold_table.sql' % (branch_name.lower(), branch_name.lower()),
            destination_dataset_table=params['%s_gold_table' % branch_name],
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            params=params,
            dag=dag
        )

        upload_to_bq_task >> update_error_table

        move_files_to_work_directory >> upload_to_bq_task >> update_stage_table >> update_gold_table

        return {'task_in': move_files_to_work_directory,
                'task_out': update_gold_table}