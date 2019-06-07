import datetime
import os
import sys
from airflow import DAG
from airflow.models import TriggerRule, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.operators.python_operator import PythonOperator


from utils.gs import get_bucket_from_name

import dag_generator
import params


if ".zip" not in __file__:
    if "properties" in sys.modules:
        del sys.modules["properties"]
    sys.path.insert(0, os.path.dirname(__file__))
import properties


batch_id = '''{{ ti.xcom_pull( key = ti.run_id ) }}'''

'''def get_batch_id():
    return int(Variable.get('batch_id', default_var=properties.unix_timestamp))'''

def generate_batch_id(**context):
    timestamp = int(Variable.get('batch_id', default_var=properties.unix_timestamp))
    print(timestamp)
    context['task_instance'].xcom_push(key=context['run_id'], value=timestamp)

def clear_xcom_bach_id(**context):
    hook = PostgresHook(postgres_conn_id='airflow_db', schema=None)
    xcom_key = context['run_id']
    sql = "delete from xcom where key= '{}'".format(xcom_key)
    hook.run(sql, autocommit=False)


params = params.generate_params(properties)

with DAG('bq_customer_loyalty',
         default_args=properties.default_args,
         schedule_interval=datetime.timedelta(days=1),
         catchup=False) as dag:

    inbound_bucket = params['inbound_bucket']
    inbound_dir = params['inbound_dir']
    google_coud_conn_id = params['google_cloud_conn_id']

    inbound_full_path = 'gs://%s/%s' % (inbound_bucket, inbound_dir)


    files_sensor_task = GoogleCloudStoragePrefixSensor(
        task_id="gs_sensor",
        google_cloud_conn_id=google_coud_conn_id,
        bucket=get_bucket_from_name(inbound_full_path),
        prefix=inbound_dir
    )

    generate_batch_id = PythonOperator(
        task_id='batch-id-gen',
        provide_context=True,
        python_callable=generate_batch_id
    )

    clear_xcom = PythonOperator(
        task_id='delete_xcom_bach_id',
        provide_context=True,
        python_callable=clear_xcom_bach_id,
        trigger_rule=TriggerRule.ALL_DONE
    )


    # OFFER_EMAIL_LIST
    offer_and_email_list = dag_generator.generate_dag(branch_name='offer_email_list', params=params, dag=dag)

    # OFFER_EMAIL_PROFILE
    offer_and_email_profile = dag_generator.generate_dag(branch_name='offer_email_profile', params=params, dag=dag)

    # OFFER
    offer = dag_generator.generate_dag(branch_name='offer', params=params, dag=dag)

    # LIST
    list = dag_generator.generate_dag(branch_name='list', params=params, dag=dag)

    # EMAIL_CAMPAIGN
    email_campaign = dag_generator.generate_dag(branch_name='email_campaign', params=params, dag=dag)


    finish_task = DummyOperator(
        task_id='ingest-loyalty-end-job',
        trigger_rule=TriggerRule.ALL_DONE,
    )


    files_sensor_task >> generate_batch_id
    generate_batch_id.set_downstream((offer_and_email_list['task_in'],
                                      offer_and_email_profile['task_in'],
                                      offer['task_in'],
                                      list['task_in'],
                                      email_campaign['task_in']
                                      ))

    clear_xcom.set_upstream((offer_and_email_list['task_out'],
                             offer_and_email_profile['task_out'],
                             offer['task_out'],
                             list['task_out'],
                             email_campaign['task_out'],
                            ))

    clear_xcom >> finish_task
