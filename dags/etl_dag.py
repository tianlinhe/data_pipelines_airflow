from datetime import date, datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (LoadFactOperator,StageToRedshiftOperator, 
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import create_tables

# No need for AWS_KEY or AWS_SECRET here,
# because we parsed the credentials with AwsHook from WebUI
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'tianlinhe',
    'start_date': datetime(2020,11,6), #start from yesterday
    'depends_on_past':False, #DAG does not have dependencies on past runs
    'catchup':False, #do not Perform scheduler catchup (or only run latest)?
    'email':['tianlinhe@example.com'], #example email(s) in list
    'email_on_failure':False, #do not email me on failure
    'email_on_retry':False, #do not email me on retry
    'retries':3, # retry 3 times
    'retry_delay':timedelta(minutes=5) #Retries happen every 5 minutes

}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@daily" #so that it run once only
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create all the tables before copy and insert
# 'create_tables'
# or, manually run codes in create_tables.sql with query editor in Redshift
create_tables_task = PostgresOperator(
  task_id='create_tables',
  dag=dag,
  sql=create_tables.sql_stmt,
  postgres_conn_id='redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentilas_id='aws_credentails',
    target_table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data'
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentilas_id='aws_credentails',
    target_table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='songplays',
    sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='users',
    sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='songs',
    sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='artists',
    sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    target_table='time',
    sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    target_table=['users','songs','artists','time'],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# define order of DAGs
start_operator >> create_tables_task

create_tables_task >> stage_songs_to_redshift
create_tables_task >> stage_events_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
