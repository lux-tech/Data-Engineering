from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from helpers import SqlQueries, DataQualityQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@monthly',
    max_active_runs = 1   
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_src_bucket='udacity-dend',
    s3_src_pattern='log_data/{execution_date.year}/{execution_date.month}/',
    data_format='json',
    jsonpaths='s3://udacity-dend/log_json_path.json',
    copy_opts="timeformat 'epochmillisecs'",
    ignore_header=0
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    execution_timeout=timedelta(minutes=10),
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_src_bucket='udacity-dend',
    s3_src_pattern='song_data',
    data_format='json',
    ignore_header=0
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="songplays",
    sql_select_stmt=SqlQueries.songplay_table_insert,
    update_mode="overwrite"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_select_stmt=SqlQueries.user_table_insert,
    update_mode="overwrite"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_select_stmt=SqlQueries.song_table_insert,
    update_mode="overwrite"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_select_stmt=SqlQueries.artist_table_insert,
    update_mode="overwrite"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_select_stmt=SqlQueries.time_table_insert,
    update_mode="overwrite"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_queries=[
        DataQualityQueries.table_not_empty("staging_songs"),
        DataQualityQueries.table_not_empty("staging_events"),
        DataQualityQueries.col_does_not_contain_null("staging_songs", "song_id"),
        DataQualityQueries.col_does_not_contain_null("staging_songs", "artist_id")
    ],
    test_results = [
        DataQualityQueries.table_not_empty_test,
        DataQualityQueries.table_not_empty_test,
        DataQualityQueries.col_does_not_contain_null_test,
        DataQualityQueries.col_does_not_contain_null_test
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator