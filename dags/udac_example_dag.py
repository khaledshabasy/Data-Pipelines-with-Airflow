from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.subdag_operator import SubDagOperator
# from load_dimension_subdag import load_dimension_dag
from helpers import SqlQueries
# from helpers import CreateTables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

tables = ["staging_events", "staging_songs", "users",
          "songs", "artists", "time", "songplays"]

start_date = datetime(2019, 1, 12)
default_args = {
    'owner': 'KhShabasy',
    'start_date': start_date,
    'depends_on_past': False,
    'email':['khaledshabasy96@gmail.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('project6_pipeline_automation_dag',
          default_args=default_args,
          max_active_runs=1,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
#           catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    cols='TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL',
#     file_format="CSV"
#     create_stmt=CreateTables.staging_events_table_create
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song-data/A/A/A/',
    cols='',
#     file_format="JSON",
#     create_stmt=CreateTables.staging_songs_table_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    insert_stmt=SqlQueries.songplay_table_insert,
#     create_stmt=CreateTables.songplay_table_create
    
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="users",
    insert_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="songs",
    insert_stmt=SqlQueries.song_table_insert
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="artists",
    insert_stmt=SqlQueries.artist_table_insert
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="time",
    insert_stmt=SqlQueries.time_table_insert
)




run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table=tables,
    dq_checks=[{'dq_query' : 'SELECT COUNT (*) FROM public.songplays WHERE playid IS NULL', 'exp_result' : 0},
               {'dq_query' : 'SELECT COUNT (*) FROM public.users WHERE userid IS NULL', 'exp_result' : 0},
               {'dq_query' : 'SELECT COUNT (*) FROM public.songs WHERE songid IS NULL', 'exp_result' : 0},
               {'dq_query' : 'SELECT COUNT (*) FROM public.artists WHERE artistid IS NULL', 'exp_result' : 0},
               {'dq_query' : 'SELECT COUNT (*) FROM public.time WHERE start_time IS NULL', 'exp_result' : 0},
               {'dq_query' : 'SELECT COUNT (DISTINCT "level") FROM public.songplays', 'exp_result' : 2}
              ]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, 
                         load_song_dimension_table, 
                         load_artist_dimension_table, 
                         load_time_dimension_table]

[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator