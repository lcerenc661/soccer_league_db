from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import snowflake.connector as sf
import pandas as pd
from utils import get_data, data_processing


default_arguments = {'owner': 'Luis_Ceren',
                     'retries': 1, 
                     'retry_delay': timedelta(minutes=5)}


with DAG('SOCCER_LEAGUES',
         default_args = default_arguments,
         description = 'Extracting Data Soccer League info', 
         start_date = datetime(2023, 6, 7), 
         schedule_interval = None, 
         tags=['ESPN'], 
         catchup=False
         ) as dag:
    
    
        params_info = Variable.get("feature_info", deserialize_json=True) #Read enviroment variables
        df = pd.read_csv('/usr/local/airflow/df_leagues.csv')
        df_teams = pd.read_csv('/usr/local/airflow/df_teams.csv')
        
        def extract_info(df, df_teams, **kwargs):
            
            df_data = data_processing(df)
            
            df_final = pd.merge(df_data, df_teams, how='inner', on='Team')
            df_final = df_final[['Team_id', 'Team','G','W','D','L','GS','GC','DIF','PTS','League','Loading_date' ]]
            
            df_final.to_csv('./premier_positions.csv', index=False)
            
            
            
        extract_data = PythonOperator(task_id='extract_soccer_data',
                                      provide_context=True,
                                      python_callable=extract_info,
                                      op_kwargs={"df":df,"df_teams":df_teams}
                                      )
        
        upload_stage = SnowflakeOperator( task_id='upload_data_stage',
                                         sql='./queries/upload_stage.sql',
                                         snowflake_conn_id='airflow_snowflake',
                                         warehouse=params_info["DWH"],
                                         database=params_info["DB"],
                                         role=params_info['ROLE'],
                                         params=params_info
                                         )
        
        ingest_table = SnowflakeOperator( task_id='ingest_data_table',
                                         sql='./queries/upload_table.sql',
                                         snowflake_conn_id='airflow_snowflake',
                                         warehouse=params_info["DWH"],
                                         database=params_info["DB"],
                                         role=params_info['ROLE'],
                                         params=params_info
                                         )
        
        extract_data >> upload_stage  >> ingest_table
            
    
    
    