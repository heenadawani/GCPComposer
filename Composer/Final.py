#don't use composer-1.7.5-airflow-1.9.0
from __future__ import print_function
import datetime
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow import DAG
import pandas as pd
from airflow.contrib.operators import gcs_to_bq
import os
os.system('pip install --upgrade pip')
os.system('sudo pip install gcsfs')
import gcsfs
from google.cloud import storage

default_dag_args = {
    'start_date': datetime.datetime(2018, 1, 1),
}

with models.DAG('Finalow',default_args=default_dag_args) as dag:
    def cleaning():
        df = pd.read_csv("gs://heena_dawani/P9-ConsoleGames.csv")
        df["Platform"].fillna("N/A", inplace = True) 
        df["NA_Sales"].fillna(0, inplace = True) 
        df["EU_Sales"].fillna(0, inplace = True) 
        df["JP_Sales"].fillna(0, inplace = True) 
        df["Other_Sales"].fillna(0, inplace = True) 
        new_data = df.dropna(axis = 0, how ='any')        
        new_data.to_csv("gs://heena_dawani/games.csv", index=False)

    Cleaning = python_operator.PythonOperator(
        task_id='Cleaning',
        python_callable=cleaning)

    GCS_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='GCS_to_BQ',
        bucket='heena_dawani',
        skip_leading_rows=1,
        autodetect=True,
        source_objects=["games.csv"],
        destination_project_dataset_table='heena_dawani.heena',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE'
        )
    Cleaning >> GCS_to_BQ
