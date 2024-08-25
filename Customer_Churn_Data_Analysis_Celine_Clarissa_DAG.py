'''
==========================================================================================================================================

Customer Churn Data Analysis

Name: Celine Clarissa

Original Dataset: https://www.kaggle.com/datasets/athu1105/book-genre-prediction/data


Background

A company providing products to customers must know the demographics and characteristics of their customers in order to minimize
customer churn.

Problem Statement and Objective

As a data analyst at a company, skills of understanding the market and extracting business insights from data are needed. By analyzing
customer churn data, it is possible to find out about user demographic, as well as their browsing behavior. After gaining information
from data, it is targeted for the company to strategize plans correlating to business insights. These insights are aimed to be displayed
in the form of a dashboard after 5 working days.

==========================================================================================================================================
'''

# import libraries
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

# import datetime
import datetime as dt
from datetime import timedelta

# import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# define function to fetch data from postgre
def FetchData():
    '''
    FetchData() function is used to fetch data from PostGreSQL, and save it into a csv file.
    This can be done by first connecting Python to PostGreSQL, then making a query and saving it into a csv file.
    '''
    # set connection to postgre database
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn=db.connect(conn_string)

    # make query using pandas
    df=pd.read_sql('select * from "P2M3_Celine_Clarissa_Data_Raw"', conn)
    df.to_csv('/opt/airflow/dags/P2M3_Celine_Clarissa_Data_Raw.csv')

# define function to clean data that was fetched from postgre
def CleanData(df):
    '''
    CleanData(df) function is used to clean the data that was fetched from PostGreSQL, and is now in csv form.
    This can be done by firstly read the file with pandas library, do cleaning (missing values handling, duplicated data handling,
    change column names), and then saving the cleaned data again into a csv file.
    '''
    # read csv and define dataframe
    df=pd.read_csv('/opt/airflow/dags/P2M3_Celine_Clarissa_Data_Raw.csv')

    # missing values handling

    ## make looping for all column names
    for i in df.columns:

        ## define missing values percentage
        percentage = df[i].isna().sum().sum()/len(df)*100

        ## make condition if missing values percentage is less than 5%
        if 0 < percentage < 5:

            ## drop missing values
            df = df.drop(list(df[df[i].isna()].index))

        ## make condition if missing values percentage is more than 5%
        else:

            ## define categorical and numerical columns
            categorical_columns = df.select_dtypes(include=['object', 'category']).columns
            numerical_columns = df.select_dtypes(include=['number']).columns

            ## make condition for numerical columns
            if i in numerical_columns:

                ## make condition for low skewness
                if df[i].skew() < 0.5:

                    ## fill missing values with mean
                    df[i] = df[i].fillna(df[i].mean())

                ## make condition for high skewness
                else:

                    ## fill missing values with median
                    df[i] = df[i].fillna(df[i].median())

            ## make condition for categorical columns
            elif i in categorical_columns:

                ## fill values with mode
                df[i] = df[i].fillna(df[i].mode([0]))

    # change column names to lowercase
    df.columns=[x.lower() for x in df.columns]

    # replace space with underscore
    df.columns = df.columns.str.replace(" ", "_")

    # drop duplicated data
    df = df.drop_duplicates()

    # save cleaned data to csv
    df.to_csv('/opt/airflow/dags/P2M3_Celine_Clarissa_Data_Clean.csv', index=False)

# define function to upload clean data to elasticsearch
def UploadData():
    '''
    UploadData() function is used to upload the previously cleaned data to ElasticSearch, to then be used to make visualizations on Kibana.
    '''
    # define elasticsearch
    es = Elasticsearch()

    # read csv and define dataframe
    df=pd.read_csv('/opt/airflow/dags/P2M3_Celine_Clarissa_Data_Clean.csv')

    # make looping to print data
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="celine", doc_type="doc", body=doc)
        print(res)

# define default_args
default_args = {
    'owner': 'Celine Clarissa',
    'start_date': dt.datetime(2024, 7, 19),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=15),
}

# define DAG
with DAG('P2M3CleanData',
         default_args=default_args,
         schedule_interval = '30 13 * * *'
         ) as dag:

    # define first task: fetch data from postgre
    fetchdata_task = PythonOperator(task_id='fetch',
                                 python_callable=FetchData)

    # define second task: clean fetched data
    cleandata_task = PythonOperator(task_id='clean',
                                 python_callable=CleanData)

    # define third task: upload cleaned data to elasticsearch
    uploaddata_task = BashOperator(task_id='upload',
                                 bash_command='cp /opt/airflow/dags/P2M3_Celine_Clarissa_Data_Clean.csv /opt/airflow/logs')
    
# set order of tasks
fetchdata_task >> cleandata_task >> uploaddata_task
