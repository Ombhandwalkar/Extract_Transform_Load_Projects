from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 

import pandas as pd 
import numpy as np 
import pickle 
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib

# Extract the data from local directory.
def extract_data():
    try:
        path = r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\flights.csv"
        df = pd.read_csv(path)
        df.drop_duplicates(inplace = True)
        df.dropna(inplace = True)
        df.to_csv(r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\extract_data.csv", index = False)
        print("The data is extracted successfully")
    except Exception as e:
        print(f"there is problem in extracting the data : {e}")

def transform_data():
    try:
        path = r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\extract_data.csv"
        df = pd.read_csv(path)
        df.drop(columns = ['travelCode', 'userCode', 'date', 'time', 'distance'], inplace = True)
        encoded_df = pd.get_dummies(df, columns = ['from', 'to', 'flightType', 'agency'], drop_first = True)
        encoded_df.to_pickle(r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\transformed_data.pkl")
        print("The data is Transformed Successfully")
    except Exception as e:
        print(f"There is error in transforming the data: {e}")

def train_model():
    try:
        path=r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\transformed_data.pkl"
        df= pd.read_pickle(path)
        x= df.drop(columns=['price'])
        y=df['price']

        X_train,X_test,y_train,y_test= train_test_split(X,y,test_size=0.2, random_state=42)

        rf= RandomForestRegressor()
        rf.fit(X_train,y_train)

        joblib.dump(r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\trained_model.pkl")
        print("the model have been saved successfully")
    except Exception as e:
        print(f"There is error while training the model {e}")

def evaluation():
    try:
        path=r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\transformed_data.pkl"
        df=pd.read_pickle(path)

        x=df.drop(columns=['price'])
        y=df['price']
        X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.2, random_state=42)

        model=joblib.load(r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\trained_model.pkl")
        y_predit= model.predict(X_test)

        mae= mean_absolute_error(y_test,y_predit)
        mse=mean_squared_error(y_test,y_predit)
        r2=r2_score(y_test,y_predit)
        
        print(f"the mae is {mae}, mse is {mse} and r2_score is {r2}")
        print("the evalution is completed")

    except Exception as e:
        print(f"There is some error in evalution step {e}")

default_args={
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='price_prediction_model',
    default_args=default_args,
    start_date=datetime(2025,6,24),
    schedule_interval='@daily',
    catchup=False
)as dag:
    
    
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    task_train = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )

    task_evaluate = PythonOperator(
        task_id='evaluation',
        python_callable=evaluation
    )

    # Define task dependencies
    task_extract >> task_transform >> task_train >> task_evaluate


