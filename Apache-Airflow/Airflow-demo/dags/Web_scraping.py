import pandas as pd 
import numpy as np
import requests 
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta

def scraping():
    # Scrap the Webpage
    url="https://www.flipkart.com/search?q=iphone&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off"
    response= requests.get(url)
    # Clean the Webpage using Beautifulsoup
    soup= BeautifulSoup(response.text,'lxml')

    brand=[]
    price=[]
    # Extract the Mobile name 
    name = soup.find_all('div',class_='KzDlHZ')
    for i in name:
        brand_name= i.text.strip()
        brand.append(brand_name)
    # Extraxt the mobile price
    price= soup.find_all('div',class_='Nx9bqj _4b5DiR')
    for j in price:
        raw_price= j.test.strip()
        clean_price= int(raw_price.replace('₹','').replace(',',''))
        price.append(clean_price)

    # Create a dataframe
    data= {'Brand':brand,f"Price at{datetime.now().strftime("%H:%M")}":price}
    df=pd.DataFrame(data)

    # Save the DataFrame in local directory
    path= r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\iphone_price.csv"
    try:
        old_file=pd.read_csv(path)
        # Append the price data each time when update
        merge_file=pd.merge(old_file,df,on='Bran',how="left")
    except FileNotFoundError:
        merge_file=df
    merge_file.to_csv(path, index=False)


# Norify when Iphone price at its lowest
def minimum_price():
    # Load existing file
    file_path= r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\iphone_price.csv"
    file= pd.read_csv(file_path)

    # Extract only Price column
    price_columns = [col for col in file.columns if col.startswith("Price at")]
    # Create a new DataFrame with "Brand" and the minimum price
    new_df = file[["Brand"]].copy()  # Retain only the "Brand" column
    new_df["Minimum Price"] = file[price_columns].min(axis=1)  # Calculate the minimum price per row

    # Save the result back to the CSV or a new file
    new_file_path = r"B:\Major_Git\Apache- Airflow\Airflow-demo\dags\iphone_price_min.csv"
    new_df.to_csv(new_file_path, index=False)

# Create the Directed Asycylic Graph- Which will create a workflow to complete a task 
with DAG(
    dag_id='Iphone_Scraping',
    start_date=datetime(2025,6,24),
    schedule_interval="* * * * *", # This will schedule for minutest
    catchup=False
    )as dag:

    # Task Call - Sequential
    scraping_task= PythonOperator(
        task_id='iphone_scraping',
        python_callable=scraping
    )

    minimum_price_task=PythonOperator(
        task_id='minimum_price',
        python_callable=minimum_price
    )
    # Workflow-
    scraping_task >> minimum_price_task
