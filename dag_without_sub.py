import time
from datetime import datetime
import os
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.subdag import SubDagOperator
from airflow.models import Variable

import requests
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

from joblib import dump


# DAG
weather_dag = DAG(
    dag_id='open_weather_dag',
    tags=['eval', 'datascientest'],
    description='OpenWeather API data collecting',
    doc_md="""# OpenWeather API best model

This `DAG` make the following action:

* data collection from the **OpenWeather API** and
store it in `JSON` files.
* convert data to `CSV` files (data.csv and fulldata.csv)
* train 3 models (Linear Regression, Decision Tree, Random Forest)

    """,
    schedule_interval="* * * * *",
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
    },
    catchup=False
)

###############################################################################
# T1
###############################################################################

Variable.set(key="cities", value=json.dumps({"cities" : ["paris", "london", "washington"]}))

def get_cities_weather():

    appid = "651fefd4972b4ca49622e521b52b0930"
    cities = Variable.get("cities", deserialize_json=True)
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    file = date_time + ".json"

    with open('/app/raw_files/' + file , 'w') as f:
        c=[]
        for city in cities["cities"]:
            print(city)
            url = "https://api.openweathermap.org/data/2.5/weather?q="+ city + "&appid=" + appid
            response = requests.get(url)
            c.append(response.json())
            time.sleep(1)
        
        json.dump(c,f,indent=4)


t1 = PythonOperator(
    task_id="collect_data",
    python_callable=get_cities_weather,
    doc_md="""# Task 1
Collecting data from OpenWeather API and store it in **JSON files** in /app/raw_files/
""",
    dag=weather_dag,
)

###############################################################################
# T2 and T3
###############################################################################

def transform_data_into_csv(n_files=None, filename='data.csv'):

    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        print(f)
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join('/app/clean_data', filename), index=False)


t2 = PythonOperator(
    task_id="data_to_csv_20_latest",
    doc_md="""# Task 2
Convert the last 20 files to one **CSV file** data.csv in /app/clean_data/
""",
    python_callable=transform_data_into_csv,
    op_kwargs={'n_files': 20, 'filename': 'data.csv'},
    dag=weather_dag,
)

t3 = PythonOperator(
    task_id="fulldata_to_csv",
    doc_md="""# Task 3
Convert all data in /app/raw_files to one **CSV file** fulldata.csv in /app/clean_data/
""",
    python_callable=transform_data_into_csv,
    op_kwargs={'filename': 'fulldata.csv'},
    dag=weather_dag,
)

t1 >> [t2,t3]

###############################################################################
# T4
###############################################################################

def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):

    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    # export df to tmp file
    try:
        df_final.to_csv('/app/clean_data/df_tmp.csv', index=False)
    except:
        return False
    
    return True

t4 = PythonOperator(
    task_id="prepare_data",
    doc_md="""# Task 4 Prepare data 
Transform dataset to be in Features and Label form
""",
    python_callable=prepare_data,
    dag=weather_dag,
    do_xcom_push=False
)


def compute_model_score(model, df_tmp_file, ti):

    df = pd.read_csv(df_tmp_file)
    X = df.drop(['target'], axis=1)
    y = df['target']    
    
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    ti.xcom_push(
        key="score",
        value=model_score
    )


t4_1 = PythonOperator(
    task_id="linear_regression_model",
    doc_md="""# Task 4.1 Linear Regression Model
Train full data on Linear Regression Model and returns score in XCom
""",
    python_callable=compute_model_score,
    op_kwargs={'model': LinearRegression(), 'df_tmp_file':'/app/clean_data/df_tmp.csv'},
    dag=weather_dag,
)

t4_2 = PythonOperator(
    task_id="decision_tree_model",
    doc_md="""# Task 4.2 Decision Tree Model
Train full data on Decision Tree Model and returns score in XCom
""",
    python_callable=compute_model_score,
    op_kwargs={'model': DecisionTreeRegressor(), 'df_tmp_file':'/app/clean_data/df_tmp.csv'},
    dag=weather_dag,
)

t4_3 = PythonOperator(
    task_id="random_forest_model",
    doc_md="""# Task 4.3 Random Forest Model
Train full data on Random Forest Model and returns score in XCom
""",
    python_callable=compute_model_score,
    op_kwargs={'model': RandomForestRegressor(), 'df_tmp_file':'/app/clean_data/df_tmp.csv'},
    dag=weather_dag,
)

t3 >> [t4]
t4 >> [t4_1, t4_2, t4_3]

###############################################################################
# T5
###############################################################################

def train_and_save_model(df_tmp_file, path_to_model, ti):

    # collecting all scores from XCom values
    scores = ti.xcom_pull(
            key="score",
            task_ids=['linear_regression_model','decision_tree_model','random_forest_model']
        )

    best_score_idx = scores.index(min(scores))
    if best_score_idx == 0:
        model = LinearRegression()
    elif best_score_idx == 1:
        model = DecisionTreeRegressor()
    else:
        model = RandomForestRegressor()

    # read dataframe from csv
    df = pd.read_csv(df_tmp_file)
    X = df.drop(['target'], axis=1)
    y = df['target']   

    # training the model
    model.fit(X, y)

    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)


t5 = PythonOperator(
    task_id="best_model_selection",
    doc_md="""# Task 5 Select Best Model
Select the best model and save it in a pickle file
""",
    python_callable=train_and_save_model,
    op_kwargs={'df_tmp_file': '/app/clean_data/df_tmp.csv',
               'path_to_model':'/app/clean_data/best_model.pickle'},
    dag=weather_dag
)

[t4_1, t4_2, t4_3] >> t5