from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
import pandas as pd

def subdag(parent_dag_id):
    """
    Generate a DAG to be used as a subdag for task 4
    """
    dag_subdag = DAG(
        dag_id=f'{parent_dag_id}.train_models',
        start_date=days_ago(0),
        catchup=False
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


    models = [LinearRegression(),DecisionTreeRegressor(),RandomForestRegressor()]

    for i in range(len(models)):
        PythonOperator(
            task_id=f'{str(models[i]).strip("()")}',
            doc_md="""# Task 4.""" + str(i+1) + """ """ + str(models[i]) + """
            Train full data on Model and returns score in XCom
            """,
            python_callable=compute_model_score,
            op_kwargs={'model': models[i], 'df_tmp_file':'/app/clean_data/df_tmp.csv'},
            dag=dag_subdag,
        )

    return dag_subdag