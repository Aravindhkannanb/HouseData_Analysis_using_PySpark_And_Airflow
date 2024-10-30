from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import os

# Define the function that contains the data processing and model training code
def run_model_training():
    # Path to the CSV file
    file_path = '/home/vboxuser/airflow/dags/HouseData.csv'  # Replace with the path to your CSV file

    # Load the dataset
    df = pd.read_csv(file_path)

    # Preprocessing steps (same as the previous script)
    df['price'] = df['price'].replace('[\$,]', '', regex=True).astype(float)
    df['sqft_living'] = df['sqft_living'].replace('[\$,]', '', regex=True).astype(float)
    df['sqft_lot'] = df['sqft_lot'].replace('[\$,]', '', regex=True).astype(float)

    # Drop unnecessary columns
    df = df.drop(['id', 'date', 'zipcode', 'lat', 'long'], axis=1)

    
    label_encoder = LabelEncoder()
    df['waterfront'] = label_encoder.fit_transform(df['waterfront'])
    df['view'] = label_encoder.fit_transform(df['view'])
    df['condition'] = label_encoder.fit_transform(df['condition'])

    # Fill missing values
    df.fillna(df.median(), inplace=True)

    # Split data into features and target
    X = df.drop('price', axis=1)
    y = df['price']

    # Split into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Standardize the data
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train the Linear Regression model
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)

    # Make predictions
    y_pred = model.predict(X_test_scaled)

    # Evaluate the model
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Output results
    print(f'Price: {y_pred}')
    print(f'R² Score: {r2}')
   

    # Log the result to a file (or store it in a database)
    result_file_path = '/home/vboxuser/airflow/dags/results.txt'
    with open(result_file_path, 'w') as file:
        file.write(f'Mean Squared Error: {mse}\n')
        file.write(f'R² Score: {r2}\n')

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'linear_dag',  # Name of the DAG
    default_args=default_args,
    description='A simple DAG for house price prediction using linear regression',
    schedule_interval=timedelta(days=1),  # Run once a day (can be modified)
    start_date=datetime(2023, 1, 1),  # Set the start date
    catchup=False,  # Prevent backfilling
)

# Define the task that will execute the Python function
run_model_task = PythonOperator(
    task_id='run_model_training',
    python_callable=run_model_training,  # The Python function to execute
    dag=dag,  # The DAG this task belongs to
)

# If you have more tasks, define them here and set task dependencies
