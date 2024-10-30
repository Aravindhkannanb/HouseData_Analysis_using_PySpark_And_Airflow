from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ensure no heavy operations outside tasks
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 16),
    'retries': 10,
}

# DAG definition: Lightweight and fast
dag = DAG(
    'home_schedule_dag',
    default_args=default_args,
    description='DAG to perform analytics with Spark',
    schedule_interval='@daily',
)

# Define the analytics task
def perform_analytics():
    # Imports moved inside the function to avoid global initialization
    import os
    import shutil
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import mean, max, min, count, regexp_replace, col
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    
    # Initialize Spark session inside the task
    spark = SparkSession.builder.appName("HouseDataAnalytics").getOrCreate()

    file_path = '/home/vboxuser/airflow/dags/house_data.dat'
    if not os.path.isfile(file_path):
        print(f"Error: File not found: {file_path}")
        return

    # Read CSV with custom schema (remapping column names)
    print("Reading the CSV file...")
    try:
        df = spark.read.csv(file_path, header=False, inferSchema=True, escape='"')
        df = df.withColumnRenamed('_c0', 'id') \
               .withColumnRenamed('_c1', 'date') \
               .withColumnRenamed('_c2', 'price') \
               .withColumnRenamed('_c3', 'bedrooms') \
               .withColumnRenamed('_c4', 'bathrooms') \
               .withColumnRenamed('_c5', 'sqft_living') \
               .withColumnRenamed('_c6', 'sqft_lot') \
               .withColumnRenamed('_c7', 'floors') \
               .withColumnRenamed('_c8', 'waterfront') \
               .withColumnRenamed('_c9', 'view') \
               .withColumnRenamed('_c10', 'condition') \
               .withColumnRenamed('_c11', 'grade') \
               .withColumnRenamed('_c12', 'yr_built') \
               .withColumnRenamed('_c13', 'zipcode') \
               .withColumnRenamed('_c14', 'lat') \
               .withColumnRenamed('_c15', 'long')

        # Clean and format the price column (removing commas, quotes, and spaces)
        df = df.withColumn('price', regexp_replace(col('price'), '[", ]', ''))
        df = df.withColumn('price', df['price'].cast('double'))
        
        print("CSV file loaded successfully!")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    # Show schema and first few rows to verify data
    print("DataFrame Schema:")
    df.printSchema()

    print("First few rows of the DataFrame:")
    df.show(5)

    if df.count() == 0:
        print("Error: DataFrame is empty. Check the data source.")
        return

    # Ensure the directory exists
    analytics_dir = '/home/vboxuser/airflow/dags/analytics'
    os.makedirs(analytics_dir, exist_ok=True)

    # Define analytics output paths
    price_stats_path = os.path.join(analytics_dir, 'price_stats_by_zipcode')
    house_features_path = os.path.join(analytics_dir, 'house_features_analysis')
    condition_distribution_path = os.path.join(analytics_dir, 'condition_distribution')
    waterfront_analysis_path = os.path.join(analytics_dir, 'waterfront_analysis')

    # 1. Price statistics by zipcode
    print("Calculating price statistics by zipcode...")
    try:
        price_stats = df.groupBy('zipcode').agg(
            mean('price').alias('avg_price'),
            max('price').alias('max_price'),
            min('price').alias('min_price'),
            count('price').alias('house_count')
        )
        price_stats.write.mode('overwrite').csv(price_stats_path, header=True)
        print(f"Price statistics saved to {price_stats_path}")
    except Exception as e:
        print(f"Error calculating price statistics: {e}")

    # 2. House features analysis
    print("Analyzing house features...")
    try:
        house_features = df.groupBy('bedrooms').agg(
            mean('price').alias('avg_price'),
            mean('sqft_living').alias('avg_sqft'),
            count('*').alias('count')
        )
        house_features.write.mode('overwrite').csv(house_features_path, header=True)
        print(f"House features analysis saved to {house_features_path}")
    except Exception as e:
        print(f"Error analyzing house features: {e}")

    # 3. Condition and grade distribution
    print("Analyzing condition and grade distribution...")
    try:
        condition_distribution = df.groupBy('condition', 'grade').count()
        condition_distribution.write.mode('overwrite').csv(condition_distribution_path, header=True)
        print(f"Condition and grade distribution saved to {condition_distribution_path}")
    except Exception as e:
        print(f"Error analyzing condition distribution: {e}")

    # 4. Waterfront property analysis
    print("Analyzing waterfront properties...")
    try:
        waterfront_analysis = df.groupBy('waterfront').agg(
            mean('price').alias('avg_price'),
            count('*').alias('count'),
            mean('view').alias('avg_view_score')
        )
        waterfront_analysis.write.mode('overwrite').csv(waterfront_analysis_path, header=True)
        print(f"Waterfront analysis saved to {waterfront_analysis_path}")
    except Exception as e:
        print(f"Error analyzing waterfront properties: {e}")

    # Function to consolidate part files into a single CSV
    def consolidate_csv(directory, output_file):
        print(f"Consolidating CSV files from {directory} into {output_file}...")
        try:
            with open(output_file, 'wb') as outfile:
                for filename in os.listdir(directory):
                    if filename.endswith('.csv'):
                        file_path = os.path.join(directory, filename)
                        with open(file_path, 'rb') as f:
                            shutil.copyfileobj(f, outfile)
            print(f"Consolidated files into {output_file}")
        except Exception as e:
            print(f"Error consolidating CSV files: {e}")

    # Consolidate files
    consolidate_csv(price_stats_path, os.path.join(analytics_dir, 'price_stats_by_zipcode_final.csv'))
    consolidate_csv(house_features_path, os.path.join(analytics_dir, 'house_features_analysis_final.csv'))
    consolidate_csv(condition_distribution_path, os.path.join(analytics_dir, 'condition_distribution_final.csv'))
    consolidate_csv(waterfront_analysis_path, os.path.join(analytics_dir, 'waterfront_analysis_final.csv'))

    # Google Drive integration
    print("Integrating with Google Drive...")
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None
    try:
        creds = Credentials.from_authorized_user_file('/home/vboxuser/airflow/dags/token.json', SCOPES)
        service = build('drive', 'v3', credentials=creds)
        print("Google Drive authentication successful!")
    except Exception as e:
        print(f"Error authenticating with Google Drive: {e}")
        return

    folder_id = '1_g7WmLJAEKDqSJXGF7e0k5j7WO-LVkRT'

    def delete_old_files():
        print("Deleting old files from Google Drive folder...")
        try:
            results = service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name)").execute()
            items = results.get('files', [])
            if not items:
                print("No old files found to delete.")
            else:
                for item in items:
                    service.files().delete(fileId=item['id']).execute()
                    print(f"Deleted file: {item['name']} with ID: {item['id']}")
        except Exception as e:
            print(f"Error deleting files: {e}")

    delete_old_files()

    # Upload consolidated CSV files to Google Drive
    final_csv_files = [
        os.path.join(analytics_dir, 'price_stats_by_zipcode_final.csv'),
        os.path.join(analytics_dir, 'house_features_analysis_final.csv'),
        os.path.join(analytics_dir, 'condition_distribution_final.csv'),
        os.path.join(analytics_dir, 'waterfront_analysis_final.csv')
    ]

    print("Uploading consolidated CSV files to Google Drive...")
    for file_path in final_csv_files:
        if os.path.isfile(file_path) and file_path.endswith(".csv"):
            file_name = os.path.basename(file_path)
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            media = MediaFileUpload(file_path, mimetype='text/csv')
            try:
                service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                print(f"Uploaded {file_name} to Google Drive.")
            except Exception as e:
                print(f"Error uploading {file_name} to Google Drive: {e}")

# Create a task to run the `perform_analytics` function
perform_analytics_task = PythonOperator(
    task_id='perform_analytics',
    python_callable=perform_analytics,
    dag=dag,
)

# Set the task dependency
perform_analytics_task
