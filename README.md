House Data Analysis with Apache PySpark and Airflow
Project Overview
This project uses Apache PySpark for data analytics and Apache Airflow for workflow orchestration. The objective is to analyze a housing dataset to derive insights on various features, including property pricing, square footage, house condition, and waterfront access. Consolidated analysis results are stored locally and uploaded to Google Drive for easy access.

Table of Contents
Project Overview
Features
Tech Stack
Installation and Setup
Data Flow
Usage
Folder Structure
Contributing
License
Features
Price Statistics by Zipcode: Aggregates price statistics (mean, max, min, and house count) for each zipcode.
House Features Analysis: Analyzes average price and square footage by the number of bedrooms.
Condition and Grade Distribution: Generates statistics on house condition and grade.
Waterfront Analysis: Analyzes waterfront properties, showing average price, count, and view score.
Tech Stack
Apache PySpark - For large-scale data processing.
Apache Airflow - To manage and automate the ETL pipeline.
Google Drive API - For uploading output files to Google Drive.
Python - Core programming language.
Google OAuth2 Credentials - For authenticating Google Drive API.
Installation and Setup
Prerequisites
Python 3.8+

Apache Spark 3.x

Apache Airflow 2.x

Google Drive API Credentials

Set up a project on Google Cloud and enable the Drive API.
Download credentials.json and save it as token.json in the Airflow DAG directory.
Install required Python packages:

bash
Copy code
pip install apache-airflow apache-airflow-providers-google apache-spark google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
Configuration
Airflow Setup: Ensure Airflow is correctly set up and running.

Google API Setup: Copy token.json (from Google Cloud Console) to /home/vboxuser/airflow/dags/ for Drive API integration.

Data File: Save the housing dataset as house_data.dat in /home/vboxuser/airflow/dags/.

Data Flow
DAG Setup: Airflow runs the DAG daily, calling the perform_analytics task.
Data Processing: PySpark reads, cleans, and processes the data.
Analysis Generation: The data undergoes various aggregations and analyses as defined in perform_analytics().
Local Storage: Intermediate results are saved in CSV format in analytics sub-directory.
Consolidation: Small CSV parts are consolidated into single CSV files for each analysis type.
Google Drive Upload: Consolidated CSV files are uploaded to Google Drive for easy access and sharing.
Usage
Running the DAG
Start the Airflow scheduler:

bash
Copy code
airflow scheduler
Trigger the DAG from the Airflow web UI or start it manually:

bash
Copy code
airflow dags trigger home_schedule_dag
Viewing Outputs
The consolidated CSV files are saved in /home/vboxuser/airflow/dags/analytics/.
They will be uploaded to Google Drive under the specified folder (configure the folder ID in perform_analytics).
Folder Structure
plaintext
Copy code
project-root/
├── dags/
│   ├── house_data.dat                    # Data file
│   ├── analytics/                        # Output analytics directory
│   ├── token.json                        # Google API token file
│   ├── house_data_analysis_dag.py        # Airflow DAG file (this script)
└── README.md                             # Project documentation
Contributing
Fork the repository.
Create a new feature branch.
Commit your changes.
Open a pull request.
License
This project is licensed under the MIT License.
