# House Data Analysis with Apache PySpark and Airflow

## Project Overview

This project leverages Apache PySpark for large-scale data analytics and Apache Airflow for workflow orchestration. The goal is to analyze a housing dataset to derive insights into property pricing, square footage, house condition, and waterfront access. The analytics results are stored locally and uploaded to Google Drive for easy access.

## Table of Contents

- [Features](#features)
- [Tech Stack](#tech-stack)
- [Installation and Setup](#installation-and-setup)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
- [Data Flow](#data-flow)
- [Usage](#usage)
  - [Running the DAG](#running-the-dag)
  - [Viewing Outputs](#viewing-outputs)
- [Folder Structure](#folder-structure)
- [Contributing](#contributing)
- [License](#license)

---

## Features

1. **Price Statistics by Zipcode:** Aggregates price statistics (mean, max, min, and house count) for each zipcode.
2. **House Features Analysis:** Analyzes average price and square footage by the number of bedrooms.
3. **Condition and Grade Distribution:** Generates statistics on house condition and grade.
4. **Waterfront Analysis:** Analyzes waterfront properties, showing average price, count, and view score.

## Tech Stack

- **Apache PySpark** - For large-scale data processing.
- **Apache Airflow** - To manage and automate the ETL pipeline.
- **Google Drive API** - For uploading output files to Google Drive.
- **Python** - Core programming language.
- **Google OAuth2 Credentials** - For authenticating Google Drive API.

## Installation and Setup

### Prerequisites

1. **Python 3.8+**
2. **Apache Spark 3.x**
3. **Apache Airflow 2.x**
4. **Google Drive API Credentials**
   - Set up a project on Google Cloud and enable the Drive API.
   - Download `credentials.json` and save it as `token.json` in the Airflow DAG directory.
5. **Install required Python packages:**

   ```bash
   pip install apache-airflow apache-airflow-providers-google apache-spark google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client

## Configuration

### Airflow Setup
- Ensure Apache Airflow is correctly installed, configured, and running.
- Start Airflow’s web server and scheduler to enable DAG scheduling and monitoring.

### Google API Setup
- Set up Google Drive API on Google Cloud Console, and download the `credentials.json` file.
- Place this file in `/home/vboxuser/airflow/dags/` and rename it to `token.json` for authentication and Drive API integration.

### Data File
- Save the housing dataset as `house_data.dat` in `/home/vboxuser/airflow/dags/` for access by the PySpark tasks within Airflow.

## Data Flow

1. **DAG Setup**: Airflow runs the DAG daily, calling the `perform_analytics` task.
2. **Data Processing**: PySpark reads, cleans, and processes the data from `house_data.dat`.
3. **Analysis Generation**: The data undergoes various aggregations and analyses as defined in `perform_analytics()`.
4. **Local Storage**: Intermediate results are saved in CSV format in the `analytics` sub-directory.
5. **Consolidation**: Small CSV parts are consolidated into single CSV files for each analysis type.
6. **Google Drive Upload**: Consolidated CSV files are uploaded to Google Drive for easy access and sharing.

## Usage

### Running the DAG

1. Start the Airflow scheduler:
   ```bash
   airflow scheduler

## Usage

### Running the DAG

To trigger the DAG from the Airflow web UI, navigate to the "DAGs" section, find `home_schedule_dag`, and click on the "Trigger DAG" button. Alternatively, you can start it manually from the command line:

```bash
airflow dags trigger home_schedule_dag
```
## Viewing Outputs
The consolidated CSV files are saved locally in /home/vboxuser/airflow/dags/analytics/.
These files will also be uploaded to the specified Google Drive folder (configure the folder ID in the perform_analytics function within the DAG script).
