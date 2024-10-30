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
