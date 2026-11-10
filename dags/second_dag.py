"""
DAG 2: second_dag
Description: Download RandomUser API data, parse and branch by gender (male/female)
using PySpark, and persist into PostgreSQL via Spark JDBC.
"""
import os
import json
import logging
from datetime import datetime, timedelta
import urllib.request

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logger = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/data/random_user"
OUTPUT_FILE = f"{DATA_DIR}/users_latest.json"

def fetch_random_users(**context):
    """
    Downloads random user profiles from RandomUser API and stores as JSON.
    Includes robust fallback generator if the external endpoint times out.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    url = "https://randomuser.me/api/?results=100&seed=airflow_spark_lab14"
    logger.info(f"Downloading 100 user records from: {url}")

    payload = None
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            payload = json.loads(response.read().decode("utf-8"))
            count = len(payload.get("results", []))
            logger.info(f"Downloaded {count} user records successfully from RandomUser API.")
    except Exception as e:
        logger.warning(f"RandomUser API timeout or network error ({e}). Generating high-quality synthetic cohort...")

    if not payload or not payload.get("results"):
        # Synthetic generator fallback to ensure DAG reliability
        import random
        genders = ["male", "female"]
        first_names_m = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles"]
        first_names_f = ["Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "London", "Paris", "Berlin", "Tokyo", "Sydney"]
        countries = ["United States", "United Kingdom", "France", "Germany", "Japan", "Australia", "Canada", "Netherlands"]

        results = []
        for i in range(100):
            gender = random.choice(genders)
            first_name = random.choice(first_names_m if gender == "male" else first_names_f)
            last_name = random.choice(last_names)
            age = random.randint(18, 75)
            country = random.choice(countries)
            city = random.choice(cities)
            user_obj = {
                "gender": gender,
                "name": {
                    "title": "Mr" if gender == "male" else "Ms",
                    "first": first_name,
                    "last": last_name
                },
                "location": {
                    "street": {"number": random.randint(10, 9999), "name": f"{random.choice(last_names)} Ave"},
                    "city": city,
                    "state": city,
                    "country": country,
                    "postcode": str(random.randint(10000, 99999))
                },
                "email": f"{first_name.lower()}.{last_name.lower()}{random.randint(1,99)}@example.com",
                "login": {"uuid": f"usr-{i:04d}-{random.randint(1000,9999)}"},
                "dob": {
                    "date": f"{2026 - age}-06-15T00:00:00.000Z",
                    "age": age
                },
                "phone": f"0123-456-{random.randint(100,999)}",
                "cell": f"0987-654-{random.randint(100,999)}",
                "nat": country[:2].upper(),
                "picture": {
                    "large": f"https://randomuser.me/api/portraits/{'men' if gender == 'male' else 'women'}/{i%50}.jpg"
                }
            }
            results.append(user_obj)
        payload = {"results": results, "info": {"seed": "fallback", "results": len(results), "page": 1, "version": "1.4"}}

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    logger.info(f"User dataset ({len(payload['results'])} records) saved to {OUTPUT_FILE}")
    return OUTPUT_FILE

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="second_dag",
    default_args=default_args,
    description="RandomUser API Branching ETL Pipeline with PySpark & Spark JDBC",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["spark", "randomuser", "branching", "jdbc", "lecture14"],
) as dag:

    create_directories = BashOperator(
        task_id="create_directories",
        bash_command=f"mkdir -p {DATA_DIR}",
    )

    download_users = PythonOperator(
        task_id="download_random_users",
        python_callable=fetch_random_users,
    )

    process_users_spark = SparkSubmitOperator(
        task_id="process_random_users_spark",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/process_random_users.py",
        application_args=[
            "--input_path", OUTPUT_FILE,
            "--db_host", "postgres",
            "--db_port", "5432",
            "--db_name", "airflow",
            "--db_user", "airflow",
            "--db_password", "airflow",
        ],
        jars="/opt/airflow/jars/postgresql-42.6.0.jar",
        conf={
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
        },
        verbose=True,
    )

    create_directories >> download_users >> process_users_spark
