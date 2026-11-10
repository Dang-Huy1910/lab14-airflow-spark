"""
DAG 3: dnd_weekly_characters
Description: Interacts with D&D 5e API to fetch reference game metadata,
uses PySpark to generate randomized characters with balanced stats,
and loads records into PostgreSQL via Spark JDBC.
Schedule: Weekly (@weekly)
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

DATA_DIR = "/opt/airflow/data/dnd"
OUTPUT_FILE = f"{DATA_DIR}/dnd_raw.json"

def fetch_dnd_metadata(**context):
    """
    Communicates with the D&D 5e REST API to fetch races, classes, and alignments.
    Includes built-in resilience fallbacks if the external service experiences latency.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    base_url = "https://www.dnd5eapi.co/api"

    default_races = [
        "Dragonborn", "Dwarf", "Elf", "Gnome",
        "Half-Elf", "Half-Orc", "Halfling", "Human", "Tiefling"
    ]
    default_classes = [
        "Barbarian", "Bard", "Cleric", "Druid", "Fighter",
        "Monk", "Paladin", "Ranger", "Rogue", "Sorcerer", "Warlock", "Wizard"
    ]
    default_alignments = [
        "Lawful Good", "Neutral Good", "Chaotic Good",
        "Lawful Neutral", "True Neutral", "Chaotic Neutral",
        "Lawful Evil", "Neutral Evil", "Chaotic Evil"
    ]

    def get_names_from_endpoint(endpoint, fallback):
        try:
            req = urllib.request.Request(f"{base_url}/{endpoint}", headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=12) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                names = [item["name"] for item in data.get("results", [])]
                if names:
                    logger.info(f"Successfully fetched {len(names)} entries for {endpoint}")
                    return names
        except Exception as e:
            logger.warning(f"Could not reach {endpoint} API ({e}). Using standard 5e reference list.")
        return fallback

    races = get_names_from_endpoint("races", default_races)
    classes = get_names_from_endpoint("classes", default_classes)
    alignments = get_names_from_endpoint("alignments", default_alignments)

    payload = {
        "fetched_at": datetime.utcnow().isoformat(),
        "races": races,
        "classes": classes,
        "alignments": alignments
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    logger.info(f"Saved D&D 5e reference metadata to {OUTPUT_FILE}")
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
    dag_id="dnd_weekly_characters",
    default_args=default_args,
    description="Weekly D&D 5e Character Generation with Apache Airflow & PySpark",
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["spark", "dnd", "jdbc", "weekly", "lecture14"],
) as dag:

    create_directories = BashOperator(
        task_id="create_directories",
        bash_command=f"mkdir -p {DATA_DIR}",
    )

    fetch_metadata = PythonOperator(
        task_id="fetch_dnd_api_metadata",
        python_callable=fetch_dnd_metadata,
    )

    generate_characters_spark = SparkSubmitOperator(
        task_id="generate_characters_spark",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/process_dnd_characters.py",
        application_args=[
            "--input_path", OUTPUT_FILE,
            "--num_characters", "50",
            "--generation_week", "{{ execution_date.strftime('%Y-W%W') }}",
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

    create_directories >> fetch_metadata >> generate_characters_spark
