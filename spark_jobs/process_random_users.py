"""
PySpark Script: RandomUser API ETL with Branching (second_dag)
Parses user JSON data, branches records by gender (male/female),
and writes each subset to PostgreSQL tables via Spark JDBC.
"""
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws, current_timestamp, avg, count

def parse_args():
    parser = argparse.ArgumentParser(description="Process RandomUser JSON Data with PySpark Branching")
    parser.add_argument("--input_path", type=str, default="/opt/airflow/data/random_user/users_latest.json",
                        help="Path to input raw RandomUser JSON file")
    parser.add_argument("--db_host", type=str, default="postgres", help="PostgreSQL host")
    parser.add_argument("--db_port", type=str, default="5432", help="PostgreSQL port")
    parser.add_argument("--db_name", type=str, default="airflow", help="Database name")
    parser.add_argument("--db_user", type=str, default="airflow", help="Database user")
    parser.add_argument("--db_password", type=str, default="airflow", help="Database password")
    return parser.parse_args()

def main():
    args = parse_args()
    print("=" * 65)
    print("Starting RandomUser PySpark Branching ETL Job")
    print(f"Input Path : {args.input_path}")
    print(f"Target DB  : {args.db_host}:{args.db_port}/{args.db_name}")
    print("=" * 65)

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("RandomUser_Gender_Branching_ETL") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Read JSON file
        print(f"\n[1/5] Reading RandomUser JSON data from: {args.input_path}")
        df_raw = spark.read.option("multiline", "true").json(args.input_path)
        
        # 2. Explode and Flatten results array
        print("\n[2/5] Exploding results array and flattening user attributes...")
        df_exploded = df_raw.select(explode(col("results")).alias("user"))

        df_users = df_exploded.select(
            col("user.login.uuid").alias("user_id"),
            col("user.gender").alias("gender"),
            col("user.name.title").alias("title"),
            col("user.name.first").alias("first_name"),
            col("user.name.last").alias("last_name"),
            concat_ws(" ", col("user.name.first"), col("user.name.last")).alias("full_name"),
            col("user.email").alias("email"),
            col("user.phone").alias("phone"),
            col("user.cell").alias("cell"),
            col("user.dob.date").alias("dob"),
            col("user.dob.age").cast("int").alias("age"),
            col("user.location.street.name").alias("street_name"),
            col("user.location.city").alias("city"),
            col("user.location.state").alias("state"),
            col("user.location.country").alias("country"),
            col("user.location.postcode").cast("string").alias("postcode"),
            col("user.nat").alias("nationality"),
            col("user.picture.large").alias("picture_url")
        ).withColumn("created_at", current_timestamp())

        total_count = df_users.count()
        print(f"Successfully processed {total_count} total user profiles.")

        # 3. Branching: Filter by gender (male / female)
        print("\n[3/5] Branching user dataset into male and female cohorts...")
        df_male = df_users.filter(col("gender") == "male")
        df_female = df_users.filter(col("gender") == "female")

        male_count = df_male.count()
        female_count = df_female.count()
        print(f"-> Male Users Count   : {male_count}")
        print(f"-> Female Users Count : {female_count}")

        # Quick Spark aggregations (Lecture 14 skill showcase)
        print("\n--- Demographics Summary (Average Age by Gender) ---")
        df_users.groupBy("gender").agg(
            count("*").alias("count"),
            avg("age").alias("average_age")
        ).show()

        print("\n--- Sample Male Users ---")
        df_male.select("user_id", "full_name", "gender", "email", "country", "age").show(5, truncate=False)

        print("\n--- Sample Female Users ---")
        df_female.select("user_id", "full_name", "gender", "email", "country", "age").show(5, truncate=False)

        # 4. Write branched data to PostgreSQL via Spark JDBC
        jdbc_url = f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"
        
        print(f"\n[4/5] Writing {male_count} male records to table 'male_users' via JDBC...")
        df_male.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "male_users") \
            .option("user", args.db_user) \
            .option("password", args.db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"[5/5] Writing {female_count} female records to table 'female_users' via JDBC...")
        df_female.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "female_users") \
            .option("user", args.db_user) \
            .option("password", args.db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print("\n" + "=" * 65)
        print("RandomUser PySpark Branching ETL Completed Successfully!")
        print("=" * 65)

    except Exception as e:
        print(f"Error during RandomUser PySpark processing: {e}", file=sys.stderr)
        raise e
    finally:
        spark.stop()
        print("SparkSession stopped cleanly.")

if __name__ == "__main__":
    main()
