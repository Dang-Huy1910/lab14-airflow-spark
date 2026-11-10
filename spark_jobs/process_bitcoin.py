"""
PySpark Script: Bitcoin Price Index ETL (first_dag)
Parses Bitcoin JSON data from CoinDesk API, extracts USD rate, and loads to PostgreSQL.
"""
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

def parse_args():
    parser = argparse.ArgumentParser(description="Process Bitcoin JSON Data with PySpark")
    parser.add_argument("--input_path", type=str, default="/opt/airflow/data/bitcoin/bitcoin_latest.json",
                        help="Path to input raw Bitcoin JSON file")
    parser.add_argument("--db_host", type=str, default="postgres", help="PostgreSQL host")
    parser.add_argument("--db_port", type=str, default="5432", help="PostgreSQL port")
    parser.add_argument("--db_name", type=str, default="airflow", help="Database name")
    parser.add_argument("--db_user", type=str, default="airflow", help="Database user")
    parser.add_argument("--db_password", type=str, default="airflow", help="Database password")
    return parser.parse_args()

def main():
    args = parse_args()
    print("=" * 60)
    print("Starting Bitcoin ETL PySpark Job")
    print(f"Input Path : {args.input_path}")
    print(f"Target DB  : {args.db_host}:{args.db_port}/{args.db_name}")
    print("=" * 60)

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Bitcoin_USD_Rate_Extraction") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Read JSON file
        print(f"\n[1/4] Reading Bitcoin JSON from: {args.input_path}")
        df_raw = spark.read.option("multiline", "true").json(args.input_path)
        
        print("\n--- Raw JSON Schema ---")
        df_raw.printSchema()

        # 2. Extract USD Rate and Metadata
        print("\n[2/4] Transforming and Extracting USD Exchange Rate...")
        df_extracted = df_raw.select(
            col("bpi.USD.code").alias("currency"),
            col("bpi.USD.description").alias("description"),
            col("bpi.USD.rate").alias("rate_str"),
            col("bpi.USD.rate_float").cast("decimal(15,4)").alias("usd_rate"),
            col("time.updated").alias("updated_time"),
            col("time.updatedISO").alias("updated_time_iso")
        ).withColumn("processed_at", current_timestamp())

        # Show extracted result in console
        print("\n--- Extracted Bitcoin Data ---")
        df_extracted.show(truncate=False)

        # Retrieve scalar USD rate for clear logging
        rows = df_extracted.collect()
        if rows:
            rate_val = rows[0]["usd_rate"]
            time_val = rows[0]["updated_time"]
            print("=" * 60)
            print(f">>> LATEST BITCOIN PRICE: ${rate_val:,.2f} USD (Updated at: {time_val}) <<<")
            print("=" * 60)

        # 3. Write to PostgreSQL via Spark JDBC
        jdbc_url = f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"
        table_name = "bitcoin_rates"
        print(f"\n[3/4] Writing extracted data to PostgreSQL table '{table_name}' via JDBC...")
        
        df_extracted.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", args.db_user) \
            .option("password", args.db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print("[4/4] Successfully persisted Bitcoin rate into PostgreSQL.")

    except Exception as e:
        print(f"Error during Bitcoin PySpark processing: {e}", file=sys.stderr)
        raise e
    finally:
        spark.stop()
        print("SparkSession stopped cleanly.")

if __name__ == "__main__":
    main()
