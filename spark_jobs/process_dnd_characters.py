"""
PySpark Script: D&D 5e Character Generation ETL (dnd_weekly_characters)
Reads D&D 5e API reference data, generates randomized characters with valid stats,
and writes them to PostgreSQL via Spark JDBC.
"""
import sys
import json
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, rand, floor, array, element_at,
    current_timestamp, when, greatest, count, avg, concat
)

def parse_args():
    parser = argparse.ArgumentParser(description="Generate D&D Characters with PySpark")
    parser.add_argument("--input_path", type=str, default="/opt/airflow/data/dnd/dnd_raw.json",
                        help="Path to input D&D reference JSON file")
    parser.add_argument("--num_characters", type=int, default=50,
                        help="Number of characters to generate")
    parser.add_argument("--generation_week", type=str,
                        default=datetime.utcnow().strftime("%Y-W%W"),
                        help="Generation week identifier (e.g. 2026-W45)")
    parser.add_argument("--db_host", type=str, default="postgres", help="PostgreSQL host")
    parser.add_argument("--db_port", type=str, default="5432", help="PostgreSQL port")
    parser.add_argument("--db_name", type=str, default="airflow", help="Database name")
    parser.add_argument("--db_user", type=str, default="airflow", help="Database user")
    parser.add_argument("--db_password", type=str, default="airflow", help="Database password")
    return parser.parse_args()

def roll_ability_stat(seed_col):
    """Generates standard 3d6 ability score (3 - 18)"""
    return floor(rand(seed_col) * 16 + 3).cast("int")

def main():
    args = parse_args()
    print("=" * 65)
    print("Starting D&D 5e Character Generation PySpark Job")
    print(f"Input Path         : {args.input_path}")
    print(f"Character Count    : {args.num_characters}")
    print(f"Generation Week    : {args.generation_week}")
    print(f"Target DB          : {args.db_host}:{args.db_port}/{args.db_name}")
    print("=" * 65)

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("DnD_Weekly_Character_Generation") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Load D&D reference metadata from API output
        print(f"\n[1/5] Loading D&D 5e Reference Metadata from: {args.input_path}")
        with open(args.input_path, "r", encoding="utf-8") as f:
            dnd_meta = json.load(f)

        races = dnd_meta.get("races", ["Human", "Elf", "Dwarf", "Halfling", "Dragonborn"])
        classes = dnd_meta.get("classes", ["Fighter", "Wizard", "Rogue", "Cleric", "Paladin"])
        alignments = dnd_meta.get("alignments", [
            "Lawful Good", "Neutral Good", "Chaotic Good",
            "Lawful Neutral", "True Neutral", "Chaotic Neutral",
            "Lawful Evil", "Neutral Evil", "Chaotic Evil"
        ])

        print(f"Loaded {len(races)} races, {len(classes)} classes, {len(alignments)} alignments.")

        # Fantasy Name syllables for procedural character name generation
        name_prefixes = ["Thor", "Ael", "Gor", "Val", "Bran", "Mor", "Dra", "Ze", "Syl", "Eld", "Kael", "Bryn"]
        name_suffixes = ["in", "ador", "gath", "wyn", "rak", "ric", "ion", "las", "mir", "thas", "or", "wen"]

        # Convert Python lists to PySpark column arrays
        races_lit = array([lit(r) for r in races])
        classes_lit = array([lit(c) for c in classes])
        alignments_lit = array([lit(a) for a in alignments])
        prefixes_lit = array([lit(p) for p in name_prefixes])
        suffixes_lit = array([lit(s) for s in name_suffixes])

        # 2. Generate Base DataFrame
        print(f"\n[2/5] Synthesizing {args.num_characters} characters with PySpark...")
        base_df = spark.range(0, args.num_characters)

        # 3. Apply randomized attribute mapping using PySpark expressions
        characters_df = base_df.select(
            # Random Character Name
            element_at(prefixes_lit, floor(rand(11) * len(name_prefixes) + 1).cast("int")).alias("pfx"),
            element_at(suffixes_lit, floor(rand(22) * len(name_suffixes) + 1).cast("int")).alias("sfx"),
            # Random Race, Class, Alignment
            element_at(races_lit, floor(rand(33) * len(races) + 1).cast("int")).alias("race"),
            element_at(classes_lit, floor(rand(44) * len(classes) + 1).cast("int")).alias("character_class"),
            element_at(alignments_lit, floor(rand(55) * len(alignments) + 1).cast("int")).alias("alignment"),
            # Ability Scores (STR, DEX, CON, INT, WIS, CHA: 3 - 18)
            roll_ability_stat(101).alias("strength"),
            roll_ability_stat(102).alias("dexterity"),
            roll_ability_stat(103).alias("constitution"),
            roll_ability_stat(104).alias("intelligence"),
            roll_ability_stat(105).alias("wisdom"),
            roll_ability_stat(106).alias("charisma"),
            # Character Level (1 - 10)
            floor(rand(201) * 10 + 1).cast("int").alias("level"),
            lit(args.generation_week).alias("generation_week"),
            current_timestamp().alias("created_at")
        ).select(
            # Concatenate name prefix and suffix
            concat(col("pfx"), col("sfx")).alias("character_name"),
            col("race"),
            col("character_class"),
            col("alignment"),
            col("strength"),
            col("dexterity"),
            col("constitution"),
            col("intelligence"),
            col("wisdom"),
            col("charisma"),
            col("level"),
            col("generation_week"),
            col("created_at")
        )

        # Calculate D&D 5e HP based on class hit die + constitution modifier
        # Base hit die: Barbarian=12, Fighter/Paladin/Ranger=10, Cleric/Druid/Monk/Rogue/Warlock/Bard=8, Wizard/Sorcerer=6
        hit_die_expr = when(col("character_class") == "Barbarian", 12) \
            .when(col("character_class").isin("Fighter", "Paladin", "Ranger"), 10) \
            .when(col("character_class").isin("Cleric", "Druid", "Monk", "Rogue", "Warlock", "Bard"), 8) \
            .otherwise(6)

        con_mod_expr = floor((col("constitution") - 10) / 2)
        hp_expr = greatest(lit(6), (hit_die_expr + con_mod_expr) + (col("level") - 1) * (hit_die_expr / 2 + 1 + con_mod_expr)).cast("int")

        final_characters_df = characters_df.withColumn("hit_points", hp_expr)

        # 4. Display sample characters & aggregations (Spark GroupBy Showcase)
        print("\n--- Sample Generated D&D 5e Characters ---")
        final_characters_df.select(
            "character_name", "race", "character_class", "level", "hit_points", "strength", "dexterity", "constitution"
        ).show(8, truncate=False)

        print("\n--- Class Distribution ---")
        final_characters_df.groupBy("character_class").agg(
            count("*").alias("count"),
            avg("hit_points").alias("avg_hp")
        ).orderBy(col("count").desc()).show()

        # 5. Write to PostgreSQL via Spark JDBC
        jdbc_url = f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"
        table_name = "dnd_characters"
        print(f"\n[4/5] Writing {args.num_characters} characters to table '{table_name}' via JDBC...")
        
        final_characters_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", args.db_user) \
            .option("password", args.db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"[5/5] Successfully persisted weekly characters for week {args.generation_week}!")
        print("=" * 65)

    except Exception as e:
        print(f"Error during D&D PySpark character processing: {e}", file=sys.stderr)
        raise e
    finally:
        spark.stop()
        print("SparkSession stopped cleanly.")

if __name__ == "__main__":
    main()
