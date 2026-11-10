-- Initialization SQL script for Lab 14 Database
-- Creates tables for the 3 ETL pipelines

-- 1. Table for first_dag (Bitcoin Price Index)
CREATE TABLE IF NOT EXISTS bitcoin_rates (
    id SERIAL PRIMARY KEY,
    currency VARCHAR(10),
    description TEXT,
    rate_str VARCHAR(50),
    usd_rate NUMERIC(15, 4),
    updated_time VARCHAR(100),
    updated_time_iso VARCHAR(100),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tables for second_dag (RandomUser branched by gender)
CREATE TABLE IF NOT EXISTS male_users (
    user_id VARCHAR(100),
    gender VARCHAR(10),
    title VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    cell VARCHAR(50),
    dob VARCHAR(50),
    age INT,
    street_name VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postcode VARCHAR(50),
    nationality VARCHAR(20),
    picture_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS female_users (
    user_id VARCHAR(100),
    gender VARCHAR(10),
    title VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    cell VARCHAR(50),
    dob VARCHAR(50),
    age INT,
    street_name VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postcode VARCHAR(50),
    nationality VARCHAR(20),
    picture_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Table for dnd_weekly_characters
CREATE TABLE IF NOT EXISTS dnd_characters (
    id SERIAL PRIMARY KEY,
    character_name VARCHAR(100),
    race VARCHAR(50),
    character_class VARCHAR(50),
    alignment VARCHAR(50),
    strength INT,
    dexterity INT,
    constitution INT,
    intelligence INT,
    wisdom INT,
    charisma INT,
    level INT,
    hit_points INT,
    generation_week VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_bitcoin_processed_at ON bitcoin_rates(processed_at);
CREATE INDEX IF NOT EXISTS idx_male_country ON male_users(country);
CREATE INDEX IF NOT EXISTS idx_female_country ON female_users(country);
CREATE INDEX IF NOT EXISTS idx_dnd_week ON dnd_characters(generation_week);
