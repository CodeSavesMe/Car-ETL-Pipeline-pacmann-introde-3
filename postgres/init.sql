-- ERD design
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'scraping') THEN
       CREATE USER scraping WITH PASSWORD 'pass';
   END IF;
END
$$;

-- Connect to database created by POSTGRES_DB
\connect "scrape-olx";

-- Enable UUID functions
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create table
CREATE TABLE IF NOT EXISTS scrape_data (
    uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255),
    price NUMERIC(16,2),
    listing_url VARCHAR(255),
    location VARCHAR(255),
    installment NUMERIC(16,2),
    posted_time VARCHAR(255),
    year FLOAT,
    lower_km FLOAT,
    upper_km FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Give permissions to 'scraping' user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO scraping;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO scraping;
