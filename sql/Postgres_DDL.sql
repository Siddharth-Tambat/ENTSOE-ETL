-- DDL to create database for entsoe data
CREATE DATABASE entsoe;

-- DDL to create table for Germany BNC reserves data
CREATE TABLE entsoe.public.germany_bnc_reserves (
    document_mrid TEXT,
    process_type_code VARCHAR(10),
    process_type TEXT,
    created_datetime TIMESTAMPTZ,
    allocation_decision_datetime TIMESTAMPTZ,
    area_domain VARCHAR(32),
    series_mrid TEXT,
    business_type_code VARCHAR(10),
    business_type TEXT,
    market_agreement_type_code VARCHAR(10),
    market_agreement_type TEXT,
    market_product_type TEXT,
    psr_type_code VARCHAR(10),
    psr_type TEXT,
    flow_direction_code VARCHAR(10),
    flow_direction TEXT,
    currency VARCHAR(10),
    quantity_unit VARCHAR(10),
    curve_type_code VARCHAR(10),
    curve_type TEXT,
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    resolution VARCHAR(20),
    position INT,
    interval_start TIMESTAMPTZ NOT NULL,
    interval_end TIMESTAMPTZ,
    quantity NUMERIC,
    procurement_price NUMERIC,
    imbalance_category TEXT,
    ingestion_time TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (area_domain, series_mrid, interval_start, process_type_code)
);

-- Speeds up time-series range queries
CREATE INDEX idx_bnc_interval_time
    ON entsoe.public.germany_bnc_reserves (interval_start, interval_end);

-- Speeds up aggregations by reserve/process type
CREATE INDEX idx_bnc_reserve_type
    ON entsoe.public.germany_bnc_reserves (process_type_code, flow_direction_code);
