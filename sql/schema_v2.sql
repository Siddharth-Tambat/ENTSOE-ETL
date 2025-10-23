-- DDL to create database for entsoe data
CREATE DATABASE entsoe;

-- DDL to create table for Germany Energy Prices data v2
DROP TABLE IF EXISTS public.energy_prices_v2;

CREATE TABLE public.energy_prices_v2 (
    curve_type_code TEXT,
    area_domain TEXT,
    currency TEXT,
    price_unit TEXT,
    resolution TEXT,
    "position" INT,
    price NUMERIC,
    interval_start TIMESTAMPTZ,
    interval_end TIMESTAMPTZ,
    ingestion_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (area_domain, resolution, interval_start)
);
