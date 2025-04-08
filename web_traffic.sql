-- Create database
CREATE DATABASE IF NOT EXISTS website_traffic;

-- Use the database
USE website_traffic;

-- Create table for raw data
CREATE TABLE IF NOT EXISTS raw_traffic_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    page_views VARCHAR(255),
    session_duration FLOAT,
    time_on_page FLOAT,
    timestamp DATETIME,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
drop table streaming_metrics;
-- Create table for processed streaming data
CREATE TABLE streaming_metrics (
    record_id INT PRIMARY KEY,
    timestamp DATETIME,
    page_views INT,
    session_duration FLOAT,
    time_on_page FLOAT,
    processing_time FLOAT
);
select * from page_views_distribution;
-- Create table for page views distribution
CREATE TABLE IF NOT EXISTS page_views_distribution (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME,
    window_end DATETIME,
    page_views INT,
    count INT,
    percentage FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table for session categories
CREATE TABLE IF NOT EXISTS session_categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME,
    window_end DATETIME,
    session_category VARCHAR(20),
    count INT,
    percentage FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table for engagement scores
CREATE TABLE IF NOT EXISTS engagement_scores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME,
    window_end DATETIME,
    avg_engagement_score FLOAT,
    min_engagement_score FLOAT,
    max_engagement_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes for better query performance
CREATE INDEX idx_raw_timestamp ON raw_traffic_data(timestamp);
CREATE INDEX idx_streaming_window ON streaming_metrics(window_start, window_end);
CREATE INDEX idx_batch_window ON batch_metrics(window_start, window_end);

SELECT * FROM streaming_metrics ;
select * from raw_traffic_data;
SELECT COUNT(*) FROM streaming_metrics;

