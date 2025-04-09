use website_traffic;
show tables;

SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''));

CREATE TABLE IF NOT EXISTS page_views_distribution_batch AS
SELECT 
    window_start,
    window_end,
    page_views,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY window_start), 2) AS percentage
FROM (
    SELECT 
        page_views,
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp)/300)*300) AS window_start,
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp)/300)*300 + 300) AS window_end
    FROM raw_traffic_data
) AS grouped
GROUP BY window_start, window_end, page_views
ORDER BY window_start, page_views;


select * from page_views_distribution_batch;


CREATE TABLE IF NOT EXISTS session_categories_batch AS
SELECT 
    FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp)/300)*300) AS window_start,
    FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp)/300)*300 + 300) AS window_end,
    session_type,
    COUNT(*) AS count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (
            PARTITION BY FLOOR(UNIX_TIMESTAMP(timestamp)/300)
        ),
        2
    ) AS percentage
FROM (
    SELECT 
        timestamp,
        CASE
            WHEN session_duration < 5 THEN 'Short'
            WHEN session_duration BETWEEN 5 AND 15 THEN 'Medium'
            ELSE 'Long'
        END AS session_type
    FROM raw_traffic_data
) AS categorized
GROUP BY window_start, window_end, session_type
ORDER BY window_start, session_type;


SELECT * FROM session_categories_batch;


-- engagement score --
CREATE TABLE IF NOT EXISTS engagement_score_batch AS
SELECT 
    window_start,
    window_end,
    ROUND(AVG(0.4 * page_views + 0.3 * session_duration + 0.3 * time_on_page), 2) AS avg_engagement_score,
    ROUND(MIN(0.4 * page_views + 0.3 * session_duration + 0.3 * time_on_page), 2) AS min_engagement_score,
    ROUND(MAX(0.4 * page_views + 0.3 * session_duration + 0.3 * time_on_page), 2) AS max_engagement_score
FROM (
    SELECT 
        page_views,
        session_duration,
        time_on_page,
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp)/300)*300) AS window_start,
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp)/300)*300 + 300) AS window_end
    FROM raw_traffic_data
) AS sessions
GROUP BY window_start, window_end
ORDER BY window_start;

SELECT * FROM engagement_score_batch;


-- quarterly trend --
CREATE TABLE IF NOT EXISTS quarterly_trend_batch AS
SELECT 
    DATE_FORMAT(
        FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp) / 900) * 900),
        '%Y-%m-%d %H:%i:00'
    ) AS quarter_window,
    COUNT(*) AS total_sessions,
    SUM(page_views) AS total_page_views,
    ROUND(AVG(page_views), 2) AS avg_page_views_per_session
FROM raw_traffic_data
GROUP BY quarter_window
ORDER BY quarter_window;

SELECT * FROM quarterly_trend_batch;
-- bounce rate --
CREATE TABLE IF NOT EXISTS bounce_rate_batch AS
SELECT 
    ROUND(
        100.0 * SUM(CASE WHEN page_views = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS bounce_rate_percentage
FROM raw_traffic_data;

SELECT * FROM bounce_rate_batch;


-- conversion rate --
CREATE TABLE IF NOT EXISTS conversion_rate_batch AS
SELECT 
    ROUND(
        100.0 * SUM(CASE WHEN page_views >= 3 AND time_on_page > 5 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS conversion_rate_percentage
FROM raw_traffic_data;

select * from session_categories_batch;
select * from page_views_distribution_batch;
select * from engagement_score_batch;

select count(*) from session_categories_batch;
select count(*) from page_views_distribution_batch;
select count(*) from engagement_score_batch;