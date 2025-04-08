use website_traffic;
show tables;

DELETE from website_sessions;
drop TABLE raw_traffic_data;

drop TABLE raw_traffic_data;
SELECT* from raw_traffic_data;

SELECT max(session_duration) from raw_traffic_data;

SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''));

SELECT 
    time_window,
    page_views,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY time_window), 2) AS percentage
FROM (
    SELECT 
        page_views,
        DATE_FORMAT(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp) / 300) * 300), '%Y-%m-%d %H:%i') AS time_window
    FROM raw_traffic_data
) AS grouped
GROUP BY time_window, page_views
ORDER BY time_window, page_views;


SELECT 
    DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i') AS time_window,
    session_type,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY FLOOR(UNIX_TIMESTAMP(timestamp)/300)), 2) AS percentage
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
GROUP BY time_window, session_type
ORDER BY time_window, session_type;

SELECT 
    DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i') AS time_window,
    ROUND(AVG(0.4 * page_views + 0.3 * session_duration + 0.3 * time_on_page), 2) AS avg_engagement_score,
    ROUND(MIN(0.4 * page_views + 0.3 * session_duration + 0.3 * time_on_page), 2) AS min_engagement_score,
    ROUND(MAX(0.4 * page_views + 0.3 * session_duration + 0.3 * time_on_page), 2) AS max_engagement_score
FROM raw_traffic_data
GROUP BY time_window
ORDER BY time_window;


