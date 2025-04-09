USE website_traffic;
-- Create the combined table first if it doesn't exist

-- 1️⃣ Session Categories Comparison
CREATE TABLE IF NOT EXISTS session_categories_comparison AS
SELECT 
    s.window_start, 
    s.window_end, 
    s.session_category,
    s.count AS stream_count,
    b.count AS batch_count,
    s.percentage AS stream_percentage,
    b.percentage AS batch_percentage,
    'comparison.sql' AS source_file
FROM session_categories s
JOIN session_categories_batch b 
  ON s.window_start = b.window_start 
     AND s.window_end = b.window_end 
     AND s.session_category = b.session_type
WHERE s.count != b.count 
   OR s.percentage != b.percentage;


-- 2️⃣ Page Views Distribution Comparison
CREATE TABLE IF NOT EXISTS page_views_distribution_comparison AS
SELECT 
    s.window_start, 
    s.window_end, 
    s.page_views,
    s.count AS stream_count,
    b.count AS batch_count,
    s.percentage AS stream_percentage,
    b.percentage AS batch_percentage,
    'comparison.sql' AS source_file
FROM page_views_distribution s
JOIN page_views_distribution_batch b 
  ON s.window_start = b.window_start 
     AND s.window_end = b.window_end 
     AND s.page_views = b.page_views
WHERE s.count != b.count 
   OR s.percentage != b.percentage;


-- 3️⃣ Engagement Scores Comparison
CREATE TABLE IF NOT EXISTS engagement_scores_comparison AS
SELECT 
    s.window_start, 
    s.window_end,
    s.avg_engagement_score AS stream_avg,
    b.avg_engagement_score AS batch_avg,
    s.min_engagement_score AS stream_min,
    b.min_engagement_score AS batch_min,
    s.max_engagement_score AS stream_max,
    b.max_engagement_score AS batch_max,
    'comparison.sql' AS source_file
FROM engagement_scores s
JOIN engagement_score_batch b 
  ON s.window_start = b.window_start 
     AND s.window_end = b.window_end
WHERE 
    s.avg_engagement_score != b.avg_engagement_score 
 OR s.min_engagement_score != b.min_engagement_score 
 OR s.max_engagement_score != b.max_engagement_score;
 
CREATE TABLE accuracy_comparison AS
SELECT
    'session_categories_comparison' AS table_name,
    COUNT(*) AS total_records,
    SUM(CASE
        WHEN ABS(COALESCE(stream_count, 0) - COALESCE(batch_count, 0)) <= 10 THEN 1
        ELSE 0
    END) AS matching_records,
    ROUND(
        SUM(CASE
            WHEN ABS(COALESCE(stream_count, 0) - COALESCE(batch_count, 0)) <= 10 THEN 1
            ELSE 0
        END) / COUNT(*) * 100, 2
    ) AS accuracy_percentage
FROM session_categories_comparison;


select * from session_categories_comparison;
select * from page_views_distribution_comparison;
select * from engagement_scores_comparison;
select * from accuracy_comparison;