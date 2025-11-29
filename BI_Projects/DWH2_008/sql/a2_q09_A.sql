-- Q9: For 2024, show Reading Events by Country × Quarter (Top 10 countries). Return the four 
-- quarters on columns (Q1–Q4) and the Top 10 countries on rows, ranked by total Reading Events in 2024
SET search_path TO dwh2_008;
WITH country_quarter AS (
    SELECT
        c.country_name,
        t.quarter_num,
        SUM(f.reading_events_count) AS reading_events
    FROM ft_param_city_month AS f
    JOIN dim_timemonth AS t
        ON f.month_key = t.month_key
    JOIN dim_city AS c
        ON f.city_key = c.city_key
    WHERE
        t.year_num = 2024
    GROUP BY
        c.country_name,
        t.quarter_num
),
country_totals AS (
    SELECT
        country_name,
        SUM(reading_events) AS total_2024_events
    FROM country_quarter
    GROUP BY country_name
    ORDER BY total_2024_events DESC
    LIMIT 10
)
SELECT
    ct.country_name,
    SUM(CASE WHEN cq.quarter_num = 1 THEN cq.reading_events ELSE 0 END) AS q1_2024,
    SUM(CASE WHEN cq.quarter_num = 2 THEN cq.reading_events ELSE 0 END) AS q2_2024,
    SUM(CASE WHEN cq.quarter_num = 3 THEN cq.reading_events ELSE 0 END) AS q3_2024,
    SUM(CASE WHEN cq.quarter_num = 4 THEN cq.reading_events ELSE 0 END) AS q4_2024,
    ct.total_2024_events
FROM country_totals AS ct
JOIN country_quarter AS cq
    ON ct.country_name = cq.country_name
GROUP BY
    ct.country_name,
    ct.total_2024_events
ORDER BY
    ct.total_2024_events DESC;