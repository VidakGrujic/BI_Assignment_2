--  For 2023, show Reading Events by Quarter for Vienna, Berlin, Moscow, and London (all 
-- parameters). Return the four cities on rows and the four quarters of 2023 (Q1–Q4) on columns.

SET search_path TO dwh2_008;
SELECT
    c.city_name,
    SUM(CASE WHEN t.quarter_num = 1 THEN f.reading_events_count ELSE 0 END) AS q1_2023,
    SUM(CASE WHEN t.quarter_num = 2 THEN f.reading_events_count ELSE 0 END) AS q2_2023,
    SUM(CASE WHEN t.quarter_num = 3 THEN f.reading_events_count ELSE 0 END) AS q3_2023,
    SUM(CASE WHEN t.quarter_num = 4 THEN f.reading_events_count ELSE 0 END) AS q4_2023
FROM ft_param_city_month AS f
JOIN dim_timemonth AS t
    ON f.month_key = t.month_key
JOIN dim_city AS c
    ON f.city_key = c.city_key
WHERE
    t.year_num = 2023
    AND c.city_name IN ('Vienna', 'Berlin', 'Moscow', 'London')
GROUP BY
    c.city_name
ORDER BY
    c.city_name;