-- Q27 - For 2024, show Exceed Days (any) by Country × Quarter for Russia, Turkey, Austria, and 
-- Germany. Return the four countries on rows and the four quarters of 2024 (Q1–Q4) on columns.

SET search_path TO dwh2_008;
SELECT
    c.country_name,
    SUM(CASE WHEN t.quarter_num = 1 THEN f.exceed_days_any ELSE 0 END) AS q1_2024,
    SUM(CASE WHEN t.quarter_num = 2 THEN f.exceed_days_any ELSE 0 END) AS q2_2024,
    SUM(CASE WHEN t.quarter_num = 3 THEN f.exceed_days_any ELSE 0 END) AS q3_2024,
    SUM(CASE WHEN t.quarter_num = 4 THEN f.exceed_days_any ELSE 0 END) AS q4_2024
FROM ft_param_city_month AS f
JOIN dim_timemonth AS t
    ON f.month_key = t.month_key
JOIN dim_city AS c
    ON f.city_key = c.city_key
WHERE
    t.year_num = 2024
    AND c.country_name IN ('Russia', 'Turkey', 'Austria', 'Germany')
GROUP BY
    c.country_name
ORDER BY
    c.country_name;