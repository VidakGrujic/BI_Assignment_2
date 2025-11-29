-- Q15 - . Show Exceed Days (any) by Country in Eastern Europe for 2023 and 2024. Return Countries 
-- (only those in Eastern Europe) on rows and two columns—2023 and 2024 totals of Exceed Days (any).
SET search_path TO dwh2_008;
SELECT
    c.country_name,
    SUM(CASE WHEN t.year_num = 2023 THEN f.exceed_days_any ELSE 0 END) AS exceed_days_2023,
    SUM(CASE WHEN t.year_num = 2024 THEN f.exceed_days_any ELSE 0 END) AS exceed_days_2024
FROM ft_param_city_month AS f
JOIN dim_timemonth AS t
    ON f.month_key = t.month_key
JOIN dim_city AS c
    ON f.city_key = c.city_key
WHERE
    t.year_num IN (2023, 2024)
    AND c.region_name = 'Eastern Europe'
GROUP BY
    c.country_name
ORDER BY
    c.country_name;