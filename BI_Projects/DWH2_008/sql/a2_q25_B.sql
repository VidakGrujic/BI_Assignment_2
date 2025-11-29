-- Q25 - For 2023 and 2024, show Exceed Days (any) by Purpose, plus the change from 2023 to 2024. 
-- Return Purposes on rows and three columns - Exceed Days 2023, Exceed Days 2024, and Change 2024–2023.
SET search_path TO dwh2_008;
SELECT
    p.purpose,
    SUM(CASE WHEN t.year_num = 2023 THEN f.exceed_days_any ELSE 0 END) AS exceed_days_2023,
    SUM(CASE WHEN t.year_num = 2024 THEN f.exceed_days_any ELSE 0 END) AS exceed_days_2024,
    SUM(CASE WHEN t.year_num = 2024 THEN f.exceed_days_any ELSE 0 END)
    - SUM(CASE WHEN t.year_num = 2023 THEN f.exceed_days_any ELSE 0 END)
      AS change_2024_minus_2023
FROM ft_param_city_month AS f
JOIN dim_timemonth AS t
    ON f.month_key = t.month_key
JOIN dim_param AS p
    ON f.param_key = p.param_key
WHERE
    t.year_num IN (2023, 2024)
GROUP BY
    p.purpose
ORDER BY
    p.purpose;