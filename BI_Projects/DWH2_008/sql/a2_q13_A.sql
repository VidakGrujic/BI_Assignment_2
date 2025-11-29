-- For Q1 of 2023, show Exceed Days (any) by City × Month where the monthly peak Alert Level 
-- is Yellow or None. Return Cities on rows and the first three months of 2023 (Jan–Mar) on 
-- columns, limited to months labelled Yellow or None.
SET search_path TO dwh2_008;
SELECT
    c.city_name,
    SUM(CASE WHEN t.month_num = 1 THEN f.exceed_days_any ELSE 0 END) AS jan_2023,
    SUM(CASE WHEN t.month_num = 2 THEN f.exceed_days_any ELSE 0 END) AS feb_2023,
    SUM(CASE WHEN t.month_num = 3 THEN f.exceed_days_any ELSE 0 END) AS mar_2023
FROM ft_param_city_month AS f
JOIN dim_timemonth AS t
    ON f.month_key = t.month_key
JOIN dim_city AS c
    ON f.city_key = c.city_key
WHERE
    t.year_num = 2023
    AND t.quarter_num = 1               
    AND f.alertpeak_key IN (1000, 1001)
GROUP BY
    c.city_name
HAVING
    SUM(f.exceed_days_any) > 0
ORDER BY
    c.city_name;