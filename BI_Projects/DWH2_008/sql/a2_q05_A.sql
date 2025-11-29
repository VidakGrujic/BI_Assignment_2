-- Q05 - For 2023 and 2024, show total Data Volume (KB) by Param Category × Year. Return Param 
-- Categories on rows and the two years (2023, 2024) on columns. 


SET search_path TO dwh2_008;

SELECT
    p.category,
    SUM(CASE WHEN t.year_num = 2023 THEN f.data_volume_kb_sum ELSE 0 END) AS volume_2023,
    SUM(CASE WHEN t.year_num = 2024 THEN f.data_volume_kb_sum ELSE 0 END) AS volume_2024
FROM ft_param_city_month AS f
JOIN dim_timemonth AS t
    ON f.month_key = t.month_key
JOIN dim_param AS p
    ON f.param_key = p.param_key
WHERE
    t.year_num IN (2023, 2024)
GROUP BY
    p.category
ORDER BY
    p.category;
