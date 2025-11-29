-- Q21 - Show Data Volume (KB) by Param Category for 2023 and 2024, limited to Purpose = Health 
-- Risk or Environmental Monitoring. Return Param Categories (only those under the two 
-- purposes) on rows and two columns—2023 and 2024 totals of Data Volume (KB).
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
    AND p.purpose IN ('Health Risk', 'Environmental Monitoring')
GROUP BY
    p.category
ORDER BY
    p.category;