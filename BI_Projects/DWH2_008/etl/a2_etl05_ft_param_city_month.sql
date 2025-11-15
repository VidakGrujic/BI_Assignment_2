SET search_path TO dwh2_008, stg2_008;

-- =======================================
-- Load ft_param_city_month
-- =======================================

TRUNCATE TABLE ft_param_city_month RESTART IDENTITY CASCADE;

WITH 
-- ----------------------------------------------------------
-- Base readings with city + country + param business keys
-- ----------------------------------------------------------
base AS (
    SELECT 
        r.id AS reading_id,
        r.sensordevid,
        sd.cityid,
        c.cityname,
        co.countryname,
        r.paramid,
        p.paramname,
        r.readat::date AS reading_date,
        r.recordedvalue,
        r.datavolumekb,
        r.dataquality
    FROM tb_readingevent r
    JOIN tb_sensordevice sd ON sd.id = r.sensordevid
    JOIN tb_city c         ON c.id = sd.cityid
    JOIN tb_country co     ON co.id = c.countryid
    JOIN tb_param p        ON p.id = r.paramid
),

-- ----------------------------------------------------------
-- Thresholds for each parameter
-- ----------------------------------------------------------
param_thresholds AS (
    SELECT
        pa.paramid,
        MAX(CASE WHEN a.alertname = 'Yellow'  THEN pa.threshold END) AS th_yellow,
        MAX(CASE WHEN a.alertname = 'Orange'  THEN pa.threshold END) AS th_orange,
        MAX(CASE WHEN a.alertname = 'Red'     THEN pa.threshold END) AS th_red,
        MAX(CASE WHEN a.alertname = 'Crimson' THEN pa.threshold END) AS th_crimson
    FROM tb_paramalert pa
    JOIN tb_alert a ON a.id = pa.alertid
    GROUP BY pa.paramid
),

-- ----------------------------------------------------------
-- Compute daily alert rank
-- ----------------------------------------------------------
daily_rank AS (
    SELECT
        b.cityid,
        b.paramid,
        b.reading_date,
        CASE
            WHEN b.recordedvalue >= pt.th_crimson THEN 4
            WHEN b.recordedvalue >= pt.th_red     THEN 3
            WHEN b.recordedvalue >= pt.th_orange  THEN 2
            WHEN b.recordedvalue >= pt.th_yellow  THEN 1
            ELSE 0
        END AS rank_value
    FROM base b
    JOIN param_thresholds pt ON pt.paramid = b.paramid
),

-- ----------------------------------------------------------
-- Daily peak (max rank per day)
-- ----------------------------------------------------------
daily_peak AS (
    SELECT
        cityid,
        paramid,
        reading_date,
        MAX(rank_value) AS daily_rank
    FROM daily_rank
    GROUP BY cityid, paramid, reading_date
),

-- ----------------------------------------------------------
-- Monthly exceedances & peak
-- ----------------------------------------------------------
monthly AS (
    SELECT
        cityid,
        paramid,
        (EXTRACT(YEAR FROM reading_date)::int * 100 +
         EXTRACT(MONTH FROM reading_date)::int) AS month_key,
        COUNT(*) FILTER (WHERE daily_rank >= 1) AS exceed_days_any,
        MAX(daily_rank) AS monthly_peak_rank
    FROM daily_peak
    GROUP BY cityid, paramid, month_key
),

-- ----------------------------------------------------------
-- Monthly aggregations for counts, averages, p95
-- ----------------------------------------------------------
reading_aggs AS (
    SELECT
        b.cityid,
        b.cityname,
        b.countryname,
        b.paramid,
        b.paramname,

        (EXTRACT(YEAR FROM b.reading_date)::int * 100 +
         EXTRACT(MONTH FROM b.reading_date)::int) AS month_key,

        COUNT(DISTINCT (b.sensordevid, b.reading_date)) AS reading_events_count,
        COUNT(DISTINCT b.sensordevid) AS devices_reporting_count,
        SUM(b.datavolumekb) AS data_volume_kb_sum,
        COUNT(DISTINCT b.reading_date) AS days_with_readings,

        AVG(b.recordedvalue) AS recordedvalue_avg,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY b.recordedvalue) AS recordedvalue_p95,
        AVG(b.dataquality) AS data_quality_avg
    FROM base b
    GROUP BY 
        b.cityid, b.cityname, b.countryname,
        b.paramid, b.paramname,
        month_key
),

-- ----------------------------------------------------------
-- Final dimension lookups
-- ----------------------------------------------------------
final_join AS (
    SELECT
        tm.month_key,
        ci.city_key,
        p.param_key,

        ra.reading_events_count,
        ra.devices_reporting_count,
        ra.data_volume_kb_sum,

        m.exceed_days_any,
        (tm.days_in_month - ra.days_with_readings) AS missing_days,

        ra.recordedvalue_avg,
        ra.recordedvalue_p95,
        ra.data_quality_avg,

        (1000 + m.monthly_peak_rank) AS alertpeak_key
    FROM reading_aggs ra
    JOIN monthly m
      ON m.cityid = ra.cityid
     AND m.paramid = ra.paramid
     AND m.month_key = ra.month_key

    JOIN dim_city ci
      ON ci.city_name = ra.cityname
     AND ci.country_name = ra.countryname

    JOIN dim_param p
      ON p.param_name = ra.paramname

    JOIN dim_timemonth tm
      ON tm.month_key = ra.month_key
)

-- ----------------------------------------------------------
-- Insert into FACT table
-- ----------------------------------------------------------
INSERT INTO ft_param_city_month (
    ft_pcm_key,
    month_key, city_key, param_key,
    reading_events_count, devices_reporting_count, data_volume_kb_sum,
    exceed_days_any, missing_days,
    recordedvalue_avg, recordedvalue_p95, data_quality_avg,
    alertpeak_key
)
SELECT
    ROW_NUMBER() OVER () AS ft_pcm_key,
    month_key, city_key, param_key,
    reading_events_count, devices_reporting_count, data_volume_kb_sum,
    exceed_days_any, missing_days,
    recordedvalue_avg, recordedvalue_p95, data_quality_avg,
    alertpeak_key
FROM final_join;
