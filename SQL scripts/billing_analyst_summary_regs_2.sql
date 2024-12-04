CREATE SCHEMA IF NOT EXISTS billing_analyst;

CREATE MATERIALIZED VIEW billing_analyst.mv_summary_regs_2
DISTKEY (site_id)
SORTKEY (date, netw, site_id, os, country, age_group)
AS
WITH user_data AS (
    SELECT DISTINCT
        up.last_ip AS IP,
        up.external_id,
        up.age
    FROM prod_shatal_db.prodmysqldatabase.user_profile up
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_last_activity la ON up.external_id = la.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_frod_list fl ON up.external_id = fl.man_external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_utm u ON u.external_id = up.external_id
    WHERE CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)::DATE = DATEADD(day, -1, CURRENT_DATE)
      AND up.name NOT LIKE '%test%'
      AND up.email NOT LIKE '%+%'
      AND fl.man_external_id IS NULL
      AND u.tail NOT LIKE '%external_id%'
    GROUP BY up.last_ip, up.external_id, up.age
),
user_profile_data AS (
    SELECT 
        CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)::DATE AS date,
        n.netw,
        m.name AS manager,
        up.site_id,
        up.gender,
        CASE 
            WHEN d.os LIKE '%Android%' THEN 'Android'
            WHEN d.os LIKE '%iOS%' THEN 'iOS'
            WHEN d.os LIKE '%Windows%' THEN 'Windows'
            WHEN d.os LIKE '%Mac%' THEN 'MacOS'
            ELSE 'other'
        END AS os,
        CASE 
            WHEN c.id IN (13, 38, 154, 224, 225) THEN c.country_name
            ELSE 'other'
        END AS country,
        CASE 
            WHEN la.country IN ('United States', 'Canada', 'United Kingdom', 'Australia', 'New Zealand', 'Denmark', 'Sweden', 'Norway') THEN la.country
            ELSE 'other'
        END AS country_la,
        CASE
            WHEN up.age < 35 THEN 'до 35'
            WHEN up.age BETWEEN 35 AND 44 THEN '35-44'
            WHEN up.age BETWEEN 45 AND 90 THEN '45-90'
            WHEN up.age > 90 THEN '91+'
            ELSE 'Невідомо'
        END AS age_group,
        t1.external_id,
        t1.age
    FROM user_data t1
    LEFT JOIN prod_shatal_db.prodmysqldatabase.user_profile up ON t1.external_id = up.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_utm u ON up.external_id = u.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_networks n ON u.network_id = n.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_sources s ON n.parent_id = s.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_managers m ON s.parent_id = m.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_user_register_device d ON up.external_id = d.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.country c ON up.country = c.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_last_activity la ON up.external_id = la.external_id
)

SELECT 
    q1.date,
    q1.netw,
    q1.manager,
    q1.site_id,
    q1.os,
    q1.country,
    q1.age_group,
    COUNT(CASE 
             WHEN q1.gender = 0 
                  AND ((q1.age >= 45 AND q1.age < 90) OR q1.age IS NULL)
                  AND q1.country_la IN ('United States', 'Canada', 'United Kingdom', 'Australia', 'New Zealand', 'Denmark', 'Sweden', 'Norway')
             THEN q1.external_id 
             ELSE NULL 
          END) AS regs_45,

    COUNT(CASE 
             WHEN q1.gender = 1 
                  AND ((q1.age >= 45 AND q1.age < 90) OR q1.age IS NULL)
                  AND q1.country_la IN ('United States', 'Canada', 'United Kingdom', 'Australia', 'New Zealand', 'Denmark', 'Sweden', 'Norway')
             THEN q1.external_id 
             ELSE NULL 
          END) AS regs_45_women,

    COUNT(CASE 
             WHEN q1.gender = 0 THEN q1.external_id 
             ELSE NULL 
          END) AS regs,

    COUNT(CASE 
             WHEN q1.gender = 1 THEN q1.external_id 
             ELSE NULL 
          END) AS regs_women

FROM user_profile_data q1
GROUP BY q1.date,
         q1.netw,
         q1.manager,
         q1.site_id,
         q1.os,
         q1.country,
         q1.age_group;