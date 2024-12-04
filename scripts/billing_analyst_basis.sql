CREATE MATERIALIZED VIEW prod_analytic_db.billing_analyst.mv_basis
DISTKEY (site_id)
SORTKEY (date, netw, site_id, os, country, age_group)
AS
WITH unique_dates AS (
    -- Generate unique dates starting from 2023-01-01
    SELECT  
        DISTINCT DATEADD(day, -numbers.n, CURRENT_DATE) AS unique_dates
    FROM (
        SELECT singles + tens + hundreds AS n
        FROM (
            SELECT 0 AS singles UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
        ) singles
        CROSS JOIN (
            SELECT 0 AS tens UNION ALL SELECT 10 UNION ALL SELECT 20 UNION ALL SELECT 30 UNION ALL SELECT 40 UNION ALL SELECT 50 UNION ALL SELECT 60 UNION ALL SELECT 70 UNION ALL SELECT 80 UNION ALL SELECT 90
        ) tens
        CROSS JOIN (
            SELECT 0 AS hundreds UNION ALL SELECT 100 UNION ALL SELECT 200 UNION ALL SELECT 300 UNION ALL SELECT 400 UNION ALL SELECT 500 UNION ALL SELECT 600 UNION ALL SELECT 700 UNION ALL SELECT 800 UNION ALL SELECT 900
        ) hundreds
    ) numbers
    WHERE DATEADD(day, -numbers.n, CURRENT_DATE) >= '2023-01-01'
),
networks AS (
    -- Get networks and corresponding managers
    SELECT n.netw, m.name AS manager
    FROM redshift_analytics_db.prodmysqldatabase.v3_networks n
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_sources s ON n.parent_id = s.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_managers m ON s.parent_id = m.id
),
sites AS (
    SELECT s.id, s.domain, n.netw
    FROM redshift_analytics_db.prodmysqldatabase.v3_site s
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON s.id = up.site_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_utm u ON up.external_id = u.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_networks n ON u.network_id = n.id
    WHERE up.created_at >= '2023-01-01'
    GROUP BY s.id, s.domain, n.netw
),
operating_systems AS (
    -- Define a set of operating systems
    SELECT 'Android' AS os UNION ALL SELECT 'iOS' UNION ALL SELECT 'Windows' UNION ALL SELECT 'MacOS' UNION ALL SELECT 'other'
),
countries AS (
    SELECT 'Australia' AS country UNION ALL SELECT 'Canada' UNION ALL SELECT 'New Zealand' UNION ALL SELECT 'United Kingdom' UNION ALL SELECT 'United States' UNION ALL SELECT 'other'
),
age_groups AS (
    -- Define possible age categories for cross join
    SELECT 'до 35' AS age_group UNION ALL
    SELECT '35-44' UNION ALL
    SELECT '45-90' UNION ALL
    SELECT '91+' UNION ALL
    SELECT 'Unknown'
)

SELECT 
    DATE(t1.unique_dates) AS date,
    t2.netw,
    t2.manager,
    t3.id AS site_id,
    t3.domain,
    t4.os,
    t5.country,
    ag.age_group 
FROM unique_dates t1
-- Perform joins based on common keys (netw, site_id, etc.)
CROSS JOIN networks t2
LEFT JOIN sites t3 ON t2.netw = t3.netw
CROSS JOIN operating_systems t4
CROSS JOIN countries t5
CROSS JOIN age_groups ag
WHERE t3.id IS NOT NULL
GROUP BY date, t2.netw, t2.manager, t3.id, t3.domain, t4.os, t5.country, ag.age_group
;


-- select count(*) from prod_analytic_db.billing_analyst.mv_basis