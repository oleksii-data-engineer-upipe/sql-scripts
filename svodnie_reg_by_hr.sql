CREATE SCHEMA IF NOT EXISTS svodnie_reg_by_hr;


-- !! in case of table needed: 
-- 1) add prefix to materialized view (mv_)
--    e.g. "CREATE MATERIALIZED VIEW svodnie_reg_by_hr.mv_gold__report ..."
-- 2) create table without prefix in name
--    and with appropriate data types and sizes.
--    e.g. "CREATE table svodnie_reg_by_hr.gold__report X int2, Y int4, W varchar(0-65536) ... INSERT INTO SELECT * FROM svodnie_reg_by_hr.mv_gold__report ..." 
-- !! schema.gold__report - general base data source for schema
-- schema.refresh() - ingest new data

CREATE MATERIALIZED VIEW svodnie_reg_by_hr.gold__report
DISTSTYLE KEY
DISTKEY(external_id)
SORTKEY(date_reg, time_reg, manager)
AS
SELECT 
    up.external_id,
    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) AS date_reg,
    CASE 
        WHEN EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) BETWEEN 0 AND 23
        THEN LPAD(CAST(EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) AS VARCHAR(2)), 2, '0') || ':00-' || 
             LPAD(CAST((EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) + 1) % 24 AS VARCHAR(2)), 2, '0') || ':00'
        ELSE NULL 
    END AS time_reg,
    up.age,
    CASE 
        WHEN up.age < 35 THEN 'до 35'
        WHEN up.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN up.age BETWEEN 45 AND 90 THEN '45-90'
        WHEN up.age > 90 THEN '91+'
        ELSE 'Невідомо'
    END AS age_category,
    CASE 
        WHEN (up.age >= 45 AND up.age <= 90) OR up.age IS NULL THEN 'целевые' 
        ELSE 'нецелевые' 
    END AS target_group,
    n.netw,
    m.name AS manager,
    n.site_id,
    CASE 
        WHEN d.os ILIKE '%Android%' THEN 'Android'
        WHEN d.os ILIKE '%iOS%' THEN 'iOS'
        WHEN d.os ILIKE '%Windows%' THEN 'Windows'
        WHEN d.os ILIKE '%Mac%' THEN 'MacOS' 
        ELSE 'other' 
    END AS os,
    CASE 
        WHEN c.id IN (13,38,154,224,225) THEN c.country_name
        ELSE 'other' 
    END AS country
FROM 
    prod_shatal_db.prodmysqldatabase.user_profile up 
LEFT JOIN prod_shatal_db.prodmysqldatabase.country 					c  ON up.country 			= 	c.id
LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history 		ph ON up.external_id 		= 	ph.external_id
LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_user_register_device 	d  ON up.external_id 		= 	d.external_id
LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_utm 					u  ON up.external_id 		= 	u.external_id
	LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_networks 				n  ON u.network_id 		= 	n.id
		LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_sources 				s  ON n.parent_id 	= 	s.id
			LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_managers 				m  ON s.parent_id = m.id

WHERE 
    up.name 	 NOT ILIKE '%test%'
    AND up.email NOT ILIKE '%test%'
    AND up.email NOT ILIKE '%upiple%'
    AND up.email NOT ILIKE '%galaktica%'
    AND up.tester = 	0
    AND up.country != 	222
    AND DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) BETWEEN '2023-01-01' AND CURRENT_DATE - INTERVAL '1 day'
GROUP BY 
    up.external_id, 
    date_reg, 
    time_reg, 
    up.age, 
    age_category,
    target_group,
    n.netw, 
    m.name, 
    n.site_id, 
    os, 
    country,
   c.id, c.country_name;
  


CREATE OR REPLACE PROCEDURE operator_profile_activity.refresh()
AS 
$$
BEGIN
	REFRESH MATERIALIZED VIEW svodnie_reg_by_hr.gold__report;
	COMMIT;	
	ANALYZE svodnie_reg_by_hr.gold__report;
	COMMIT;
END;
$$ 
LANGUAGE plpgsql;



call operator_profile_activity.refresh();


select max(date_reg)
from svodnie_reg_by_hr.gold__report
;

