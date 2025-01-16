CREATE MATERIALIZED VIEW prod_analytic_db.summary.mv_user_spend_metrics
DISTSTYLE KEY DISTKEY(date) 
SORTKEY(date, netw, country)
AS
WITH valid_users AS (
   SELECT 
       NVL(fl.parent_external_id, 
       NVL(cr.global_parent_external_id, ph.external_id)) AS external_id,
       DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', MIN(ph.date_added))) AS first_purchase_date,
       up.gender,
       up.site_id,
       DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) AS reg_date,
       CASE 
           WHEN d.os LIKE '%Android%' THEN 'Android'
           WHEN d.os LIKE '%iOS%' THEN 'iOS'
           WHEN d.os LIKE '%Windows%' THEN 'Windows'
           WHEN d.os LIKE '%Mac%' THEN 'MacOS'
           ELSE 'other' 
       END AS os,
       CASE 
           WHEN c.id IN (13,38,154,224,225) THEN c.country_name 
           ELSE 'other' 
       END AS country,
       n.netw 
   FROM 	 	  redshift_analytics_db.prodmysqldatabase.user_profile up 
   INNER JOIN redshift_analytics_db.prodmysqldatabase.v2_purchase_history ph ON ph.external_id = up.external_id
   LEFT JOIN  redshift_analytics_db.prodmysqldatabase.v2_frod_list fl 		ON ph.external_id = fl.man_external_id
   LEFT JOIN  redshift_analytics_db.prodmysqldatabase.v3_cross_marketing cr 	ON ph.external_id = cr.user_external_id
   LEFT JOIN  redshift_analytics_db.prodmysqldatabase.v2_utm u 				ON up.external_id = u.external_id
   LEFT JOIN  redshift_analytics_db.prodmysqldatabase.v3_networks n 			ON u.network_id = n.id
   LEFT JOIN  redshift_analytics_db.prodmysqldatabase.v3_user_register_device d ON ph.external_id = d.external_id
   LEFT JOIN  redshift_analytics_db.prodmysqldatabase.country c 				ON up.country = c.id
   WHERE DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) >= '2023-01-01'
   AND DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) <= CURRENT_DATE - 1
   AND up.id NOT IN (
       SELECT id 
       FROM redshift_analytics_db.prodmysqldatabase.user_profile 
       WHERE name LIKE '%test%' 
       OR (email LIKE '%test%' AND email NOT LIKE '%delete%')
       OR email LIKE '%+%' 
       OR tester = 1 
       OR country = 222
       OR email LIKE '%upiple%' 
       OR email LIKE '%irens%'
   )
   AND ph.first_package = 1
   GROUP BY 
       NVL(fl.parent_external_id, NVL(cr.global_parent_external_id, ph.external_id)),
       up.gender, up.site_id, up.created_at,
       d.os, c.id, c.country_name, n.netw
),

spend_data AS (
   SELECT 
       vu.*,
       DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) as spend_date,
       ph.price as spend,
       SUM(ph.price) OVER (
           PARTITION BY vu.external_id 
           ORDER BY ph.date_added
           ROWS UNBOUNDED PRECEDING
       ) AS cumulative_spend
   FROM valid_users vu
   JOIN redshift_analytics_db.prodmysqldatabase.v2_purchase_history ph ON vu.external_id = ph.external_id
),

threshold_dates AS (
   SELECT 
       external_id, gender,
       first_purchase_date, reg_date,
       netw, site_id, os, country,
       MIN(spend_date) AS date_to_500
   FROM spend_data
   WHERE cumulative_spend >= 500
   GROUP BY 
       external_id, gender,
       first_purchase_date, reg_date,
       netw, site_id, os, country
)

SELECT 
   date_to_500 AS date,
   netw,
   site_id,
   os,
   country,
   -- Стандартні метрики для чоловіків
   COUNT(CASE WHEN gender = 0 AND date_to_500 = first_purchase_date 		THEN external_id END) AS "500_day_to_day",
   COUNT(CASE WHEN gender = 0 AND date_to_500 = reg_date 				THEN external_id END) AS "500_day_to_day_reg",
   COUNT(CASE WHEN gender = 0  											THEN external_id END) AS "500_nakop",
   COUNT(CASE WHEN gender = 0 AND DATE_PART(month, date_to_500) = DATE_PART(month, reg_date) THEN external_id END) AS "500_nakop_reg",
   COUNT(CASE WHEN gender = 0 AND date_to_500 <= first_purchase_date + 3 THEN external_id END) AS "500_delay_3_day",
   COUNT(CASE WHEN gender = 0 AND date_to_500 <= reg_date + 3 			THEN external_id END) AS "500_delay_3_day_reg",

   -- Ті ж метрики для жінок
   COUNT(CASE WHEN gender = 1 AND date_to_500 = first_purchase_date 		THEN external_id END) AS "500_day_to_day_women",
   COUNT(CASE WHEN gender = 1 AND date_to_500 = reg_date 				THEN external_id END) AS "500_day_to_day_reg_women",
   COUNT(CASE WHEN gender = 1 											THEN external_id END) AS "500_nakop_women",
   COUNT(CASE WHEN gender = 1 AND DATE_PART(month, date_to_500) = DATE_PART(month, reg_date) THEN external_id END) AS "500_nakop_reg_women",
   COUNT(CASE WHEN gender = 1 AND date_to_500 <= first_purchase_date + 3 THEN external_id END) AS "500_delay_3_day_women",
   COUNT(CASE WHEN gender = 1 AND date_to_500 <= reg_date + 3 			THEN external_id END) AS "500_delay_3_day_reg_women"
FROM threshold_dates
GROUP BY 
   date, netw, site_id, os, country
  ;
  
 