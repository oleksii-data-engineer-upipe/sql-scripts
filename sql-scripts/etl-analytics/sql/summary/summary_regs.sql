CREATE MATERIALIZED VIEW prod_analytic_db.summary.mv_summary_new_paid_users
DISTSTYLE KEY
DISTKEY(date)
SORTKEY(
    date,         
    netw,         
    country       
)
AS
-- Фільтрує історію покупок, видаляє шахрайські та тестові акаунти
WITH base_purchases AS (
   SELECT 
       ph.external_id,
       CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added) as purchase_date
   FROM redshift_analytics_db.prodmysqldatabase.v2_purchase_history ph
   WHERE DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) BETWEEN '2023-01-01' AND CURRENT_DATE - 1
       AND ph.first_package = 1
),

-- Фільтрує небажані акаунти (шахрайські, тестові, тощо)
filtered_purchases AS (
   SELECT bp.*
   FROM base_purchases bp
   WHERE NOT EXISTS (
       SELECT 1 
       FROM redshift_analytics_db.prodmysqldatabase.v2_frod_list fl 
       WHERE bp.external_id = fl.man_external_id
   )
   AND NOT EXISTS (
       SELECT 1 
       FROM redshift_analytics_db.prodmysqldatabase.v3_cross_marketing cr
       WHERE cr.user_external_id = bp.external_id
       AND cr.parent_external_id IS NOT NULL
   )
   AND NOT EXISTS (
       SELECT 1 
       FROM redshift_analytics_db.prodmysqldatabase.user_profile up
       WHERE bp.external_id = up.external_id
       AND (
           up.name ILIKE '%test%' 
           OR up.email ILIKE '%test%' 
           OR up.email ILIKE '%+%'
           OR up.tester = 1 
           OR up.country = 222
           OR up.email ILIKE '%upiple%'
           OR up.email ILIKE '%irens%'
           OR up.email ILIKE '%galaktica%'
           OR up.email ILIKE '%i.ua%'
       )
   )
),

-- Витягує основну інформацію про користувача
user_info AS (
   SELECT 
       up.external_id,
       up.gender,
       up.age,
       up.site_id,
       up.country as country_id,
       CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at) as registration_date,
       d.os
   FROM redshift_analytics_db.prodmysqldatabase.user_profile up
   LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_user_register_device d 
       ON up.external_id = d.external_id
   WHERE EXISTS (
       SELECT 1 FROM filtered_purchases fp 
       WHERE fp.external_id = up.external_id
   )
),

-- Отримує інформацію про мережі та менеджерів
network_info AS (
   SELECT 
       u.external_id,
       n.netw,
       m.name as manager
   FROM redshift_analytics_db.prodmysqldatabase.v2_utm u
   JOIN redshift_analytics_db.prodmysqldatabase.v3_networks n 
       ON u.network_id = n.id
   JOIN redshift_analytics_db.prodmysqldatabase.v3_sources s 
       ON n.parent_id = s.id
   JOIN redshift_analytics_db.prodmysqldatabase.v3_managers m 
       ON s.parent_id = m.id
   WHERE EXISTS (
       SELECT 1 FROM filtered_purchases fp 
       WHERE fp.external_id = u.external_id
   )
)

SELECT 
   DATE(fp.purchase_date) as date,
   ni.netw,
   ni.manager,
   ui.site_id,
   CASE 
       WHEN ui.os ILIKE '%Android%' THEN 'Android'
       WHEN ui.os ILIKE '%iOS%' 		THEN 'iOS'
       WHEN ui.os ILIKE '%Windows%' THEN 'Windows'
       WHEN ui.os ILIKE '%Mac%' 		THEN 'MacOS'
       ELSE 'other'
   END AS os,
   CASE 
       WHEN ui.country_id IN (13,38,154,224,225) 
       THEN c.country_name
       ELSE 'other'
   END AS country,
   -- Метрики для чоловіків
   COUNT(CASE WHEN ui.gender = 0 THEN fp.external_id END) AS paid,
   COUNT(CASE WHEN ui.gender = 0 AND (ui.age >= 45 OR ui.age IS NULL) 
       THEN fp.external_id END) AS paid_45,
   COUNT(CASE WHEN ui.gender = 0 
       AND DATE_PART('month', fp.purchase_date) = DATE_PART('month', ui.registration_date)
       AND DATE_PART('year', fp.purchase_date) = DATE_PART('year', ui.registration_date)
       THEN fp.external_id END) AS paid_reg,
   COUNT(CASE WHEN ui.gender = 0 
       AND (ui.age >= 45 OR ui.age IS NULL)
       AND DATE_PART('month', fp.purchase_date) = DATE_PART('month', ui.registration_date)
       AND DATE_PART('year', fp.purchase_date) = DATE_PART('year', ui.registration_date)
       THEN fp.external_id END) AS paid_45_reg,
   -- Метрики для жінок
   COUNT(CASE WHEN ui.gender = 1 THEN fp.external_id END) AS paid_women,
   COUNT(CASE WHEN ui.gender = 1 AND (ui.age >= 45 OR ui.age IS NULL) 
       THEN fp.external_id END) AS paid_45_women,
   COUNT(CASE WHEN ui.gender = 1 
       AND DATE_PART('month', fp.purchase_date) = DATE_PART('month', ui.registration_date)
       AND DATE_PART('year', fp.purchase_date) = DATE_PART('year', ui.registration_date)
       THEN fp.external_id END) AS paid_reg_women,
   COUNT(CASE WHEN ui.gender = 1 
       AND (ui.age >= 45 OR ui.age IS NULL)
       AND DATE_PART('month', fp.purchase_date) = DATE_PART('month', ui.registration_date)
       AND DATE_PART('year', fp.purchase_date) = DATE_PART('year', ui.registration_date)
       THEN fp.external_id END) AS paid_45_reg_women
FROM filtered_purchases fp
JOIN user_info ui ON fp.external_id = ui.external_id
JOIN network_info ni ON fp.external_id = ni.external_id
LEFT JOIN redshift_analytics_db.prodmysqldatabase.country c 
   ON ui.country_id = c.id
GROUP BY 1, 2, 3, 4, 5, 6;