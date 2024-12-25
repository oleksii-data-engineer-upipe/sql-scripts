CREATE MATERIALIZED VIEW billing_analyst.mv_summary_new_paid
DISTKEY (site_id)
SORTKEY (date, netw, site_id, os, country, age_group)
AS
WITH purchase_data AS (
    SELECT
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) AS date,
        n.netw AS netw,
        m.name AS manager,
        up.site_id,
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
        up.gender,
        up.age,
        ph.external_id,
        ph.date_added,
        up.created_at,
        CASE 
            WHEN up.age < 35 THEN 'Under 35'
            WHEN up.age BETWEEN 35 AND 44 THEN '35-44'
            WHEN up.age BETWEEN 45 AND 90 THEN '45-90'
            WHEN up.age >= 91 THEN '91+'
            ELSE 'Unknown'
        END AS age_group
    FROM prod_shatal_db.prodmysqldatabase.v2_purchase_history ph
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_utm u ON ph.external_id = u.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_frod_list fl ON ph.external_id = fl.man_external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_networks n ON u.network_id = n.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_sources s ON n.parent_id = s.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_managers m ON s.parent_id = m.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.user_profile up ON ph.external_id = up.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_user_register_device d ON ph.external_id = d.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.country c ON up.country = c.id
    WHERE DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) BETWEEN '2023-01-01' 
          AND DATEADD(day, -1, CURRENT_DATE)
      AND ph.first_package = 1
      AND fl.man_external_id IS NULL
      AND up.id NOT IN (
          SELECT up.id 
          FROM prod_shatal_db.prodmysqldatabase.user_profile up
          WHERE up.name LIKE '%test%' 
            OR (up.email LIKE '%test%' AND up.email NOT LIKE '%delete%')  
            OR up.email LIKE '%+%' 
            OR up.tester = 1 
            OR up.country = 222  
            OR up.email LIKE '%upiple%' 
            OR up.email LIKE '%irens%' 
            OR up.email LIKE '%galaktica%' 
            OR up.email LIKE '%i.ua%'
      )
      AND ph.external_id NOT IN (
          SELECT cr.user_external_id
          FROM prod_shatal_db.prodmysqldatabase.v3_cross_marketing cr
          LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph ON cr.user_external_id = ph.external_id
          WHERE ph.first_package = 1
            AND cr.parent_external_id IS NOT NULL
      )
)

SELECT
    date,
    netw,
    manager,
    site_id,
    os,
    country,
    age_group,  
    
    COUNT(CASE WHEN gender = 0 THEN external_id ELSE NULL END) AS paid,
    COUNT(CASE WHEN (age >= 45 OR age IS NULL) AND gender = 0 THEN external_id ELSE NULL END) AS paid_45,
    COUNT(CASE WHEN EXTRACT(MONTH FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', date_added)) = EXTRACT(MONTH FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', created_at)) 
               AND EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', date_added)) = EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', created_at)) 
               AND gender = 0 THEN external_id ELSE NULL END) AS paid_reg,
    COUNT(CASE WHEN EXTRACT(MONTH FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', date_added)) = EXTRACT(MONTH FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', created_at)) 
               AND EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', date_added)) = EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', created_at)) 
               AND (age >= 45 OR age IS NULL) AND gender = 0 THEN external_id ELSE NULL END) AS paid_45_reg,

    COUNT(CASE WHEN gender = 1 THEN external_id ELSE NULL END) AS paid_women,
    COUNT(CASE WHEN (age >= 45 OR age IS NULL) AND gender = 1 THEN external_id ELSE NULL END) AS paid_45_women,
    COUNT(CASE WHEN EXTRACT(MONTH FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', date_added)) = EXTRACT(MONTH FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', created_at)) 
               AND EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', date_added)) = EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', created_at)) 
               AND gender = 1 THEN external_id ELSE NULL END) AS paid_reg_women,
    COUNT(CASE WHEN EXTRACT(MONTH FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', date_added)) = EXTRACT(MONTH FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', created_at)) 
               AND EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', date_added)) = EXTRACT(YEAR FROM CONVERT_TIMEZONE('UTC', 'Europe/Kiev', created_at)) 
               AND (age >= 45 OR age IS NULL) AND gender = 1 THEN external_id ELSE NULL END) AS paid_45_reg_women

FROM purchase_data
GROUP BY 
    date, netw, manager, site_id, os, country, age_group;  -- Додаємо групування за віковими категоріями