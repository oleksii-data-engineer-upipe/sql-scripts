create MATERIALIZED VIEW billing_analyst.mv_summary_500
DISTKEY (site_id)
SORTKEY (date, netw, site_id, os, country, age_group)
AS
WITH fraud_global_purchases AS (
    SELECT 
        bp.external_id,
        cr.parent_external_id,
        bp.date_1st_ph,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph_fraud.date_added)) AS date_1st_ph_fraud,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph_global.date_added)) AS date_1st_ph_global,
        CASE WHEN bp.external_id IN (
            SELECT cr.user_external_id
            FROM prod_shatal_db.prodmysqldatabase.v3_cross_marketing cr
            WHERE cr.parent_external_id IS NOT NULL
        ) THEN cr.global_parent_external_id ELSE NULL END AS global_external_id
    FROM (
	    SELECT ph.external_id, fl.parent_external_id, DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) AS date_1st_ph
	    FROM prod_shatal_db.prodmysqldatabase.v2_purchase_history ph
	    LEFT JOIN prod_shatal_db.prodmysqldatabase.user_profile up ON ph.external_id = up.external_id
	    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_frod_list fl ON ph.external_id = fl.man_external_id
	    WHERE ph.first_package = 1
	    AND DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) BETWEEN '2023-01-01' AND DATEADD(day, -1, CONVERT_TIMEZONE('UTC', 'Europe/Kiev', GETDATE()))
	    AND up.id NOT IN (
	        SELECT up.id 
	        FROM prod_shatal_db.prodmysqldatabase.user_profile up
	        WHERE up.name LIKE '%test%' 
	            OR (up.email LIKE '%test%' AND up.email NOT LIKE '%delete%') OR up.email LIKE '%+%' 
	            OR up.tester = 1 OR up.country = 222 OR up.email LIKE '%upiple%' OR up.email LIKE '%irens%'
	    )
    ) bp
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph_fraud ON bp.parent_external_id = ph_fraud.external_id AND ph_fraud.first_package = 1
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph_global ON bp.external_id = ph_global.external_id AND ph_global.first_package = 1
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_cross_marketing cr ON bp.external_id = cr.user_external_id
    WHERE (bp.parent_external_id IS NULL AND bp.external_id IS NULL) 
        OR (
            (DATE_PART('month', bp.date_1st_ph) = DATE_PART('month', ph_fraud.date_added) AND DATE_PART('year', bp.date_1st_ph) = DATE_PART('year', ph_fraud.date_added)) 
            OR (DATE_PART('month', bp.date_1st_ph) = DATE_PART('month', ph_global.date_added) AND DATE_PART('year', bp.date_1st_ph) = DATE_PART('year', ph_global.date_added))
        )
),
spend_accumulation AS (
    SELECT external_id, date_1st_ph, date_ph, spend, SUM(spend) OVER (PARTITION BY external_id ORDER BY date_ph ROWS UNBOUNDED PRECEDING) AS spend_nakop
    FROM ( 
	    SELECT 
	        COALESCE(fgp.parent_external_id, fgp.global_external_id, fgp.external_id) AS external_id,
	        COALESCE(fgp.date_1st_ph_fraud, fgp.date_1st_ph_global, fgp.date_1st_ph) AS date_1st_ph,
	        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) AS date_ph,
	        SUM(ph.price) AS spend
	    FROM fraud_global_purchases fgp
	    JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph ON fgp.external_id = ph.external_id 
	        AND DATE_PART('month', CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) = DATE_PART('month', fgp.date_1st_ph)
	        AND DATE_PART('year', CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) = DATE_PART('year', fgp.date_1st_ph)
	    GROUP BY 1, 2, 3
	    )
),
date_to_500 AS (
    SELECT external_id, date_1st_ph, MIN(CASE WHEN spend_nakop >= 500 THEN date_ph END) AS date_to_500
    FROM spend_accumulation
    GROUP BY 1, 2
)

SELECT 
    t11.date_to_500 AS date,
    t11.netw,
    t11.site_id,
    t11.os,
    t11.country,
    t11.age_group,
    COUNT(t11."500_day_to_day") AS "500_day_to_day",
    COUNT(t11."500_day_to_day_reg") AS "500_day_to_day_reg",
    COUNT(t11."500_nakop") AS "500_nakop",
    COUNT(t11."500_nakop_reg") AS "500_nakop_reg",
    COUNT(t11."500_delay_3_day") AS "500_delay_3_day",
    COUNT(t11."500_delay_3_day_reg") AS "500_delay_3_day_reg",
    COUNT(t11."500_day_to_day_women") AS "500_day_to_day_women",
    COUNT(t11."500_day_to_day_reg_women") AS "500_day_to_day_reg_women",
    COUNT(t11."500_nakop_women") AS "500_nakop_women",
    COUNT(t11."500_nakop_reg_women") AS "500_nakop_reg_women",
    COUNT(t11."500_delay_3_day_women") AS "500_delay_3_day_women",
    COUNT(t11."500_delay_3_day_reg_women") AS "500_delay_3_day_reg_women"
FROM (
    SELECT 
        t10.*,
        CASE WHEN t10.date_to_500 = t10.date_1st_ph AND t10.gender = 0 	THEN t10.external_id ELSE NULL
        END AS "500_day_to_day",
        CASE WHEN t10.date_to_500 = t10.date_reg AND t10.gender = 0 	THEN t10.external_id ELSE NULL
        END AS "500_day_to_day_reg",
        CASE WHEN t10.gender = 0 										THEN t10.external_id ELSE NULL
        END AS "500_nakop",
        CASE WHEN EXTRACT(MONTH FROM t10.date_to_500) = EXTRACT(MONTH FROM t10.date_reg) AND t10.gender = 0 THEN t10.external_id ELSE NULL
        END AS "500_nakop_reg",
        CASE WHEN t10.date_to_500 <= DATEADD(day, 3, t10.date_1st_ph) AND t10.gender = 0 	THEN t10.external_id ELSE NULL
        END AS "500_delay_3_day",
        CASE WHEN t10.date_to_500 <= DATEADD(day, 3, t10.date_reg) AND t10.gender = 0 		THEN t10.external_id ELSE NULL
        END AS "500_delay_3_day_reg",
        CASE WHEN t10.date_to_500 = t10.date_1st_ph AND t10.gender = 1 	THEN t10.external_id ELSE NULL
        END AS "500_day_to_day_women",
        CASE WHEN t10.date_to_500 = t10.date_reg AND t10.gender = 1 	THEN t10.external_id ELSE NULL
        END AS "500_day_to_day_reg_women",
        CASE WHEN t10.gender = 1 										THEN t10.external_id ELSE NULL
        END AS "500_nakop_women",
        CASE WHEN EXTRACT(MONTH FROM t10.date_to_500) = EXTRACT(MONTH FROM t10.date_reg) AND t10.gender = 1 THEN t10.external_id ELSE NULL
        END AS "500_nakop_reg_women",
        CASE WHEN t10.date_to_500 <= DATEADD(day, 3, t10.date_1st_ph) AND t10.gender = 1 	THEN t10.external_id ELSE NULL
        END AS "500_delay_3_day_women",
        CASE WHEN t10.date_to_500 <= DATEADD(day, 3, t10.date_reg) AND t10.gender = 1 		THEN t10.external_id ELSE NULL
        END AS "500_delay_3_day_reg_women"
    FROM (
		SELECT 
		    d2h.*,
		    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) AS date_reg,
		    n.netw,
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
	            WHEN up.age < 35 THEN 'Under 35'
	            WHEN up.age BETWEEN 35 AND 44 THEN '35-44'
	            WHEN up.age BETWEEN 45 AND 90 THEN '45-90'
	            WHEN up.age >= 91 THEN '91+'
	            ELSE 'Unknown'
	        END AS age_group,
		    CASE 
		        WHEN c.id IN (13, 38, 154, 224, 225) THEN c.country_name
		        ELSE 'other' 
		    END AS country
		FROM date_to_500 d2h
		LEFT JOIN prod_shatal_db.prodmysqldatabase.user_profile up ON d2h.external_id = up.external_id
		LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_utm u ON up.external_id = u.external_id
		LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_networks n ON u.network_id = n.id
		LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_user_register_device d ON d2h.external_id = d.external_id
		LEFT JOIN prod_shatal_db.prodmysqldatabase.country c ON up.country = c.id
		WHERE d2h.date_to_500 IS NOT null
    ) t10
) t11
GROUP BY t11.date_to_500, t11.netw, t11.site_id, t11.os, t11.country, t11.age_group
;

