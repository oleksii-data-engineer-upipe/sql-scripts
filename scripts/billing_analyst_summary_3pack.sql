CREATE MATERIALIZED VIEW billing_analyst.mv_3pack_base_data
DISTKEY (site_id)
SORTKEY (date_1st_ph)
AS
SELECT 
    ph.external_id,
    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) AS date_1st_ph,
    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) AS date_reg,
    n.netw,
    up.site_id,
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
    CASE 
        WHEN fl.man_external_id IS NOT NULL THEN fl.parent_external_id
        ELSE NULL 
    END AS fraud,
    up.gender,
    CASE
        WHEN up.age < 35 THEN 'Under 35'
        WHEN up.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN up.age BETWEEN 45 AND 90 THEN '45-90'
        WHEN up.age > 90 THEN '91+'
        ELSE 'Unknown'
    END AS age_group
FROM prod_shatal_db.prodmysqldatabase.v2_purchase_history ph
LEFT JOIN prod_shatal_db.prodmysqldatabase.user_profile up ON ph.external_id = up.external_id
LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_utm u ON ph.external_id = u.external_id
LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_networks n ON u.network_id = n.id
LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_frod_list fl ON ph.external_id = fl.man_external_id
LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_user_register_device d ON ph.external_id = d.external_id
LEFT JOIN prod_shatal_db.prodmysqldatabase.country c ON up.country = c.id
WHERE ph.first_package = 1
AND up.id NOT IN (
    SELECT up_inner.id 
    FROM prod_shatal_db.prodmysqldatabase.user_profile up_inner
    WHERE up_inner.name LIKE '%test%' 
    OR (up_inner.email LIKE '%test%' AND up_inner.email NOT LIKE '%delete%')
    OR up_inner.email LIKE '%+%' 
    OR up_inner.tester = 1 
    OR up_inner.country = 222  
    OR up_inner.email LIKE '%upiple%' 
    OR up_inner.email LIKE '%irens%'
)
AND DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) BETWEEN '2023-01-01' AND DATEADD(day, -1, CURRENT_DATE);


CREATE MATERIALIZED VIEW billing_analyst.mv_3pack_enriched_data
DISTKEY (external_id)
SORTKEY (date_1st_ph)
AS
SELECT 
    t.*,
    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph_fraud.date_added)) AS date_1st_ph_fraud,
    CASE 
        WHEN cr.user_external_id IN (
            SELECT cr_inner.user_external_id
            FROM prod_shatal_db.prodmysqldatabase.v3_cross_marketing cr_inner
            WHERE cr_inner.parent_external_id IS NOT NULL
            AND cr_inner.user_external_id IN (
                SELECT ph_inner.external_id
                FROM prod_shatal_db.prodmysqldatabase.v2_purchase_history ph_inner
                WHERE ph_inner.first_package = 1
            )
        ) THEN cr.global_parent_external_id
        ELSE NULL 
    END AS cr,
    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph_cr.date_added)) AS date_1st_ph_cr
FROM billing_analyst.mv_3pack_base_data t
LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph_fraud ON t.fraud = ph_fraud.external_id AND ph_fraud.first_package = 1
LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_cross_marketing cr ON t.external_id = cr.user_external_id
LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph_cr ON cr.global_parent_external_id = ph_cr.external_id AND ph_cr.first_package = 1;


CREATE MATERIALIZED VIEW billing_analyst.mv_3pack_final_data
DISTKEY (final_external_id)
SORTKEY (date_ph)
AS
SELECT 
    CASE 
        WHEN t.cr IS NOT NULL THEN t.cr
        WHEN t.date_1st_ph_fraud IS NULL THEN t.external_id
        ELSE t.fraud 
    END AS final_external_id,
    t.gender,
    t.age_group,
    t.date_1st_ph,
    t.date_reg,
    t.netw,
    t.site_id,
    t.os,
    t.country,
    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) AS date_ph,
    ROW_NUMBER() OVER (PARTITION BY 
        CASE 
            WHEN t.cr IS NOT NULL THEN t.cr
            WHEN t.date_1st_ph_fraud IS NULL THEN t.external_id
            ELSE t.fraud 
        END 
    ORDER BY ph.date_added) AS n
FROM billing_analyst.mv_3pack_enriched_data t
LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph ON 
    CASE 
        WHEN t.cr IS NOT NULL THEN t.cr
        WHEN t.date_1st_ph_fraud IS NULL THEN t.external_id
        ELSE t.fraud 
    END = ph.external_id
WHERE t.fraud IS NULL 
OR (DATE_PART(month, t.date_1st_ph) = DATE_PART(month, t.date_1st_ph_fraud) AND DATE_PART(year, t.date_1st_ph) = DATE_PART(year, t.date_1st_ph_fraud))
OR (DATE_PART(month, t.date_1st_ph) = DATE_PART(month, t.date_1st_ph_cr) AND DATE_PART(year, t.date_1st_ph) = DATE_PART(year, t.date_1st_ph_cr));


CREATE MATERIALIZED VIEW billing_analyst.mv_summary_3pack
DISTKEY (site_id)
SORTKEY (date, netw, site_id, os, country, age_group)
AS
SELECT 
    date_ph AS date,
    netw,
    site_id,
    os,
    country,
    age_group,
    COUNT(CASE WHEN date_ph = date_1st_ph AND gender = 0 THEN final_external_id END) AS pack3_day_to_day,
    COUNT(CASE WHEN DATEDIFF(day, date_1st_ph, date_ph) <= 3 AND gender = 0 THEN final_external_id END) AS pack3_3day,
    COUNT(CASE WHEN gender = 0 THEN final_external_id END) AS pack3_nakop,
    COUNT(CASE WHEN date_ph = date_1st_ph AND DATE_PART(month, date_1st_ph) = DATE_PART(month, date_reg) AND DATE_PART(year, date_1st_ph) = DATE_PART(year, date_reg) AND gender = 0 THEN final_external_id END) AS pack3_day_to_day_reg,
    COUNT(CASE WHEN DATEDIFF(day, date_1st_ph, date_ph) <= 3 AND DATE_PART(month, date_ph) = DATE_PART(month, date_reg) AND DATE_PART(year, date_ph) = DATE_PART(year, date_reg) AND gender = 0 THEN final_external_id END) AS pack3_3day_reg,
    COUNT(CASE WHEN date_ph = date_1st_ph AND gender = 1 THEN final_external_id END) AS pack3_day_to_day_women,
    COUNT(CASE WHEN DATEDIFF(day, date_1st_ph, date_ph) <= 3 AND gender = 1 THEN final_external_id END) AS pack3_3day_women,
    COUNT(CASE WHEN gender = 1 THEN final_external_id END) AS pack3_nakop_women,
    COUNT(CASE WHEN date_ph = date_1st_ph AND DATE_PART(month, date_1st_ph) = DATE_PART(month, date_reg) AND DATE_PART(year, date_1st_ph) = DATE_PART(year, date_reg) AND gender = 1 THEN final_external_id END) AS pack3_day_to_day_reg_women,
    COUNT(CASE WHEN DATEDIFF(day, date_1st_ph, date_ph) <= 3 AND DATE_PART(month, date_ph) = DATE_PART(month, date_reg) AND DATE_PART(year, date_ph) = DATE_PART(year, date_reg) AND gender = 1 THEN final_external_id END) AS pack3_3day_reg_women
FROM billing_analyst.mv_3pack_final_data
WHERE n = 3 AND DATE_PART(month, date_1st_ph) = DATE_PART(month, date_ph) AND DATE_PART(year, date_1st_ph) = DATE_PART(year, date_ph)
GROUP BY date_ph, netw, site_id, os, country, age_group;