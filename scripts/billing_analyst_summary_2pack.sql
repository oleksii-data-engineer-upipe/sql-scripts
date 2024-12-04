CREATE refsMATERIALIZED VIEW billing_analyst.mv_summary_2pack
DISTKEY (site_id)
SORTKEY (date, netw, site_id, os, country, age_group)
AS
WITH base_data AS (
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
    AND DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) BETWEEN '2023-01-01' AND DATEADD(day, -1, CURRENT_DATE)
),
cross_marketing AS (
    SELECT cr.user_external_id, cr.global_parent_external_id
    FROM prod_shatal_db.prodmysqldatabase.v3_cross_marketing cr
    WHERE cr.parent_external_id IS NOT NULL
    AND cr.user_external_id IN (
        SELECT ph_inner.external_id
        FROM prod_shatal_db.prodmysqldatabase.v2_purchase_history ph_inner
        WHERE ph_inner.first_package = 1
    )
),
enriched_data AS (
    SELECT 
        b.*,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph_fraud.date_added)) AS date_1st_ph_fraud,
        cm.global_parent_external_id AS cr,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph_cr.date_added)) AS date_1st_ph_cr
    FROM base_data b
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph_fraud ON b.fraud = ph_fraud.external_id AND ph_fraud.first_package = 1
    LEFT JOIN cross_marketing cm ON b.external_id = cm.user_external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph_cr ON cm.global_parent_external_id = ph_cr.external_id AND ph_cr.first_package = 1
),
final_data AS (
    SELECT 
        CASE 
            WHEN e.cr IS NOT NULL THEN e.cr
            WHEN e.date_1st_ph_fraud IS NULL THEN e.external_id
            ELSE e.fraud 
        END AS final_external_id,
        e.gender,
        e.age_group,
        e.date_1st_ph,
        e.date_reg,
        e.netw,
        e.site_id,
        e.os,
        e.country,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', ph.date_added)) AS date_ph,
        ROW_NUMBER() OVER (PARTITION BY 
            CASE 
                WHEN e.cr IS NOT NULL THEN e.cr
                WHEN e.date_1st_ph_fraud IS NULL THEN e.external_id
                ELSE e.fraud 
            END 
        ORDER BY ph.date_added) AS n
    FROM enriched_data e
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_purchase_history ph ON 
        CASE 
            WHEN e.cr IS NOT NULL THEN e.cr
            WHEN e.date_1st_ph_fraud IS NULL THEN e.external_id
            ELSE e.fraud 
        END = ph.external_id
    WHERE e.fraud IS NULL 
    OR (DATE_PART(month, e.date_1st_ph) = DATE_PART(month, e.date_1st_ph_fraud) AND DATE_PART(year, e.date_1st_ph) = DATE_PART(year, e.date_1st_ph_fraud))
    OR (DATE_PART(month, e.date_1st_ph) = DATE_PART(month, e.date_1st_ph_cr) AND DATE_PART(year, e.date_1st_ph) = DATE_PART(year, e.date_1st_ph_cr))
)
SELECT 
    f.date_ph AS date,
    f.netw,
    f.site_id,
    f.os,
    f.country,
    f.age_group,
    COUNT(CASE WHEN f.date_ph = f.date_1st_ph AND f.gender = 0 THEN f.final_external_id END) AS pack2_day_to_day,
    COUNT(CASE WHEN DATEDIFF(day, f.date_1st_ph, f.date_ph) <= 3 AND f.gender = 0 THEN f.final_external_id END) AS pack2_3day,
    COUNT(CASE WHEN f.gender = 0 THEN f.final_external_id END) AS pack2_nakop,
    COUNT(CASE WHEN f.date_ph = f.date_1st_ph AND DATE_PART(month, f.date_1st_ph) = DATE_PART(month, f.date_reg) AND DATE_PART(year, f.date_1st_ph) = DATE_PART(year, f.date_reg) AND f.gender = 0 THEN f.final_external_id END) AS pack2_day_to_day_reg,
    COUNT(CASE WHEN DATEDIFF(day, f.date_1st_ph, f.date_ph) <= 3 AND DATE_PART(month, f.date_ph) = DATE_PART(month, f.date_reg) AND DATE_PART(year, f.date_ph) = DATE_PART(year, f.date_reg) AND f.gender = 0 THEN f.final_external_id END) AS pack2_3day_reg,
    COUNT(CASE WHEN f.date_ph = f.date_1st_ph AND f.gender = 1 THEN f.final_external_id END) AS pack2_day_to_day_women,
    COUNT(CASE WHEN DATEDIFF(day, f.date_1st_ph, f.date_ph) <= 3 AND f.gender = 1 THEN f.final_external_id END) AS pack2_3day_women,
    COUNT(CASE WHEN f.gender = 1 THEN f.final_external_id END) AS pack2_nakop_women,
    COUNT(CASE WHEN f.date_ph = f.date_1st_ph AND DATE_PART(month, f.date_1st_ph) = DATE_PART(month, f.date_reg) AND DATE_PART(year, f.date_1st_ph) = DATE_PART(year, f.date_reg) AND f.gender = 1 THEN f.final_external_id END) AS pack2_day_to_day_reg_women,
    COUNT(CASE WHEN DATEDIFF(day, f.date_1st_ph, f.date_ph) <= 3 AND DATE_PART(month, f.date_ph) = DATE_PART(month, f.date_reg) AND DATE_PART(year, f.date_ph) = DATE_PART(year, f.date_reg) AND f.gender = 1 THEN f.final_external_id END) AS pack2_3day_reg_women
FROM final_data f
WHERE f.n = 2 AND DATE_PART(month, f.date_1st_ph) = DATE_PART(month, f.date_ph) AND DATE_PART(year, f.date_1st_ph) = DATE_PART(year, f.date_ph)
GROUP BY f.date_ph, f.netw, f.site_id, f.os, f.country, f.age_group
;