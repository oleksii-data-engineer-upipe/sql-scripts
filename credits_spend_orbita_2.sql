CREATE MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_average_check
DISTSTYLE KEY DISTKEY(external_id) SORTKEY(date_added)
as
WITH base_profiles AS (
    SELECT 
        external_id, 
        gender, 
        site_id, 
		DATEADD(second, CAST(register_date AS BIGINT), '1970-01-01') as register_date
    FROM redshift_analytics_db.prodmysqldatabase.user_profile
    WHERE name NOT LIKE '%test%'
        AND email NOT LIKE '%test%'
        AND email NOT LIKE '%delete%'
        AND email NOT LIKE '%+%'
        AND email NOT LIKE '%upiple%'
        AND email NOT LIKE '%irens%'
        AND email NOT LIKE '%galaktica%'
        AND email NOT LIKE '%i.ua%'
        AND tester = 0
),

user_roles AS (
    SELECT 
        external_id,
        CAST(date_heigh_role AS DATE) AS date_heigh_role,
        CAST(date_maybe_height AS DATE) AS date_maybe_height
    FROM redshift_analytics_db.prodmysqldatabase.v3_paid_user_marked
),

paid_actions AS (
    SELECT
        CAST(fp.date AS DATE) AS date_added,
        fp.male_external_id AS external_id,
        up.gender,
        up.site_id,
        SUM(fp.action_price) * 0.22 AS amount,
        CASE 
            WHEN ur.date_heigh_role <= CAST(fp.date AS DATE) THEN 2
            WHEN ur.date_maybe_height <= CAST(fp.date AS DATE) THEN 1
            ELSE 0
        END AS type,
        up.register_date,
        CASE 
            WHEN fp.action_type IN ('GET_MEETING', 'MAKE_ORDER_APPROVE') THEN 'Gift'
            ELSE 'Balance'
        END AS action_type
    FROM prod_analytic_db.credits_spend_orbita.man_paid_actions fp
    INNER JOIN base_profiles up  	ON up.external_id = fp.male_external_id
    LEFT JOIN user_roles ur  		ON ur.external_id = fp.male_external_id
    WHERE CAST(fp.date AS DATE) >= DATEADD(day, -30, TRUNC(CURRENT_DATE))
    GROUP BY 1,2,3,4,6,7,8
),

recent_purchases AS (
    SELECT 
        external_id,
        CAST(date_added AS DATE) AS date_added,
        price,
        first_package
    FROM redshift_analytics_db.prodmysqldatabase.v2_purchase_history
    WHERE CAST(date_added AS DATE) >= DATEADD(day, -30, TRUNC(CURRENT_DATE))
),

purchase_data AS (
    SELECT
        ph.date_added,
        ph.external_id,
        up.gender,
        up.site_id,
        SUM(ph.price) AS amount,
        CASE 
            WHEN ur.date_heigh_role <= ph.date_added THEN 2
            WHEN ur.date_maybe_height <= ph.date_added THEN 1
            ELSE 0
        END AS type,
        up.register_date,
        'Пополнение' AS action_type
    FROM recent_purchases ph
    INNER JOIN base_profiles up  	ON up.external_id = ph.external_id
    LEFT JOIN user_roles ur 			ON ur.external_id = ph.external_id
    GROUP BY 1,2,3,4,6,7,8
),

combined_actions AS (
    SELECT * FROM paid_actions
    UNION ALL
    SELECT * FROM purchase_data
),

final_result AS (
    SELECT
        ca.*,
        SUM(rp.price) OVER (
            PARTITION BY ca.external_id 
            ORDER BY ca.date_added
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total,
        FIRST_VALUE(
            CASE 
                WHEN rp.first_package = 1 THEN rp.date_added 
                ELSE NULL 
            END IGNORE NULLS
        ) OVER (
            PARTITION BY ca.external_id 
            ORDER BY rp.date_added
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS converted_date
    FROM combined_actions ca
    LEFT JOIN recent_purchases rp
        ON rp.external_id = ca.external_id
        AND rp.date_added <= ca.date_added
)

SELECT *
FROM final_result
WHERE date_added = DATEADD(day, -1, TRUNC(CURRENT_DATE))
