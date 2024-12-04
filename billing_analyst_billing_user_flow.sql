CREATE MATERIALIZED VIEW billing_analyst.mv_billing_user_flow
DISTKEY (external_id)
SORTKEY ("date", "time", external_id, age_group)
AS
WITH base_data AS (
    SELECT 
        cp.id, 
        CASE 
            WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY') THEN DATE(cp.date_created)
            ELSE DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp))
        END AS date,
        CASE 
            WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY') THEN LEFT(REPLACE(cp.date_created::VARCHAR, '.', '-'), 19)
            ELSE LEFT(REPLACE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp)::VARCHAR, '.', '-'), 19)
        END AS time,
        cp.external_id, 
        cp.payment_method, 
        cp.state, 
        cp.reason, 
        cp.amount,
        t.time_first_ph,
        CASE 
            WHEN t.time_first_ph >= 
                CASE 
                    WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY') THEN LEFT(REPLACE(cp.date_created::VARCHAR, '.', '-'), 19)
                    ELSE LEFT(REPLACE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp)::VARCHAR, '.', '-'), 19)
                END THEN 0
            ELSE 1
        END AS is_check,
        COALESCE(up.name::VARCHAR, 'N/A')||', '||COALESCE(up.age::VARCHAR, 'N/A') AS user,
        CASE WHEN fl.man_external_id IS NULL THEN 0 ELSE 1 END AS fraud,
        CASE WHEN up.site_id = 1 THEN 'sofiadate.com' ELSE s.domain END AS domain,
        up.gender,
        CASE
            WHEN up.age < 35 THEN 'до 35'
            WHEN up.age BETWEEN 35 AND 44 THEN '35-44'
            WHEN up.age BETWEEN 45 AND 90 THEN '45-90'
            WHEN up.age > 90 THEN '91+'
            ELSE 'Невідомо'
        END AS age_group
    FROM redshift_analytics_db.prodmysqldatabase.v2_center_payment cp
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON cp.external_id = up.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_frod_list fl ON cp.external_id = fl.man_external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_site s ON up.site_id = s.id
    LEFT JOIN (
        SELECT 
            cp_inner.external_id,
            MIN(
                CASE 
                    WHEN cp_inner.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY') THEN LEFT(REPLACE(cp_inner.date_created::VARCHAR, '.', '-'), 19)
                    ELSE LEFT(REPLACE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp_inner.date_created::timestamp)::VARCHAR, '.', '-'), 19)
                END
            ) AS time_first_ph
        FROM redshift_analytics_db.prodmysqldatabase.v2_center_payment cp_inner
        WHERE cp_inner.reason = 'approved'
        GROUP BY 1
    ) t ON cp.external_id = t.external_id
    WHERE cp.reason NOT LIKE '%bandone%'
    AND cp.reason NOT LIKE '%Cancelled by customer%'
    AND cp.external_id != 0
    AND up.name NOT LIKE '%test%'
    AND up.email NOT LIKE '%test%'
    AND up.email NOT LIKE '%delete%'
    AND up.email NOT LIKE '%+%'
    AND up.tester = 0
    AND up.country != 222
    AND up.email NOT LIKE '%upiple%'
    AND up.email NOT LIKE '%irens%'
    AND up.email NOT LIKE '%galaktica%'
    AND cp.external_id IN (
        SELECT DISTINCT cp_inner.external_id
        FROM redshift_analytics_db.prodmysqldatabase.v2_center_payment cp_inner
        WHERE CASE 
            WHEN cp_inner.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY') THEN DATE(cp_inner.date_created)
            ELSE DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp_inner.date_created::timestamp))
        END >= DATEADD(year, -1, CURRENT_DATE)
    )
),
numbered_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY time) AS row_num,
        LAG(external_id) OVER (ORDER BY external_id, time) AS prev_ext_id,
        LAG(date) OVER (ORDER BY external_id, time) AS prev_date,
        LAG(reason) OVER (ORDER BY external_id, time) AS prev_reason,
        LAG(is_check) OVER (ORDER BY external_id, time) AS prev_is_check
    FROM base_data
)
SELECT 
    nd.*,
    CASE 
        WHEN nd.prev_ext_id = nd.external_id AND nd.prev_date = nd.date AND nd.prev_is_check = nd.is_check THEN
            CASE 
                WHEN nd.prev_reason != 'approved' THEN 
                    ROW_NUMBER() OVER (PARTITION BY nd.external_id, nd.date, nd.is_check ORDER BY nd.time)
                ELSE 
                    ROW_NUMBER() OVER (PARTITION BY nd.external_id, nd.date, nd.is_check, 
                                       CASE WHEN nd.prev_reason = 'approved' THEN nd.row_num ELSE 0 END 
                                       ORDER BY nd.time)
            END
        ELSE 1
    END AS num_payment_method,
    SUM(CASE WHEN nd.reason = 'approved' THEN 1 ELSE 0 END) OVER (
        PARTITION BY nd.external_id 
        ORDER BY nd.time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS num_pack
FROM numbered_data nd
;

-- select date::date, count(*) from billing_analyst.mv_billing_user_flow group by 1 order by 1 desc limit 10
