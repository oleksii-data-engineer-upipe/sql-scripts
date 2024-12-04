CREATE MATERIALIZED VIEW billing_analyst.mv_billing_stat_alt
DISTKEY (external_id)
SORTKEY (time)
as 
with cte as (
        SELECT 
            cp.id,
            CASE WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY') THEN DATE(cp.date_created)
                ELSE DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp))
            END AS date,
            CASE WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY') THEN LEFT(REPLACE(cp.date_created::VARCHAR, '.', '-'), 19)
                ELSE LEFT(REPLACE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp)::VARCHAR, '.', '-'), 19)
            END AS time,
            cp.external_id,
            up.gender,
            cp.payment_method::VARCHAR AS payment_method,
            cp.state::VARCHAR AS state,
            cp.reason::VARCHAR AS reason,
            cp.amount,
            cp.mid::VARCHAR AS mid,
            cp.order_id,
            cp.description,
            cp.card_number AS card
        FROM prod_shatal_db.prodmysqldatabase.v2_center_payment cp
        LEFT JOIN prod_shatal_db.prodmysqldatabase.user_profile up ON cp.external_id = up.external_id
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
                SELECT DISTINCT cp.external_id
                FROM prod_shatal_db.prodmysqldatabase.v2_center_payment cp
                WHERE CASE 
                    WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY') THEN DATE(cp.date_created)
                    ELSE DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp))
                END >= DATEADD(year, -1, CURRENT_DATE)
            )
        UNION ALL
        SELECT 
            e.id::int AS id,
            DATE(e."createdAt"::TIMESTAMP) AS date,
            CAST(e."createdAt" AS VARCHAR) AS time,
            e."userExternalId" AS external_id,
            up.gender,
            CASE WHEN e."merchantId" IN (4, 5) THEN 'ACQUIRING'
                WHEN m.bank LIKE '%UNLIMINT%' THEN 'CARDPAY'
                ELSE m.bank
            END::VARCHAR AS payment_method,
            CASE WHEN e.type = 'PAYMENT_SUCCEDED' THEN 'DEPOSITED'
                ELSE 'DECLINED'
            END::VARCHAR AS state,
            CASE WHEN e.type = 'PAYMENT_SUCCEDED' THEN 'approved'
                ELSE REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '')
            END::VARCHAR AS reason,
            e.details."paymentResponse"."order"."amount"::float AS amount,
            NULL::VARCHAR AS mid,
            NULL::VARCHAR AS order_id,
            NULL::VARCHAR AS description,
            e.details."paymentResponse"."order"."cardBin"::varchar AS card
        FROM prod_shatal_db.sphera."Event" e
        LEFT JOIN prod_shatal_db.sphera."Merchant" m ON e."merchantId" = m.id
        LEFT JOIN prod_shatal_db.prodmysqldatabase.user_profile up ON e."userExternalId" = up.external_id
        WHERE e.type IN ('PAYMENT_FAILED', 'PAYMENT_SUCCEDED')
            AND m.bank NOT LIKE '%PASTABANK%'
            AND up.name NOT LIKE '%test%'
            AND up.email NOT LIKE '%test%'
            AND up.email NOT LIKE '%delete%'
            AND up.email NOT LIKE '%+%'
            AND up.tester = 0
            AND up.country != 222
            AND up.email NOT LIKE '%upiple%'
            AND up.email NOT LIKE '%irens%'
            AND up.email NOT LIKE '%galaktica%'
            AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%bandone%'
            AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%Cancelled by customer%'
    )
    
SELECT 
    pp1.*,
    CASE WHEN pp1.ph = 1 THEN ROW_NUMBER() OVER (PARTITION BY pp1.external_id, pp1.ph ORDER BY pp1.time)
        ELSE NULL 
    END AS num_ph,
    CASE WHEN pp1.time > pp1.time_1st_ph THEN 1 ELSE 0 END AS check
FROM (
    SELECT 
        t1.*,
        up.name||', '||up.age::VARCHAR AS "user",
        CASE WHEN fl.man_external_id IS NULL THEN 0 ELSE 1 END AS fraud,
        ROW_NUMBER() OVER (PARTITION BY t1.external_id, t1.payment_method ORDER BY t1.time) AS num_try,
        MIN(t1.time) OVER (PARTITION BY t1.external_id) AS time_first_try,
        MIN(CASE WHEN t1.reason = 'approved' THEN DATE(t1.time) ELSE NULL END) 	OVER (PARTITION BY t1.external_id) AS date_1st_ph,
        MIN(CASE WHEN t1.reason = 'approved' THEN t1.time 		ELSE NULL END) 	OVER (PARTITION BY t1.external_id) AS time_1st_ph,
        LAG(t1.state) 			OVER (PARTITION BY t1.external_id ORDER BY t1.external_id, t1.time) AS lag_state,
        LEAD(t1.state) 			OVER (PARTITION BY t1.external_id ORDER BY t1.external_id, t1.time) AS next_state,
        LAG(t1.payment_method) 	OVER (PARTITION BY t1.external_id ORDER BY t1.external_id, t1.time) AS lag_payment_method,
        LEAD(t1.payment_method) OVER (PARTITION BY t1.external_id ORDER BY t1.external_id, t1.time) AS next_payment_method,
        LAG(t1.reason) 			OVER (PARTITION BY t1.external_id, t1.payment_method ORDER BY t1.external_id, t1.time) AS lag_reason,
        CASE WHEN t1.reason = 'approved' THEN 1 ELSE NULL END AS ph,
        n.netw,
        s.name AS source,
        m.name AS manager,
        CASE WHEN up.country IS NULL THEN NULL ELSE c.country_name END AS country,
        d.os,
        CASE WHEN site.domain = 'www.sofiadate.com' THEN 'sofiadate.com'
            WHEN up.site_id IS NULL THEN NULL ELSE site.domain END AS domain,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at::timestamp)) AS date_reg,
        up.age,
        up.abtest
    FROM cte t1
    LEFT JOIN prod_shatal_db.prodmysqldatabase.user_profile up 	ON t1.external_id = up.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_utm u 		ON t1.external_id = u.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v2_frod_list fl 	ON t1.external_id = fl.man_external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_networks n 	ON u.network_id = n.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_sources s 	ON n.parent_id = s.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_managers m 	ON s.parent_id = m.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.country c 		ON up.country = c.id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_user_register_device d ON t1.external_id = d.external_id
    LEFT JOIN prod_shatal_db.prodmysqldatabase.v3_site site 	ON site.id = up.site_id
) pp1
;




