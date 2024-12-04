create MATERIALIZED VIEW billing_analyst.mv_billing_user_flow_routing 
DISTKEY (external_id)
SORTKEY (time)
AS
WITH payment_data AS (
    SELECT 
        e."id"::int AS id,
        DATE(e."createdAt"::TIMESTAMP) AS date,
        e."createdAt"::TIMESTAMP AS time,
        e."userExternalId" AS external_id,
        CASE WHEN e."merchantId" IN (4, 5) THEN 'ACQUIRING' WHEN m.bank LIKE '%UNLIMINT%' THEN 'CARDPAY' ELSE m.bank END::VARCHAR AS payment_method,
        CASE WHEN e.type = 'PAYMENT_SUCCEDED' THEN 'DEPOSITED' ELSE 'DECLINED' END::VARCHAR AS state,
        CASE WHEN e.type = 'PAYMENT_SUCCEDED' THEN 'approved'
            ELSE REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') END::VARCHAR AS reason,
        e.details."paymentResponse"."order"."amount"::float AS amount,
        CASE when e.details."paymentResponse"."order"."cardBin"::varchar = '' THEN NULL END AS card
    FROM redshift_analytics_db.sphera."Event" e
    LEFT JOIN redshift_analytics_db.sphera."Merchant" m ON e."merchantId" = m.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON e."userExternalId" = up.external_id
    WHERE e.type IN ('PAYMENT_FAILED', 'PAYMENT_SUCCEDED')
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
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%ancelled by customer%'
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%sufficient%'
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%AUTH Error (code = 60022)%'
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%— (code = —)%'
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%Stolen Card%'
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%Your card has expired. (code = expired_card)%'
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%Expired Card%'
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%(code = 60022)%'
        AND REGEXP_REPLACE(e.details."paymentResponse"."message"::varchar, '^.*Error:', '') NOT LIKE '%MAC 02: Policy%'
),
first_payment AS (
    SELECT 
        "userExternalId" AS external_id,
        MIN("createdAt"::TIMESTAMP) AS time_1st_ph
    FROM redshift_analytics_db.sphera."Event"
    WHERE type = 'PAYMENT_SUCCEDED'
    GROUP BY 1
),
payment_analysis AS (
    SELECT 
        pd.*,
        fp.time_1st_ph,
        CASE WHEN pd.time > fp.time_1st_ph THEN 1 ELSE 0 END AS check,
        up.name||', '||up.age::VARCHAR AS user,
        CASE WHEN fl.man_external_id IS NULL THEN 0 ELSE 1 END AS fraud,
        CASE WHEN site.domain = 'www.sofiadate.com' THEN 'sofiadate.com' WHEN up.site_id IS NULL THEN NULL ELSE site.domain END AS domain,
        up.gender,
        ROW_NUMBER() OVER (PARTITION BY pd.external_id, pd.date, 
                           CASE WHEN pd.time > fp.time_1st_ph THEN 1 ELSE 0 END 
                           ORDER BY pd.time) AS num_payment_method,
        SUM(CASE WHEN pd.reason = 'approved' THEN 1 ELSE 0 END) 
            OVER (PARTITION BY pd.external_id ORDER BY pd.time ROWS UNBOUNDED PRECEDING) AS num_pack
    FROM payment_data pd
    LEFT JOIN first_payment fp ON pd.external_id = fp.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON pd.external_id = up.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_utm u ON pd.external_id = u.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_frod_list fl ON pd.external_id = fl.man_external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_networks n ON u.network_id = n.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_sources s ON n.parent_id = s.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_managers m ON s.parent_id = m.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.country c ON up.country = c.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_user_register_device d ON pd.external_id = d.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_site site ON site.id = up.site_id
)


SELECT * 
FROM payment_analysis
;


--
--	select date::date, count(1) 
--  from billing_analyst.mv_billing_user_flow_routing 
--  group by 1 order by 1 desc limit 7
