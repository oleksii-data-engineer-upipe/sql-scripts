CREATE MATERIALIZED VIEW billing_analyst.mv_billing_stat_alt
DISTKEY (external_id)
SORTKEY (time)
as 
WITH base_payments AS (
    SELECT cp.id,
           CASE WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY')
                THEN cp.date_created::timestamp
                ELSE CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp)
           END::date AS "date",
        
           CASE WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY')
                THEN cp.date_created::timestamp
                ELSE CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp)
           END::timestamp AS "time",
               
           cp.external_id,
           up.gender,
           cp.payment_method::varchar as payment_method,
           cp.state::varchar as state,
           cp.reason::varchar as reason,
           cp.amount,
           cp.mid::varchar as mid,
           cp.order_id,
           cp.description,
           cp.card_number as card,
           0 as pci
    FROM redshift_analytics_db.prodmysqldatabase.v2_center_payment cp
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON cp.external_id = up.external_id
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
          AND cp.description NOT LIKE 'Sphera%'
          AND cp.external_id IN (
              SELECT DISTINCT cp.external_id
              FROM redshift_analytics_db.prodmysqldatabase.v2_center_payment cp
              WHERE CASE WHEN cp.payment_method IN ('PASTABANK', 'PASTABANK_APPLEPAY')
                        THEN cp.date_created::date
                        ELSE CONVERT_TIMEZONE('UTC', 'Europe/Kiev', cp.date_created::timestamp)::date
                   END >= DATEADD(year, -1, CURRENT_DATE)
          )
),

sphera_events AS (
    SELECT e.id,
           TO_TIMESTAMP(e."createdAt", 'YYYY-MM-DD HH24:MI:SS')::date as "date",
           TO_TIMESTAMP(e."createdAt", 'YYYY-MM-DD HH24:MI:SS') as "time",
           e."userExternalId" as external_id,
           up.gender,
           (CASE 
               WHEN e."merchantId" IN (4, 5) THEN 'ACQUIRING'
               WHEN m."bank" = 'UNLIMINT' AND m."type" = 'GOOGLE_PAY' THEN 'CARDPAY_GOOGLE_PAY'
               WHEN m."bank" LIKE '%UNLIMINT%' THEN 'CARDPAY'
               WHEN m."bank" = 'PASTABANK' AND m."type" = 'APPLE_PAY' THEN 'PASTABANK_APPLEPAY'
               ELSE m."bank" 
           END)::varchar as payment_method,
           (CASE WHEN e."type" = 'PAYMENT_SUCCEDED' THEN 'DEPOSITED' ELSE 'DECLINED' END)::varchar as state,
           (CASE WHEN e."type" = 'PAYMENT_SUCCEDED' THEN 'approved' 
                 ELSE SPLIT_PART(e."details"."paymentResponse".message::varchar, 'Error:', 2)
            END)::varchar as reason,
           e."details"."paymentResponse".order.amount::float as amount,
           NULL AS mid,
           NULL AS order_id,
           NULL as description,
           CASE 
               WHEN m."bank" = 'UNIVERSEPAY' AND LENGTH(e."details"::varchar) < 1000 
                    THEN CONCAT(
                         e."details"."paymentResponse".order.cardBin::varchar,
                         SUBSTRING(e."details"."paymentResponse".order.cardMask::varchar, 7))
               WHEN m."bank" = 'UNIVERSEPAY' 
                    THEN CONCAT(
                         e."details"."paymentResponse".response.payment_method.bin::varchar,
                         SUBSTRING(e."details"."paymentResponse".order.cardMask::varchar, 7))
               WHEN POSITION('masked_pan' IN e."details"::varchar) > 0 AND m."bank" = 'UNLIMINT'
                    THEN e."details"."paymentResponse".response.card_account.masked_pan::varchar
               WHEN m."bank" = 'UNLIMINT'
                    THEN CONCAT(
                         e."details"."paymentResponse".order.cardBin::varchar,
                         RIGHT(e."details"."paymentResponse".order.cardMask::varchar, 10))
               WHEN POSITION('Number' IN e."details"::varchar) > 0
                    THEN e."details"."paymentResponse".response.Card.Number::varchar
               WHEN POSITION('masked_pan' IN e."details"::varchar) > 0
                    THEN e."details"."paymentResponse".response.card_account.masked_pan::varchar
               WHEN m."bank" = 'PASTABANK' AND LENGTH(e."details"::varchar) < 1200
                    THEN CONCAT(
                         e."details"."paymentResponse".order.cardBin::varchar,
                         SUBSTRING(e."details"."paymentResponse".order.cardMask::varchar, 7))
               ELSE NULL 
           END as card,
           1 as pci
    FROM redshift_analytics_db.sphera."Event" e
    LEFT JOIN redshift_analytics_db.sphera."Merchant" m ON e."merchantId" = m.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON e."userExternalId" = up.external_id
    WHERE e."type" IN ('PAYMENT_FAILED', 'PAYMENT_SUCCEDED')
          AND up.name NOT LIKE '%test%'
          AND up.email NOT LIKE '%test%'
          AND up.email NOT LIKE '%delete%'
          AND up.email NOT LIKE '%+%'
          AND up.tester = 0
          AND up.country != 222
          AND up.email NOT LIKE '%upiple%'
          AND up.email NOT LIKE '%irens%'
          AND up.email NOT LIKE '%galaktica%'
          AND SPLIT_PART(e."details"."paymentResponse".message::varchar, 'Error:', 2) NOT LIKE '%bandone%'
          AND SPLIT_PART(e."details"."paymentResponse".message::varchar, 'Error:', 2) NOT LIKE '%ancelled by customer%'
),

combined_data AS (
    SELECT * FROM base_payments
    UNION ALL
    SELECT * FROM sphera_events
    --ORDER BY external_id, "time"
    --limit 10
),

enriched_data AS (
    SELECT t1.*,
           up.name::varchar || ', ' || up.age::varchar AS "user",
           CASE WHEN fl.man_external_id IS NULL THEN 0 ELSE 1 END AS fraud,
           ROW_NUMBER() OVER (PARTITION BY t1.external_id, t1.payment_method ORDER BY t1."time") AS num_try,
           MIN(t1."time") OVER (PARTITION BY t1.external_id) AS time_first_try,
           MIN(CASE WHEN t1.reason = 'approved' THEN DATE(t1."time") ELSE NULL END) 
               OVER (PARTITION BY t1.external_id) AS date_1st_ph,
           MIN(CASE WHEN t1.reason = 'approved' THEN t1."time" ELSE NULL END) 
               OVER (PARTITION BY t1.external_id) AS time_1st_ph,
           LAG(t1.state) OVER (PARTITION BY t1.external_id ORDER BY t1.external_id, t1."time") AS "lag state",
           LEAD(t1.state) OVER (PARTITION BY t1.external_id ORDER BY t1.external_id, t1."time") AS "next state",
           LAG(t1.payment_method) OVER (PARTITION BY t1.external_id ORDER BY t1.external_id, t1."time") AS "lag payment_method",
           LEAD(t1.payment_method) OVER (PARTITION BY t1.external_id ORDER BY t1.external_id, t1."time") AS "next payment_method",
           LAG(t1.reason) OVER (PARTITION BY t1.external_id, t1.payment_method ORDER BY t1.external_id, t1."time") AS "lag reason",
           CASE WHEN t1.reason = 'approved' THEN 1 ELSE NULL END AS ph,
           n.netw,
           s.name AS source,
           m.name AS manager,
           CASE WHEN up.country IS NULL THEN NULL ELSE c.country_name END AS country,
           d.os,
           CASE WHEN site.domain = 'www.sofiadate.com' THEN 'sofiadate.com'
                WHEN up.site_id IS NULL THEN NULL
                ELSE site.domain
           END AS domain,
           DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at::timestamp)) AS date_reg,
           up.age,
           up.abtest
    FROM combined_data t1
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON t1.external_id = up.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_utm u ON t1.external_id = u.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_frod_list fl ON t1.external_id = fl.man_external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_networks n ON u.network_id = n.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_sources s ON n.parent_id = s.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_managers m ON s.parent_id = m.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.country c ON up.country = c.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_user_register_device d ON t1.external_id = d.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_site site ON site.id = up.site_id
)

SELECT ed.*,
       CASE WHEN ed.ph = 1 
            THEN ROW_NUMBER() OVER (PARTITION BY ed.external_id, ed.ph ORDER BY ed."time")
            ELSE NULL 
       END AS num_ph,
       CASE WHEN ed."time" > ed.time_1st_ph THEN 1 ELSE 0 END AS check
FROM enriched_data ed
;

