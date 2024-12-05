
create MATERIALIZED VIEW prod_analytic_db.billing_analyst.mv_mails_sender
DISTSTYLE KEY 
DISTKEY(man_external_id) 
SORTKEY(dd)
AS

WITH tt1 AS (
    SELECT 
        p.updated_at AS dd,
        p.man_external_id,
        p.id,
        p.type AS "type"
    FROM redshift_analytics_db.prodmysqldatabase.v3_personal_invites p
    WHERE DATE(p.updated_at) > DATE(DATEADD(year, -1, GETDATE()))
      AND p.status = 2

    UNION ALL 

    SELECT 
        sh.created_at AS dd,
        sh.man_external_id,
        sh.id,
        CASE 
            WHEN sh.sender_type = 'Chat' THEN 'INVITE' 
            ELSE 'LETTER' 
        END AS "type"
    FROM redshift_analytics_db.prodmysqldatabase.v2_sender_history sh
    WHERE DATE(sh.created_at) > DATE(DATEADD(year, -1, GETDATE()))

    UNION ALL

    SELECT 
        th.created_at AS dd,
        th.man_external_id,
        th.id,
        'TRANSACTION' AS "type"
    FROM redshift_analytics_db.prodmysqldatabase.v3_transaction_history th
    WHERE th.transaction_type != 'MATCH'
      AND DATE(th.created_at) > DATE(DATEADD(year, -1, GETDATE()))
)

SELECT 
    a.dd,
    a.man_external_id,
    a.id,
    a.type,
    up.gender,
    up.site_id AS domain_id,
    CASE
        WHEN up.age < 35 THEN 'до 35'
        WHEN up.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN up.age BETWEEN 45 AND 90 THEN '45-90'
        WHEN up.age > 90 THEN '91+'
        ELSE 'Невідомо'
    END AS age_group
FROM tt1 a
LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up 
    ON a.man_external_id = up.external_id
;

 /* SELECT 
	dd::date, 
	count(distinct man_external_id) as unique_users,
	count(distinct id) as unique_sent,
	count(*) as all
 FROM prod_analytic_db.billing_analyst.mv_mails_sender 				
 GROUP BY 1 
 ORDER BY 1 DESC 
 LIMIT 20 */