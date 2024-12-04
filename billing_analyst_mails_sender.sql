-- DROP MATERIALIZED VIEW billing_analyst.mv_mails_sender

CREATE MATERIALIZED VIEW billing_analyst.mv_mails_sender
DISTKEY (man_external_id)
SORTKEY (dd,man_external_id, gender, domain_id, age_group)
AS
with tt1 as 
(
	SELECT 
		p.updated_at AS dd,
		p.man_external_id,
		p.id,
		p.type as "type"
    FROM redshift_analytics_db.prodmysqldatabase.v3_personal_invites p
    WHERE 1=1 
    		AND date(p.updated_at) = DATE(DATEADD(day, -1, getdate()))
    		AND p.status = 2
    		
    UNION ALL 
    
    SELECT 
    		sh.created_at AS dd,
    		sh.man_external_id,
    		sh.id, 
    		CASE WHEN sh.sender_type = 'Chat' THEN 'INVITE' ELSE 'LETTER' END AS "type"
    FROM redshift_analytics_db.prodmysqldatabase.v2_sender_history sh
    WHERE 1=1
    		AND date(sh.created_at) = DATE(DATEADD(day, -1, getdate()))
    		
    UNION ALL
    
    SELECT 
    		th.created_at AS dd,
    		th.man_external_id,
    		th.id,
    		'TRANSACTION' as "type"
    FROM redshift_analytics_db.prodmysqldatabase.v3_transaction_history th
    WHERE 1=1
    		AND th.transaction_type != 'MATCH' 
    		AND date(th.created_at) = DATE(DATEADD(day, -1, getdate()))
    )
      
SELECT 	a.dd,
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
LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON a.man_external_id = up.external_id;

/*

select 
	dd::date, 
	count(distinct man_external_id) men_sent, 
	count( man_external_id),
	count( id)/count(distinct man_external_id)
from billing_analyst.mv_mails_sender
group by 1
order by 1 desc
limit 5

*/
