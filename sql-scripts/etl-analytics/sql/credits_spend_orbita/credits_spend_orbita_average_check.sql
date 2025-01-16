CREATE MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_average_check
DISTSTYLE KEY DISTKEY(external_id) SORTKEY(date_added)
AS
WITH profiles AS (
    SELECT external_id, gender, site_id, 
           DATE(TIMESTAMP 'epoch' + CAST(register_date AS BIGINT) * INTERVAL '1 second') as registered
    FROM redshift_analytics_db.prodmysqldatabase.user_profile
    WHERE name NOT LIKE '%test%' AND email NOT LIKE '%test%'
        AND email NOT LIKE '%delete%' AND email NOT LIKE '%+%'
        AND tester = 0 AND email NOT LIKE '%upiple%' AND email NOT LIKE '%irens %'
),

actions_grouped AS (
    SELECT fp.date as date_added, fp.male_external_id as external_id,
        SUM(fp.action_price) * 0.22 as amount,
        CASE 
            WHEN fp.action_type IN ('GET_MEETING', 'MAKE_ORDER_APPROVE') THEN 'Gift'
            ELSE 'Balance'
        END as Action
    FROM prod_analytic_db.credits_spend_orbita.man_paid_actions fp
    WHERE fp.date >= DATEADD(MONTH, -2, CURRENT_DATE)
    GROUP BY fp.date, fp.male_external_id, Action
),

m1 AS (
    SELECT ag.date_added, ag.external_id, p.gender, p.site_id, p.registered,
        ag.amount, 
        CASE 
            WHEN pm.date_heigh_role <= ag.date_added THEN 2
            WHEN pm.date_maybe_height <= ag.date_added THEN 1 
            ELSE 0 
        END as type, 
        ag.Action
    FROM actions_grouped ag
    LEFT JOIN profiles p ON p.external_id = ag.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_paid_user_marked pm ON pm.external_id = ag.external_id
),

purchases_grouped AS (
    SELECT ph.date_added, ph.external_id, SUM(ph.price) as amount,
        'Пополнение' as Action
    FROM redshift_analytics_db.prodmysqldatabase.v2_purchase_history ph
    WHERE ph.date_added >= DATEADD(MONTH, -2, CURRENT_DATE)
    GROUP BY ph.date_added, ph.external_id
),

m2 AS (
    SELECT pg.date_added, pg.external_id, p.gender, p.site_id, p.registered,
        pg.amount, 
        CASE 
            WHEN pm.date_heigh_role <= pg.date_added THEN 2
            WHEN pm.date_maybe_height <= pg.date_added THEN 1 
            ELSE 0 
        END as type, 
        pg.Action
    FROM purchases_grouped pg
    LEFT JOIN profiles p ON p.external_id = pg.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_paid_user_marked pm ON pm.external_id = pg.external_id
),

combined_data AS (
    SELECT date_added, external_id, gender, site_id, registered as register_date, amount, type, Action
    FROM m1
    UNION ALL
    SELECT date_added, external_id, gender, site_id, registered, amount, type, Action
    FROM m2
)


SELECT cd.*, ph_agg.running_total, ph_agg.converted
FROM combined_data cd
LEFT JOIN (
    SELECT external_id, date_added,
        SUM(price) as running_total,
        MAX(CASE WHEN first_package = 1 THEN date_added END) as converted
    FROM redshift_analytics_db.prodmysqldatabase.v2_purchase_history
    GROUP BY external_id, date_added
) ph_agg ON ph_agg.external_id = cd.external_id 
    AND ph_agg.date_added <= cd.date_added
    group by cd.date_added, cd.external_id, cd.gender, cd.site_id, register_date, cd.amount, cd.type, cd.action, ph_agg.running_total, ph_agg.converted
    
    
    -------------------------------
    -------------------------------

    
    
    --- STEP 2. LOAD INTO TABLE FROM VIEW FOR ALL PERIOD

--create table prod_analytic_db.credits_spend_orbita.average_check
--DISTSTYLE KEY DISTKEY(external_id) SORTKEY(date_added)
--AS 
--SELECT * 
--FROM prod_analytic_db.credits_spend_orbita.mv_average_check

--- STEP 3. MODIFY VIEW FOR LAST # DAYS PERIOD AND REFRESH TABLE


CREATE OR REPLACE PROCEDURE prod_analytic_db.credits_spend_orbita.refresh_average_check()
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_average_check;
    
    DELETE 
		FROM prod_analytic_db.credits_spend_orbita.average_check 
		WHERE date_added >= CURRENT_DATE - INTERVAL '2 month'
		;

    INSERT INTO prod_analytic_db.credits_spend_orbita.average_check
		SELECT
		   	date_added
			,external_id
			,gender
			,site_id
			,amount
			,"type"
			,register_date
			,action
			,running_total
			,converted
		FROM prod_analytic_db.credits_spend_orbita.mv_average_check
		WHERE date_added >= CURRENT_DATE - INTERVAL '2 month';

    ANALYZE prod_analytic_db.credits_spend_orbita.average_check;
    
END;
$$ LANGUAGE plpgsql;


call prod_analytic_db.credits_spend_orbita.refresh_average_check();

--
--select date_added, count(*)
--from credits_spend_orbita.average_check
--group by date_added
--order by 1 desc
--;



    