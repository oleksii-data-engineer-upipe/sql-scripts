WITH profiles AS (
    SELECT  external_id, gender, site_id, date(register_date) as registered
    FROM profiles
    WHERE name NOT LIKE '%test%'
    AND email NOT LIKE '%test%'
    AND email NOT LIKE '%delete%'
    AND email NOT LIKE '%+%'
    AND tester = 0
    AND email NOT LIKE '%upiple%'
    AND email NOT LIKE '%irens %'
),
m1 as (
    SELECT 
        fp.date date_added,
        fp.male_external_id as external_id,
        p.gender,
        p.site_id,
        p.registered,
        SUM(fp.action_price) * 0.22 as amount,
        if(pm.date_heigh_role <= fp.date, 2, IF(pm.date_maybe_height <= fp.date, 1, 0)) as type,
        IF(fp.action_type IN ('GET_MEETING', 'MAKE_ORDER_APPROVE'), 'Gift', 'Balance') as Action
    FROM f_man_paid_actions fp
    LEFT JOIN profiles p ON p.external_id = fp.male_external_id
    LEFT JOIN paid_user_marked pm ON pm.external_id = fp.male_external_id
    WHERE fp.date = DATE(NOW()) - INTERVAL 1 DAY
    GROUP BY fp.date, fp.male_external_id, Action
),
m2 as (
    SELECT 
        ph.date_added,
        ph.external_id,
        p.gender,
        p.site_id,
        p.registered,
        SUM(ph.price) as amount,
        if(pm.date_heigh_role <= ph.date_added, 2, IF(pm.date_maybe_height <= ph.date_added, 1, 0)) as type,
        'Пополнение' as Action
    FROM purchase_history ph
    LEFT JOIN profiles p ON p.external_id = ph.external_id
    LEFT JOIN paid_user_marked pm ON pm.external_id = ph.external_id
    WHERE ph.date_added = DATE(NOW()) - INTERVAL 1 DAY
    GROUP BY ph.date_added, ph.external_id, Action
)

SELECT m1.*,
    SUM(ph.price) running_total,
    if(ph.first_package = 1, ph.date_added, NULL) converted
FROM m1
    LEFT JOIN purchase_history ph ON ph.external_id = m1.external_id
    AND ph.date_added <= m1.date_added
GROUP BY m1.date_added,
    m1.external_id
UNION ALL
SELECT m2.*,
    SUM(ph.price) running_total,
    if(ph.first_package = 1, ph.date_added, NULL) converted
FROM m2
    LEFT JOIN purchase_history ph ON ph.external_id = m2.external_id
    AND ph.date_added <= m2.date_added
GROUP BY m2.date_added,
    m2.external_id




    --- STEP 2. LOAD INTO TABLE FROM VIEW FOR ALL PERIOD

{# create table prod_analytic_db.credits_spend_orbita.average_check
DISTSTYLE KEY DISTKEY(external_id) SORTKEY(date_added)
AS 
SELECT * 
FROM prod_analytic_db.credits_spend_orbita.mv_average_check #}

--- STEP 3. MODIFY VIEW FOR LAST # DAYS PERIOD AND REFRESH TABLE


CREATE OR REPLACE PROCEDURE prod_analytic_db.credits_spend_orbita.refresh_average_check()
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_average_check;
    
    DELETE 
		FROM prod_analytic_db.credits_spend_orbita.average_check 
		WHERE date_added >= CURRENT_DATE - INTERVAL '7 days'
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
			,action_type
			,running_total
			,converted_date
		FROM prod_analytic_db.credits_spend_orbita.mv_average_check
		WHERE date_added >= CURRENT_DATE - INTERVAL '7 days';

    ANALYZE prod_analytic_db.credits_spend_orbita.average_check;
    
END;
$$ LANGUAGE plpgsql;


call prod_analytic_db.credits_spend_orbita.refresh_average_check();

-- REFRESH MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_average_check


-- select * from prod_analytic_db.credits_spend_orbita.mv_average_check

/*
SELECT date_added, count(*) 
FROM credits_spend_orbita.average_check
GROUP BY date_added 
ORDER BY 1 DESC 
LIMIT 10
;
*/

/*
SELECT date_added, count(*) 
FROM credits_spend_orbita.mv_average_check
GROUP BY date_added 
ORDER BY 1 DESC 
LIMIT 25
;
*/