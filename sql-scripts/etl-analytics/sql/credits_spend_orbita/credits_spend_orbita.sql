CREATE SCHEMA credits_spend_orbita;

--CREATE drop MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.f_users_free_given
--DISTSTYLE KEY DISTKEY(user_id) SORTKEY(date_created)
--AS
--SELECT 
--    DATE(l.date_created) AS date_created,
--    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', l.date_created)) AS date_created_kiev,
--    l.user_id,
--    l.action_price AS free_given
--FROM redshift_analytics_db.prodmysqldatabase.log l
--WHERE l.action_type = 'REGISTRATION_BONUS'
--
--CREATE drop MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.f_users_free_spent
--DISTSTYLE KEY DISTKEY(user_id) SORTKEY(date_created)
--AS
--SELECT 
--    DATE(l.date_created) AS date_created,
--    DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', l.date_created)) AS date_created_kiev,
--    l.user_id,
--    l.operator_id,
--    SUM(l.action_price) AS free_spent
--FROM redshift_analytics_db.prodmysqldatabase.log l
--WHERE l.is_male = 1
--    AND l.reward_status = 2
--    AND l.operator_id != 0
--GROUP BY 1, 2, 3, 4
--
--
--CREATE drop MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.f_man_paid_actions
--DISTSTYLE KEY DISTKEY(operator_id) SORTKEY(date)
--AS
--SELECT 
--    t.date,
--    t.date_kiev,
--    t.operator_id,
--    up.external_id AS male_external_id,
--    wi.external_id AS female_external_id,
--    t.action_type,
--    t.action_price,
--    t.operator_price
--FROM (
--    SELECT 
--        DATE(l.date_created) AS date,
--        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', l.date_created)) AS date_kiev,
--        l.operator_id,
--        l.user_id,
--        l.profile_id,
--        l.action_type,
--        SUM(l.action_price) AS action_price,
--        SUM(l.operator_price) AS operator_price
--    FROM redshift_analytics_db.prodmysqldatabase.log l
--    WHERE l.is_male = 1
--        AND l.reward_status = 1
--        AND l.operator_id != 0
--    GROUP BY 1, 2, 3, 4, 5, 6
--) t
--LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON t.user_id = up.id
--LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_woman_information wi ON t.profile_id = wi.id;



CREATE TABLE credits_spend_orbita.users_free_given
DISTSTYLE KEY DISTKEY(user_id) SORTKEY(date_created)
AS SELECT * FROM prod_analytic_db.credits_spend_orbita.f_users_free_given;

CREATE TABLE credits_spend_orbita.users_free_spent
DISTSTYLE KEY DISTKEY(user_id) SORTKEY(date_created)
AS SELECT * FROM prod_analytic_db.credits_spend_orbita.f_users_free_spent;

CREATE TABLE credits_spend_orbita.man_paid_actions
DISTSTYLE KEY DISTKEY(operator_id) SORTKEY(date)
AS SELECT * FROM prod_analytic_db.credits_spend_orbita.f_man_paid_actions;



CREATE OR REPLACE PROCEDURE prod_analytic_db.credits_spend_orbita.refresh()
AS $$
BEGIN
    -- Створення тимчасової таблиці для f_users_free_given
    CREATE TEMP TABLE temp_users_free_given AS
    SELECT 
        DATE(l.date_created) AS date_created,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', l.date_created)) AS date_created_kiev,
        l.user_id,
        l.action_price AS free_given
    FROM redshift_analytics_db.prodmysqldatabase.log l
    WHERE l.action_type = 'REGISTRATION_BONUS'
    AND DATE(l.date_created) > DATEADD(week, -1, CURRENT_DATE);

    -- Створення тимчасової таблиці для f_users_free_spent
    CREATE TEMP TABLE temp_users_free_spent AS
    SELECT 
        DATE(l.date_created) AS date_created,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', l.date_created)) AS date_created_kiev,
        l.user_id,
        l.operator_id,
        SUM(l.action_price) AS free_spent
    FROM redshift_analytics_db.prodmysqldatabase.log l
    WHERE l.is_male = 1
        AND l.reward_status = 2
        AND l.operator_id != 0
        AND DATE(l.date_created) > DATEADD(week, -1, CURRENT_DATE)
    GROUP BY 1, 2, 3, 4;

    -- Створення тимчасової таблиці для f_man_paid_actions
    CREATE TEMP TABLE temp_man_paid_actions AS
    SELECT
        t.date,
        t.date_kiev,
        t.operator_id,
        up.external_id AS male_external_id,
        wi.external_id AS female_external_id,
        t.action_type,
        t.action_price,
        t.operator_price
    FROM (
        SELECT
            DATE(l.date_created) AS date,
            DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', l.date_created)) AS date_kiev,
            l.operator_id,
            l.user_id,
            l.profile_id,
            l.action_type,
            SUM(l.action_price) AS action_price,
            SUM(l.operator_price) AS operator_price
        FROM redshift_analytics_db.prodmysqldatabase.log l
        WHERE l.is_male = 1
            AND l.reward_status = 1
            AND l.operator_id != 0
            AND DATE(l.date_created) > DATEADD(week, -1, CURRENT_DATE)
        GROUP BY 1, 2, 3, 4, 5, 6
    ) t
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON t.user_id = up.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_woman_information wi ON t.profile_id = wi.id;

    -- Оновлення основних таблиць
    DELETE FROM prod_analytic_db.credits_spend_orbita.users_free_given WHERE date_created > DATEADD(week, -1, CURRENT_DATE);
    INSERT INTO prod_analytic_db.credits_spend_orbita.users_free_given
    SELECT * FROM temp_users_free_given;

    DELETE FROM prod_analytic_db.credits_spend_orbita.users_free_spent WHERE date_created > DATEADD(week, -1, CURRENT_DATE);
    INSERT INTO prod_analytic_db.credits_spend_orbita.users_free_spent
    SELECT * FROM temp_users_free_spent;

    DELETE FROM prod_analytic_db.credits_spend_orbita.man_paid_actions WHERE date > DATEADD(week, -1, CURRENT_DATE);
    INSERT INTO prod_analytic_db.credits_spend_orbita.man_paid_actions
    SELECT * FROM temp_man_paid_actions;

    -- Аналіз таблиць
    ANALYZE prod_analytic_db.credits_spend_orbita.users_free_given;
    ANALYZE prod_analytic_db.credits_spend_orbita.users_free_spent;
    ANALYZE prod_analytic_db.credits_spend_orbita.man_paid_actions;

    -- Видалення тимчасових таблиць
    DROP TABLE IF EXISTS temp_users_free_given;
    DROP TABLE IF EXISTS temp_users_free_spent;
    DROP TABLE IF EXISTS temp_man_paid_actions;

END;
$$ LANGUAGE plpgsql
;


call prod_analytic_db.credits_spend_orbita.refresh()
;


--
--select *
--from prod_analytic_db.credits_spend_orbita.users_free_spent
--order by date_created desc
--limit 10 
--;
--
--select *
--from prod_analytic_db.credits_spend_orbita.users_free_given
--order by date_created desc
--limit 10 
--;
--
--select date, count(*)
--from prod_analytic_db.credits_spend_orbita.man_paid_actions
--group by date
--order by date desc
--limit 10 
;
