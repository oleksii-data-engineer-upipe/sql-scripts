
CREATE MATERIALIZED VIEW prod_analytic_db.sender_templates.gold__mv_mailer_report
DISTSTYLE KEY 
DISTKEY(subscriber_external_id) 
SORTKEY(date)
as
WITH cte AS (

    SELECT 
        mailer_send_stat.id                             AS id,
        mailer_send_stat.created_at::date               AS date,
        DATE_PART('week', mailer_send_stat.created_at)  AS week,
        mailer_send_stat.chain_name                     AS chain_name,
        mailer_send_stat.template_name                  AS template_name,
        mailer_send_stat.subscriber_external_id         AS subscriber_external_id,
        mailer_send_stat.subscriber_external_id     		AS external_id,
        mailer_send_stat.created_at::date           		AS created_at,
        up.gender                                   		AS gender,
        s.id                                        		AS domain_id,
        up.age                                      		AS age,
        SPLIT_PART(up.email, '@', 2)               		AS email_domain,
        s.domain                                    		AS site_domain,
        CASE 
            WHEN msav.id IS NOT NULL THEN mailer_send_stat.id 
            ELSE NULL 
        END AS view,
        CASE 
            WHEN msac.id IS NOT NULL THEN mailer_send_stat.id 
            ELSE NULL 
        END AS click,
        CASE 
            WHEN msav.id IS NOT NULL THEN DATE_PART('week', msav.created_at) 
            ELSE NULL 
        END AS view_week,
        CASE 
            WHEN msac.id IS NOT NULL THEN DATE_PART('week', msac.created_at) 
            ELSE NULL 
        END AS click_week,
        CASE 
            WHEN msav.id IS NOT NULL AND msav.created_at < mailer_send_stat.created_at + INTERVAL '24 hours' 
            THEN mailer_send_stat.id 
            ELSE NULL 
        END AS view_24h,
        CASE 
            WHEN msac.id IS NOT NULL AND msac.created_at < mailer_send_stat.created_at + INTERVAL '24 hours' 
            THEN mailer_send_stat.id 
            ELSE NULL 
        END AS click_24h,
        CASE 
            WHEN msav.id IS NOT NULL AND msav.created_at < mailer_send_stat.created_at + INTERVAL '24 hours' 
            THEN DATE_PART('week', msav.created_at) 
            ELSE NULL 
        END AS view_week_24h,
        CASE 
            WHEN msac.id IS NOT NULL AND msac.created_at < mailer_send_stat.created_at + INTERVAL '24 hours' 
            THEN DATE_PART('week', msac.created_at) 
            ELSE NULL 
        END AS click_week_24h,
        FIRST_VALUE(c.country_name::varchar(64)) OVER (
            PARTITION BY mailer_send_stat.subscriber_external_id 
            ORDER BY ua.created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS country
    FROM redshift_analytics_db.prodmysqldatabase.v3_mailer_send_stat AS mailer_send_stat
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_mailer_user_action_stat AS msav 
        ON msav.mail_uuid = mailer_send_stat.uuid 
        AND msav.action = 'view'
        AND msav.created_at::date >= CURRENT_DATE - INTERVAL '3 days'
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_mailer_user_action_stat AS msac  
        ON msac.mail_uuid = mailer_send_stat.uuid 
        AND msac.action = 'click'
        AND msac.created_at::date >= CURRENT_DATE - INTERVAL '3 days'
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile AS up 
        ON up.external_id = mailer_send_stat.subscriber_external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_site AS s 
        ON s.id = up.site_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_activity ua 
        ON ua.external_id = mailer_send_stat.subscriber_external_id             
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.country c 
        ON ua.country_id = c.id
    WHERE mailer_send_stat.status = 1
        AND up.name NOT LIKE '%test%'
        AND up.email NOT LIKE '%test%' 
        AND up.email NOT LIKE '%delete%' 
        AND up.email NOT LIKE '%+%' 
        AND up.tester = 0 
        AND up.email NOT LIKE '%upiple%' 
        AND up.email NOT LIKE '%irens%'
        AND up.email NOT LIKE '%galaktica%'
        AND up.country != 222
        AND DATE(mailer_send_stat.created_at) >= CURRENT_DATE - INTERVAL '3 days'
)
SELECT 
    cte.*,
    CASE 
        WHEN ph.date_added IS NOT NULL AND cte.click IS NOT NULL 
        THEN cte.id 
        ELSE NULL 
    END AS tu,
    CASE 
        WHEN ph.date_added IS NOT NULL AND cte.click IS NOT NULL  
        AND DATE_PART('week', ph.date_added) = cte.week 
        THEN cte.id 
        ELSE NULL 
    END AS tu_week,
    CASE 
        WHEN ph.date_added IS NOT NULL AND cte.click IS NOT NULL 
        AND cte.date <= ph.date_added + INTERVAL '24 hours' 
        THEN cte.id 
        ELSE NULL 
    END AS tu_24h,
    CASE 
        WHEN ph.date_added IS NOT NULL AND cte.click IS NOT NULL 
        AND DATE_PART('week', ph.date_added) = cte.week 
        AND cte.date <= ph.date_added + INTERVAL '24 hours' 
        THEN cte.id 
        ELSE NULL 
    END AS tu_week_24h
FROM cte
LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_purchase_history AS ph 
    ON ph.external_id = cte.subscriber_external_id 
    AND ph.date_added BETWEEN cte.date AND cte.date + INTERVAL '2 days'
   ;
  
  
  
  
 ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------
 ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------
 ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------
  
  
-- call prod_analytic_db.sender_templates.refresh()
-- select * from prod_analytic_db.sender_templates.etl_logs order by 1 desc limit 18
  
  
 ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------
 ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------
 ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------ ------
  
  
  
  
CREATE OR REPLACE PROCEDURE prod_analytic_db.sender_templates.refresh()
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_step_start_time TIMESTAMP;
    v_deleted_rows INTEGER;
	v_inserted_rows INTEGER;
    v_error_text TEXT;
BEGIN
    v_start_time := GETDATE();
    v_step_start_time := v_start_time;

	-- divider
	INSERT INTO prod_analytic_db.sender_templates.etl_logs (log_time, step_name, message) VALUES (GETDATE(), 'START', '--------');

    -- 1. Оновлення мат view
    BEGIN
        REFRESH MATERIALIZED VIEW prod_analytic_db.sender_templates.gold__mv_mailer_report;     
        INSERT INTO prod_analytic_db.sender_templates.etl_logs (step_name, message, duration_seconds)
        VALUES ('Step 1. Refresh_mv', 'Success', DATEDIFF(seconds, v_step_start_time, GETDATE()) );
    EXCEPTION WHEN OTHERS THEN
        SELECT SQLERRM INTO v_error_text;
        IF v_error_text IS NOT NULL AND v_error_text != '' THEN
            INSERT INTO prod_analytic_db.sender_templates.etl_logs (step_name, message, duration_seconds)
            VALUES ('refresh_mv_error', 'Error: ' || v_error_text, DATEDIFF(seconds, v_step_start_time, GETDATE()) );
        END IF;
        RAISE EXCEPTION 'Error in refresh_mv step: %', v_error_text;
    END;

    v_step_start_time := GETDATE();

    -- 2. Оновлення даних
    BEGIN
        -- 2.1 Видалення старих даних
        DELETE FROM prod_analytic_db.sender_templates.gold__basic_v2 
        WHERE date >= CURRENT_DATE - INTERVAL '1 days';
        
        GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;
        
        INSERT INTO prod_analytic_db.sender_templates.etl_logs (step_name, message, affected_rows, duration_seconds)
        VALUES ('Step 2. Delete_old_data', 'Success', v_deleted_rows, DATEDIFF(seconds, v_step_start_time, GETDATE()) );

        v_step_start_time := GETDATE();

        -- 2.2 Вставка нових даних
        INSERT INTO prod_analytic_db.sender_templates.gold__basic_v2
        SELECT 
            id, date, week,
            chain_name, template_name,
            subscriber_external_id,
            view, view_24h, view_week, view_week_24h,
            click, click_24h, click_week, click_week_24h,
            domain_id, gender,
            tu, tu_week, tu_24h, tu_week_24h,
            age, email_domain, site_domain, country
        FROM prod_analytic_db.sender_templates.gold__mv_mailer_report
        WHERE date >= CURRENT_DATE - INTERVAL '1 days'
        GROUP BY id, date, week,
            chain_name, template_name, subscriber_external_id,
            view, view_24h, view_week, view_week_24h,
            click, click_24h, click_week, click_week_24h,
            domain_id, gender, tu, tu_week, tu_24h, tu_week_24h,
            age, email_domain, site_domain,
            country;

        GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;
        
        INSERT INTO prod_analytic_db.sender_templates.etl_logs (step_name, message, affected_rows, duration_seconds)
        VALUES ( 'Step 3. Insert_new_data', 'Success', v_inserted_rows, DATEDIFF(seconds, v_step_start_time, GETDATE()) );

    EXCEPTION WHEN OTHERS THEN
        SELECT SQLERRM INTO v_error_text;
        IF v_error_text IS NOT NULL AND v_error_text != '' THEN
            INSERT INTO prod_analytic_db.sender_templates.etl_logs ( step_name, message, duration_seconds)
            VALUES ( 'data_update_error', 'Error updating data: ' || v_error_text, DATEDIFF(seconds, v_step_start_time, GETDATE()));
        END IF;
        RAISE EXCEPTION 'Error in data update step: %', v_error_text;
    END;

    v_step_start_time := GETDATE();

    -- 3. Аналіз таблиці
    BEGIN
        ANALYZE prod_analytic_db.sender_templates.gold__basic_v2;
        
        INSERT INTO prod_analytic_db.sender_templates.etl_logs (step_name, message, duration_seconds)
        VALUES ( 'Step 4. Analyze_table', 'Success',  DATEDIFF(seconds, v_step_start_time, GETDATE()));

    EXCEPTION WHEN OTHERS THEN
        SELECT SQLERRM INTO v_error_text;
        IF v_error_text IS NOT NULL AND v_error_text != '' THEN
            INSERT INTO prod_analytic_db.sender_templates.etl_logs (step_name, message, duration_seconds)
            VALUES ( 'analyze_error', 'Error during ANALYZE: ' || v_error_text, DATEDIFF(seconds, v_step_start_time, GETDATE()));
        END IF;
        RAISE EXCEPTION 'Error in analyze step: %', v_error_text;
    END;

    v_step_start_time := GETDATE();

    -- 4. Оновлення представлення
    BEGIN
        CREATE OR REPLACE VIEW prod_analytic_db.sender_templates.gold__vw_report_3_months AS 
        SELECT 
            id, date, week,
            chain_name, template_name, subscriber_external_id,
            view, view_24h, view_week, view_week_24h,
            click, click_24h, click_week, click_week_24h,
            domain_id, gender, tu, tu_week, tu_24h, tu_week_24h,
            age, email_domain, site_domain,
            subscriber_external_id AS external_id,
            "date"::timestamp AS created_at, 
            country 
        FROM prod_analytic_db.sender_templates.gold__basic_v2 
        WHERE date > CURRENT_DATE - INTERVAL '3 months';

        INSERT INTO prod_analytic_db.sender_templates.etl_logs (step_name, message, duration_seconds )
        VALUES ( 'Step 5. Update_view', 'Success',DATEDIFF(seconds, v_step_start_time, GETDATE()));

    EXCEPTION WHEN OTHERS THEN
        SELECT SQLERRM INTO v_error_text;
        IF v_error_text IS NOT NULL AND v_error_text != '' THEN
            INSERT INTO prod_analytic_db.sender_templates.etl_logs ( step_name, message, duration_seconds)
            VALUES ( 'view_update_error', 'Error updating view: ' || v_error_text, DATEDIFF(seconds, v_step_start_time, GETDATE()));
        END IF;
        RAISE EXCEPTION 'Error in view update step: %', v_error_text;
    END;

    -- Логуємо успішне завершення
    INSERT INTO prod_analytic_db.sender_templates.etl_logs ( step_name, message, affected_rows,  duration_seconds)
    VALUES ( 'Step 6. Procedure_complete', 'Success', v_inserted_rows - v_deleted_rows, DATEDIFF(seconds, v_start_time, GETDATE()));

EXCEPTION WHEN OTHERS THEN
    SELECT SQLERRM INTO v_error_text;
    IF v_error_text IS NOT NULL AND v_error_text != '' THEN
        INSERT INTO prod_analytic_db.sender_templates.etl_logs ( step_name, message, duration_seconds)
        VALUES ( 'procedure_error', 'Fatal procedure error: ' || v_error_text, DATEDIFF(seconds, v_start_time, GETDATE()));
    END IF;
    RAISE EXCEPTION 'Fatal procedure error: %', v_error_text;
END;
$$ LANGUAGE plpgsql;  


