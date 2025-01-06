--CREATE TABLE prod_analytic_db.credits_spend_orbita.refresh_logs (
--   log_id INT IDENTITY(1,1),
--   log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--   view_name VARCHAR(255),
--   status VARCHAR(50),
--   message VARCHAR(256),
--   duration_seconds NUMERIC(10,2)
--);


CREATE OR REPLACE PROCEDURE prod_analytic_db.credits_spend_orbita.refresh()
AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
BEGIN
  start_time := CURRENT_TIMESTAMP;

  -- Free given
  BEGIN
      CREATE TEMP TABLE temp_users_free_given AS 
      SELECT
          DATE(l.date_created) 	AS date_created,
          DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', l.date_created)) AS date_created_kiev,
          l.user_id,
          l.action_price 		AS free_given
      FROM redshift_analytics_db.prodmysqldatabase.log l
      WHERE 1=1
		AND l.action_type 		 = 	'REGISTRATION_BONUS'
		AND DATE(l.date_created) > 	DATEADD(week, -1, CURRENT_DATE)
	  	;

      DELETE FROM prod_analytic_db.credits_spend_orbita.users_free_given WHERE date_created > DATEADD(week, -1, CURRENT_DATE);
      INSERT INTO prod_analytic_db.credits_spend_orbita.users_free_given
      SELECT * FROM temp_users_free_given;
      
      DROP TABLE IF EXISTS temp_users_free_given;
   
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message, duration_seconds)
      VALUES ('users_free_given', 'Success', 'Updated Successfully',
              EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)));
  EXCEPTION WHEN OTHERS THEN
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message)
      VALUES ('users_free_given', 'Error', SQLERRM);
      RAISE;
  END;

  -- Free spent
  BEGIN
      CREATE TEMP TABLE temp_users_free_spent AS
      SELECT
          DATE(l.date_created) 	AS date_created,
          DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', l.date_created)) AS date_created_kiev,
          l.user_id,
          l.operator_id,
          SUM(l.action_price) 	AS free_spent
      FROM redshift_analytics_db.prodmysqldatabase.log l
      WHERE 1=1
		AND l.is_male 		 = 1
      	AND l.reward_status 	 = 2
      	AND l.operator_id 	!= 0
      	AND DATE(l.date_created) > DATEADD(week, -1, CURRENT_DATE)
      GROUP BY 1, 2, 3, 4;

      DELETE FROM prod_analytic_db.credits_spend_orbita.users_free_spent WHERE date_created > DATEADD(week, -1, CURRENT_DATE);
      INSERT INTO prod_analytic_db.credits_spend_orbita.users_free_spent
      SELECT * FROM temp_users_free_spent;

      DROP TABLE IF EXISTS temp_users_free_spent;

      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message, duration_seconds)
      VALUES ('users_free_spent', 'Success', 'Updated Successfully',
              EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)));
  EXCEPTION WHEN OTHERS THEN
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message)
      VALUES ('users_free_spent', 'Error', SQLERRM);
      RAISE;
  END;

  -- Paid actions
  BEGIN
      CREATE TEMP TABLE temp_man_paid_actions AS
      WITH paid_actions AS (
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
          WHERE 1=1
			AND l.is_male = 1
          	AND l.reward_status = 1
          	AND l.operator_id != 0
          	AND DATE(l.date_created) > DATEADD(week, -1, CURRENT_DATE)
          GROUP BY 1, 2, 3, 4, 5, 6
      )
      SELECT
          pa.date,
          pa.date_kiev,
          pa.operator_id,
          up.external_id AS male_external_id,
          wi.external_id AS female_external_id,
          pa.action_type,
          pa.action_price,
          pa.operator_price
      FROM paid_actions pa
      LEFT JOIN redshift_analytics_db.prodmysqldatabase.user_profile up ON pa.user_id = up.id
      LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_woman_information wi ON pa.profile_id = wi.id;

      DELETE FROM prod_analytic_db.credits_spend_orbita.man_paid_actions WHERE "date" > DATEADD(week, -1, CURRENT_DATE);
      INSERT INTO prod_analytic_db.credits_spend_orbita.man_paid_actions
      SELECT * FROM temp_man_paid_actions;

      DROP TABLE IF EXISTS temp_man_paid_actions;

      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message, duration_seconds)
      VALUES ('man_paid_actions', 'Success', 'Updated Successfully',
              EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)));
  EXCEPTION WHEN OTHERS THEN
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message)
      VALUES ('man_paid_actions', 'Error', SQLERRM);
      RAISE;
  END;

  -- Аналіз та оптимізація таблиць
    ANALYZE prod_analytic_db.credits_spend_orbita.users_free_given;
    ANALYZE prod_analytic_db.credits_spend_orbita.users_free_spent;
    ANALYZE prod_analytic_db.credits_spend_orbita.man_paid_actions;

  -- Оновлення матеріалізованих представлень
  BEGIN
      REFRESH MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day;
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message, duration_seconds)
      VALUES ('mv_user_spend_day_to_day', 'Success', 'Refreshed Successfully',
              EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)));
  EXCEPTION WHEN OTHERS THEN
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message)
      VALUES ('mv_user_spend_day_to_day', 'Error', SQLERRM);
      RAISE;
  END;

  BEGIN
      REFRESH MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day_v2;
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message, duration_seconds)
      VALUES ('mv_user_spend_day_to_day_v2', 'Success', 'Refreshed Successfully',
              EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)));
  EXCEPTION WHEN OTHERS THEN
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message)
      VALUES ('mv_user_spend_day_to_day_v2', 'Error', SQLERRM);
      RAISE;
  END;

  BEGIN
      REFRESH MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day_dtd;
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message, duration_seconds)
      VALUES ('mv_user_spend_day_to_day_dtd', 'Success', 'Refreshed Successfully',
              EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)));
  EXCEPTION WHEN OTHERS THEN
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message)
      VALUES ('mv_user_spend_day_to_day_dtd', 'Error', SQLERRM);
      RAISE;
  END;

  BEGIN
      REFRESH MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_average_check;
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message, duration_seconds)
      VALUES ('mv_average_check', 'Success', 'Refreshed Successfully',
              EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)));
  EXCEPTION WHEN OTHERS THEN
      INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
      (view_name, status, message)
      VALUES ('mv_average_check', 'Error', SQLERRM);
      RAISE;
  END;

  -- Фінальний лог успішного виконання
  end_time := CURRENT_TIMESTAMP;
  INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
  (view_name, status, message, duration_seconds)
  VALUES ('FULL_REFRESH', 'Success', 'Procedure completed successfully',
          EXTRACT(EPOCH FROM (end_time - start_time)));

EXCEPTION WHEN OTHERS THEN
  INSERT INTO prod_analytic_db.credits_spend_orbita.refresh_logs 
  (view_name, status, message, duration_seconds)
  VALUES ('FULL_REFRESH', 'Error', SQLERRM,
          EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)));
  RAISE;
END;
$$ LANGUAGE plpgsql;



-- Виклик процедури
--CALL prod_analytic_db.credits_spend_orbita.refresh();

--select * 
--from prod_analytic_db.credits_spend_orbita.refresh_logs
--order by log_id desc limit 15
--;

--
--select "date", count(*)
--from prod_analytic_db.credits_spend_orbita.man_paid_actions
--group by "date"
--order by 1 desc limit 5