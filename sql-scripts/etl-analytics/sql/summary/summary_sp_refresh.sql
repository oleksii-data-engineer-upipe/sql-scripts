--CREATE TABLE IF NOT EXISTS prod_analytic_db.summary.refresh_logs (
--    log_id BIGINT IDENTITY(1,1),
--    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--    view_name VARCHAR(255),
--    status VARCHAR(50),
--    message TEXT,
--    updated_rows INTEGER,
--    duration_seconds DECIMAL(10,2)
--) DISTSTYLE AUTO
--  SORTKEY(log_time);


CREATE OR REPLACE PROCEDURE prod_analytic_db.summary.refresh()
AS $$
DECLARE
    rec RECORD;
    v_start_time TIMESTAMP;
    v_step_start_time TIMESTAMP;
    v_error_message TEXT;
    refresh_query TEXT;
BEGIN
    v_start_time := GETDATE();
	INSERT INTO prod_analytic_db.summary.refresh_logs ( view_name, status, message, duration_seconds) 
	VALUES ('----------', '----------', 'START', 0);
    FOR rec IN 
        SELECT name 
        FROM stv_mv_info 
        WHERE schema = 'summary' AND name LIKE 'mv_%'
    LOOP
        v_step_start_time := GETDATE();
        BEGIN
			-- v_step_start_time := GETDATE();
            refresh_query := 'REFRESH MATERIALIZED VIEW summary.' || rec.name;
            EXECUTE refresh_query;
			
            INSERT INTO prod_analytic_db.summary.refresh_logs ( view_name, status, message, duration_seconds)
            VALUES ( rec.name, 'Success', 'Materialized view refreshed successfully.', DATEDIFF(seconds, v_step_start_time, GETDATE()));

        EXCEPTION WHEN OTHERS THEN
            SELECT SQLERRM INTO v_error_message;
			--v_step_start_time := GETDATE();
            INSERT INTO prod_analytic_db.summary.refresh_logs ( view_name, status, message, duration_seconds)
            VALUES (rec.name, 'Error', v_error_message, DATEDIFF(seconds, v_step_start_time, GETDATE()));

            RAISE NOTICE 'Error refreshing materialized view %: %', rec.name, v_error_message;
            CONTINUE;
        END;
    END LOOP;
prod_analytic_db.summary.mv_summary_500
EXCEPTION WHEN OTHERS THEN
    SELECT SQLERRM INTO v_error_message;
    INSERT INTO prod_analytic_db.summary.refresh_logs (view_name, status, message, duration_seconds)
    VALUES ( 'Procedure', 'Fatal Error', v_error_message, DATEDIFF(seconds, v_start_time, GETDATE()));

    RAISE EXCEPTION 'Fatal Error in procedure: %', v_error_message;
END;
$$ LANGUAGE plpgsql;


-- call prod_analytic_db.summary.refresh()
-- select * from prod_analytic_db.summary.refresh_logs order by 1 desc limit 15
