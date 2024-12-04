CREATE OR REPLACE PROCEDURE update_retention_data()
AS $$
DECLARE
    last_activity_date DATE;
    v_affected_rows INTEGER;
    v_error_text TEXT;
BEGIN
    -- Створюємо тимчасові таблиці
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_activity_data (
        sender_external_id VARCHAR(255) ENCODE zstd,
        activity_day DATE ENCODE delta,
        send_active INTEGER ENCODE runlength
    ) 
    DISTKEY(sender_external_id)
    SORTKEY(activity_day);

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_profile_data (
        external_id VARCHAR(255) ENCODE zstd,
        reg_date DATE ENCODE delta,
        site_id INTEGER ENCODE zstd,
        country VARCHAR(100) ENCODE zstd,
        age INTEGER ENCODE az64,
        utm_site_id INTEGER ENCODE zstd,
        network_id INTEGER ENCODE zstd,
        network_name VARCHAR(255) ENCODE zstd,
        netw VARCHAR(100) ENCODE zstd,
        source_name VARCHAR(255) ENCODE zstd,
        manager_name VARCHAR(255) ENCODE zstd,
        domain_name VARCHAR(255) ENCODE zstd,
        reg_device VARCHAR(100) ENCODE zstd,
        last_act_device VARCHAR(100) ENCODE zstd,
        browser VARCHAR(100) ENCODE zstd
    )
    DISTKEY(external_id)
    SORTKEY(reg_date);

    -- Починаємо транзакцію
    BEGIN
        -- Отримуємо останню дату
        SELECT MAX(activity_day) INTO last_activity_date
        FROM retention_user_activity;

        -- Видаляємо старі дані
        DELETE FROM on_demand.retention_user_activity
        WHERE activity_day = last_activity_date;

        -- Наповнюємо тимчасову таблицю активності
        INSERT INTO temp_activity_data
        SELECT sender_external_id, activity_day, send_active
        FROM (
            -- Чат активність
            SELECT DISTINCT
                sender_external_id,
                DATE(date_created) AS activity_day,
                1 AS send_active
            FROM prodmysqldatabase.v2_chat_message
            WHERE is_male = 1
            AND date_created >= last_activity_date

            UNION ALL

            -- Email активність
            SELECT DISTINCT
                up.external_id AS sender_external_id,
                DATE(vum.date_created) AS activity_day,
                1 AS send_active
            FROM prodmysqldatabase.v2_user_mail vum
            INNER JOIN prodmysqldatabase.user_profile up
                ON up.id = vum.sender_id
            WHERE vum.operator = 0
            AND vum.date_created >= last_activity_date

            UNION ALL

            -- Реєстрації
            SELECT DISTINCT
                external_id AS sender_external_id,
                DATE(created_at) AS activity_day,
                0 AS send_active
            FROM prodmysqldatabase.user_profile
            WHERE created_at >= last_activity_date
        );

        -- Наповнюємо тимчасову таблицю профілів
        INSERT INTO temp_profile_data
        SELECT DISTINCT
            up.external_id,
            DATE(up.created_at) AS reg_date,
            up.site_id,
            COALESCE(c.country_name, 'Unknown') AS country,
            up.age,
            utm.site_id AS utm_site_id,
            utm.network_id,
            COALESCE(nw.name, 'Unknown') AS network_name,
            COALESCE(nw.netw, 'Unknown') AS netw,
            COALESCE(s.name, 'Unknown') AS source_name,
            COALESCE(m.name, 'Unknown') AS manager_name,
            COALESCE(site.domain, 'Unknown') AS domain_name,
            COALESCE(d.os, 'Unknown') AS reg_device,
            COALESCE(la.os, 'Unknown') AS last_act_device,
            COALESCE(la.browser, 'Unknown') AS browser
        FROM prodmysqldatabase.user_profile up
        LEFT JOIN prodmysqldatabase.country c 
            ON up.country = c.id
        LEFT JOIN prodmysqldatabase.v2_utm utm 
            ON utm.external_id = up.external_id
        LEFT JOIN prodmysqldatabase.v3_networks nw 
            ON utm.network_id = nw.id
        LEFT JOIN prodmysqldatabase.v3_sources s 
            ON nw.parent_id = s.id
        LEFT JOIN prodmysqldatabase.v3_managers m 
            ON s.parent_id = m.id
        LEFT JOIN prodmysqldatabase.v3_site site 
            ON site.id = nw.site_id
        LEFT JOIN prodmysqldatabase.v3_user_register_device d 
            ON up.external_id = d.external_id
        LEFT JOIN prodmysqldatabase.v3_last_activity la 
            ON up.external_id = la.external_id
        WHERE up.external_id IN (
            SELECT DISTINCT sender_external_id 
            FROM temp_activity_data
        );

        -- Фінальне вставлення даних
        INSERT INTO on_demand.retention_user_activity
        SELECT
            p.external_id,
            p.reg_date,
            p.site_id,
            p.country,
            p.age,
            p.utm_site_id,
            p.network_id,
            p.network_name,
            p.netw,
            p.source_name,
            p.manager_name,
            p.domain_name,
            p.reg_device,
            p.last_act_device,
            p.browser,
            t.sender_external_id,
            t.activity_day,
            t.send_active,
            DATEDIFF(day, p.reg_date, t.activity_day) AS day
        FROM temp_activity_data t
        INNER JOIN temp_profile_data p
            ON t.sender_external_id = p.external_id;

        -- Якщо все успішно, підтверджуємо транзакцію
        COMMIT;

    EXCEPTION WHEN OTHERS THEN
        -- У випадку помилки відкочуємо зміни
        ROLLBACK;
        SELECT SQLERRM INTO v_error_text;
        RAISE EXCEPTION 'Error in transaction: %', v_error_text;
    END;

    -- Очищення тимчасових таблиць (поза транзакцією)
    DROP TABLE IF EXISTS temp_activity_data;
    DROP TABLE IF EXISTS temp_profile_data;

END;
$$ LANGUAGE plpgsql;