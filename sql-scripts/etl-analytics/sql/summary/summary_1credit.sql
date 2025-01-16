CREATE MATERIALIZED VIEW prod_analytic_db.summary.mv_summary_1credit
DISTSTYLE KEY
DISTKEY(date)
SORTKEY(
    date,         
    netw,         
    country       
)
AS

-- Базові реєстрації з фільтрацією
WITH base_registrations AS (
    SELECT 
        up.id,
        DATE(CONVERT_TIMEZONE('UTC', 'Europe/Kiev', up.created_at)) as register_date_kiev,
        up.last_ip,
        up.external_id,
        up.gender,
        up.site_id,
        la.country as activity_country,
        TO_DATE( LPAD(b_year::VARCHAR, 4, '0') || '-' || LPAD(b_month::VARCHAR, 2, '0') || '-' || LPAD(b_day::VARCHAR, 2, '0'), 'YYYY-MM-DD' ) as birth_date
    FROM redshift_analytics_db.prodmysqldatabase.user_profile up
    JOIN redshift_analytics_db.prodmysqldatabase.v3_last_activity la  ON up.external_id = la.external_id
    WHERE DATE(up.created_at) >= '2023-01-01'
        AND up.name NOT ILIKE '%test%'
        AND up.email NOT ILIKE '%+%'
        AND la.country IN (
            'United States', 'Canada', 'United Kingdom', 
            'Australia', 'New Zealand', 'Denmark', 
            'Sweden', 'Norway'
        )
        AND NOT EXISTS (
            SELECT 1 
            FROM redshift_analytics_db.prodmysqldatabase.v2_frod_list fl 
            WHERE up.external_id = fl.man_external_id
        )
),

-- Інформація про мережі та пристрої
enriched_registrations AS (
    SELECT 
        br.id,
        br.register_date_kiev,
        br.gender,
        br.site_id,
        n.netw,
        m.name as manager,
        CASE 
            WHEN d.os ILIKE '%Android%' THEN 'Android'
            WHEN d.os ILIKE '%iOS%' 		THEN 'iOS'
            WHEN d.os ILIKE '%Windows%' THEN 'Windows'
            WHEN d.os ILIKE '%Mac%' 		THEN 'MacOS'
            ELSE 'other'
        END AS os,
        CASE 
            WHEN c.id IN (13,38,154,224,225) 
            THEN c.country_name
            ELSE 'other'
        END AS country
    FROM base_registrations br
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_utm u ON br.external_id = u.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_networks n ON u.network_id = n.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_managers m ON n.parent_manager = m.id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_user_register_device d ON br.external_id = d.external_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.country c ON br.site_id = c.id
    where
            (DATEDIFF(year, br.birth_date, CURRENT_DATE) >= 45 
            AND DATEDIFF(year, br.birth_date, CURRENT_DATE) < 90) 
            OR br.birth_date IS NULL
        
),

-- Агрегація реєстрацій
registration_stats AS (
    SELECT 
        DATE(register_date_kiev) as date,
        netw,
        manager,
        site_id,
        os,
        country,
        COUNT(CASE WHEN gender = 0 THEN id END) as regs,
        COUNT(CASE WHEN gender = 1 THEN id END) as regs_women
    FROM enriched_registrations
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- Перші кредити
first_credits AS (
    SELECT 
        user_id,
        MIN(DATE(date_created_kiev)) as first_credit_date
    FROM prod_analytic_db.credits_spend_orbita.users_free_spent
    WHERE DATE(date_created_kiev) >= '2023-01-01'
    GROUP BY user_id
),

-- Статистика перших кредитів
first_credit_stats AS (
    SELECT 
        fc.first_credit_date as date,
        er.netw,
        er.manager,
        er.site_id,
        er.os,
        er.country,
        COUNT(CASE WHEN er.gender = 0 THEN fc.user_id END) as spend_1st_credit,
        COUNT(CASE WHEN er.gender = 1 THEN fc.user_id END) as spend_1st_credit_women
    FROM first_credits fc
    JOIN enriched_registrations er ON fc.user_id = er.id
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- Накопичувальні кредити
credits_accumulation AS (
    SELECT 
        user_id,
        date_created_kiev,
        SUM(free_spent) OVER (
            PARTITION BY user_id 
            ORDER BY date_created_kiev
            ROWS UNBOUNDED PRECEDING
        ) as total_credits
    FROM prod_analytic_db.credits_spend_orbita.users_free_spent
    WHERE DATE(date_created_kiev) >= '2023-01-01'
),

-- Дата досягнення 18 кредитів
credits_18_milestone AS (
    SELECT 
        user_id,
        MIN(date_created_kiev) as date_18_credits
    FROM credits_accumulation
    WHERE total_credits >= 18
    GROUP BY user_id
),

-- Статистика по 18+ кредитах
credits_18_stats AS (
    SELECT 
        c18.date_18_credits as date,
        er.netw,
        er.manager,
        er.site_id,
        er.os,
        er.country,
        COUNT(CASE WHEN er.gender = 0 THEN c18.user_id END) as spend_18_credits,
        COUNT(CASE WHEN er.gender = 1 THEN c18.user_id END) as spend_18_credits_women
    FROM credits_18_milestone c18
    JOIN enriched_registrations er ON c18.user_id = er.id
    WHERE DATE_PART('month', c18.date_18_credits) = DATE_PART('month', er.register_date_kiev)
        AND DATE_PART('year', c18.date_18_credits) = DATE_PART('year', er.register_date_kiev)
    GROUP BY 1, 2, 3, 4, 5, 6
)

-- Фінальна вибірка
SELECT 
    rs.*,
    fcs.spend_1st_credit,
    c18s.spend_18_credits,
    fcs.spend_1st_credit_women,
    c18s.spend_18_credits_women,
    s.domain
FROM registration_stats rs
LEFT JOIN first_credit_stats fcs 
    ON rs.date = fcs.date 
    AND rs.netw = fcs.netw 
    AND rs.manager = fcs.manager 
    AND rs.site_id = fcs.site_id 
    AND rs.os = fcs.os 
    AND rs.country = fcs.country
LEFT JOIN credits_18_stats c18s 
    ON rs.date = c18s.date 
    AND rs.netw = c18s.netw 
    AND rs.manager = c18s.manager 
    AND rs.site_id = c18s.site_id 
    AND rs.os = c18s.os 
    AND rs.country = c18s.country
LEFT JOIN redshift_analytics_db.prodmysqldatabase.v3_site as s 
    ON rs.site_id = s.id;