CREATE MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day_dtd
DISTSTYLE EVEN SORTKEY(dd)
AS
WITH filtered_profiles AS (
    SELECT id, external_id, gender, email, name, payment_total, country, tester
    FROM redshift_analytics_db.prodmysqldatabase.user_profile 
    WHERE payment_total != 0
        AND name NOT LIKE '%test%'
        AND email NOT LIKE '%test%'
        AND email NOT LIKE '%delete%'
        AND email NOT LIKE '%+%'
        AND email NOT LIKE '%upiple%'
        AND email NOT LIKE '%irens%'
        AND email NOT LIKE '%galaktica%'
        AND email NOT LIKE '%.ua%'
        AND tester = 0
        AND country != 222
),

user_purchase_stats AS (
    SELECT 
        ph.external_id AS user_id,
        DATEADD(hour, 2, ph.date_added)::date AS dd,
        SUM(CASE WHEN up.gender = 0 THEN ph.price ELSE 0 END) AS price,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 0 THEN ph.price ELSE 0 END) AS price_nofraud,
        SUM(CASE WHEN up.gender = 0 THEN ph.amount ELSE 0 END) AS amount,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 0 THEN ph.amount ELSE 0 END) AS amount_nofraud,
        SUM(CASE WHEN up.gender = 1 THEN ph.price ELSE 0 END) AS price_women,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 1 THEN ph.price ELSE 0 END) AS price_nofraud_women,
        SUM(CASE WHEN up.gender = 1 THEN ph.amount ELSE 0 END) AS amount_women,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 1 THEN ph.amount ELSE 0 END) AS amount_nofraud_women
    FROM redshift_analytics_db.prodmysqldatabase.v2_purchase_history ph
    LEFT JOIN filtered_profiles up ON up.id = ph.user_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_frod_list fl ON ph.external_id = fl.man_external_id
    GROUP BY ph.external_id, dd
),

user_actions AS (
    SELECT 
        DATEADD(hour, 2, date)::date AS dd,
        male_external_id AS user_id,
        action_type,
        action_price,
        CASE 
            WHEN action_type IN ('GET_AUDIO','GET_AUDIO_NEW','GET_VIDEO','GET_VIDEO_NEW','GET_VIDEO_SHOW',
                               'SENT_AUDIO','SENT_IMAGE','SENT_STICKER','SENT_TEXT','SENT_VIDEO','SENT_LIKE',
                               'SENT_WINK','GET_CONTACT','GET_MEETING','GET_AUDIO_MAIL','GET_AUDIO_MAIL_NEW',
                               'GET_IMAGE_MAIL','GET_VIDEO_MAIL','GET_VIDEO_MAIL_NEW','READ_MAIL','SENT_AUDIO_MAIL',
                               'SENT_IMAGE_MAIL','SENT_MAIL','SENT_MAIL_FIRST','SENT_MAIL_SECOND','SENT_VIDEO_MAIL',
                               'SENT_VIRTUAL_GIFT') THEN 'Chats'
            WHEN action_type = 'MAKE_ORDER' THEN 'Gift'
            ELSE 'Other'
        END AS category
    FROM credits_spend_orbita.man_paid_actions
    
    UNION ALL
    
    SELECT 
        DATEADD(hour, 2, date_created)::date AS dd,
        user_external_id AS user_id,
        'MAKE_ORDER' AS action_type,
        price AS action_price,
        'Gift' AS category
    FROM redshift_analytics_db.prodmysqldatabase.v2_order
),

user_action_stats AS (
    SELECT 
        a.dd,
        a.user_id,
        a.action_type,
        a.category,
        SUM(CASE WHEN up.gender = 0 THEN a.action_price ELSE 0 END) AS action_price,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 0 THEN a.action_price ELSE 0 END) AS action_price_nofraud,
        SUM(CASE WHEN up.gender = 1 THEN a.action_price ELSE 0 END) AS action_price_women,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 1 THEN a.action_price ELSE 0 END) AS action_price_nofraud_women
    FROM user_actions a
    LEFT JOIN filtered_profiles up ON up.external_id = a.user_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_frod_list fl ON a.user_id = fl.man_external_id
    GROUP BY 1,2,3,4
),

normalized_user_actions AS (
    SELECT 
        ua.dd,
        ua.user_id,
        ua.category,
        LEAST(ua.action_price, ups.amount) AS action_price,
        LEAST(ua.action_price_nofraud, ups.amount_nofraud) AS action_price_nofraud,
        LEAST(ua.action_price_women, ups.amount_women) AS action_price_women,
        LEAST(ua.action_price_nofraud_women, ups.amount_nofraud_women) AS action_price_nofraud_women
    FROM user_action_stats ua
    LEFT JOIN user_purchase_stats ups ON ua.dd = ups.dd AND ua.user_id = ups.user_id
    WHERE ups.user_id IS NOT NULL
),

daily_category_stats AS (
    SELECT 
        dd,
        category,
        SUM(action_price) AS spend,
        SUM(action_price_nofraud) AS spend_nofraud,
        SUM(action_price_women) AS spend_women,
        SUM(action_price_nofraud_women) AS spend_nofraud_women
    FROM normalized_user_actions
    GROUP BY dd, category
),

daily_totals AS (
    SELECT 
        dd,
        SUM(price) AS price,
        SUM(price_nofraud) AS price_nofraud,
        SUM(amount) AS amount,
        SUM(amount_nofraud) AS amount_nofraud,
        SUM(price_women) AS price_women,
        SUM(price_nofraud_women) AS price_nofraud_women,
        SUM(amount_women) AS amount_women,
        SUM(amount_nofraud_women) AS amount_nofraud_women
    FROM user_purchase_stats
    GROUP BY dd
)

SELECT 
    c.dd,
    c.category,
    t.price,
    t.price_nofraud,
    c.spend * (t.price/NULLIF(t.amount, 0)) AS spend,
    c.spend_nofraud * (t.price_nofraud/NULLIF(t.amount_nofraud, 0)) AS spend_nofraud,
    t.price_women,
    t.price_nofraud_women,
    c.spend_women * (t.price_women/NULLIF(t.amount_women, 0)) AS spend_women,
    c.spend_nofraud_women * (t.price_nofraud_women/NULLIF(t.amount_nofraud_women, 0)) AS spend_nofraud_women
FROM daily_category_stats c
LEFT JOIN daily_totals t ON t.dd = c.dd
;

-- refresh MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day_dtd;
