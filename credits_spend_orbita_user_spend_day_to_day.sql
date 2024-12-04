create MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day
DISTSTYLE KEY DISTKEY(dd) SORTKEY(dd)
as
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

purchase_stats AS (
    SELECT DATEADD(hour, 2, ph.date_added)::date AS dd,
        SUM(CASE WHEN up.gender = 0 THEN ph.price ELSE 0 END) AS price,
        SUM(CASE WHEN up.gender = 1 THEN ph.price ELSE 0 END) AS price_women,
        SUM(CASE WHEN up.gender = 0 THEN ph.amount ELSE 0 END) AS amount,
        SUM(CASE WHEN up.gender = 1 THEN ph.amount ELSE 0 END) AS amount_women,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 0 THEN ph.price ELSE 0 END) AS price_nofraud,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 1 THEN ph.price ELSE 0 END) AS price_nofraud_women,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 0 THEN ph.amount ELSE 0 END) AS amount_nofraud,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 1 THEN ph.amount ELSE 0 END) AS amount_nofraud_women
    FROM redshift_analytics_db.prodmysqldatabase.v2_purchase_history ph
    LEFT JOIN filtered_profiles up ON up.id = ph.user_id
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_frod_list fl ON ph.external_id = fl.man_external_id
    --WHERE DATEADD(hour, 2, ph.date_added)::date > DATEADD(day, -10, TRUNC(CURRENT_DATE)) 
    GROUP BY 1
),

actions_union AS (
    SELECT 
        DATEADD(hour, 2, mpa.date) AS date_kiev,
        mpa.male_external_id AS external_id,
        mpa.action_type,
        mpa.action_price
    FROM credits_spend_orbita.man_paid_actions mpa
    JOIN filtered_profiles up ON up.external_id = mpa.male_external_id
    --WHERE DATEADD(hour, 2, mpa.date)::date > DATEADD(day, -10, TRUNC(CURRENT_DATE))
    
    UNION ALL
    
    SELECT 
        DATEADD(hour, 2, o.date_created) AS date_kiev,
        o.user_external_id,
        'MAKE_ORDER' AS action_type,
        o.price
    FROM redshift_analytics_db.prodmysqldatabase.v2_order o
    JOIN filtered_profiles up ON up.external_id = o.user_external_id
    --WHERE DATEADD(hour, 2, o.date_created)::date > DATEADD(day, -10, TRUNC(CURRENT_DATE))
),

action_stats AS (
    SELECT 
        a.date_kiev::date AS dd,
        SUM(CASE WHEN up.gender = 0 AND a.action_type IN ('GET_AUDIO','GET_AUDIO_NEW','GET_VIDEO','GET_VIDEO_NEW','GET_VIDEO_SHOW','SENT_AUDIO','SENT_IMAGE','SENT_STICKER','SENT_TEXT','SENT_VIDEO','SENT_LIKE','SENT_WINK','GET_CONTACT','GET_MEETING','SENT_VIRTUAL_GIFT') 
            THEN a.action_price ELSE 0 END) AS Chat,
        SUM(CASE WHEN up.gender = 0 AND a.action_type IN ('GET_AUDIO_MAIL','GET_AUDIO_MAIL_NEW','GET_IMAGE_MAIL','GET_VIDEO_MAIL','GET_VIDEO_MAIL_NEW','READ_MAIL','SENT_AUDIO_MAIL','SENT_IMAGE_MAIL','SENT_MAIL','SENT_MAIL_FIRST','SENT_MAIL_SECOND','SENT_VIDEO_MAIL')
            THEN a.action_price ELSE 0 END) AS Letter,
        SUM(CASE WHEN up.gender = 0 AND a.action_type = 'MAKE_ORDER' 
            THEN a.action_price ELSE 0 END) AS Gift,
        -- Women stats
        SUM(CASE WHEN up.gender = 1 AND a.action_type IN ('GET_AUDIO','GET_AUDIO_NEW','GET_VIDEO','GET_VIDEO_NEW','GET_VIDEO_SHOW','SENT_AUDIO','SENT_IMAGE','SENT_STICKER','SENT_TEXT','SENT_VIDEO','SENT_LIKE','SENT_WINK','GET_CONTACT','GET_MEETING','SENT_VIRTUAL_GIFT')
            THEN a.action_price ELSE 0 END) AS Chat_women,
        SUM(CASE WHEN up.gender = 1 AND a.action_type IN ('GET_AUDIO_MAIL','GET_AUDIO_MAIL_NEW','GET_IMAGE_MAIL','GET_VIDEO_MAIL','GET_VIDEO_MAIL_NEW','READ_MAIL','SENT_AUDIO_MAIL','SENT_IMAGE_MAIL','SENT_MAIL','SENT_MAIL_FIRST','SENT_MAIL_SECOND','SENT_VIDEO_MAIL')
            THEN a.action_price ELSE 0 END) AS Letter_women,
        SUM(CASE WHEN up.gender = 1 AND a.action_type = 'MAKE_ORDER'
            THEN a.action_price ELSE 0 END) AS Gift_women,
        -- No fraud stats
        SUM(CASE WHEN up.gender = 0 AND fl.man_external_id IS NULL AND a.action_type IN ('GET_AUDIO','GET_AUDIO_NEW','GET_VIDEO','GET_VIDEO_NEW','GET_VIDEO_SHOW','SENT_AUDIO','SENT_IMAGE','SENT_STICKER','SENT_TEXT','SENT_VIDEO','SENT_LIKE','SENT_WINK','GET_CONTACT','GET_MEETING','SENT_VIRTUAL_GIFT')
            THEN a.action_price ELSE 0 END) AS Chat_nofraud,
        SUM(CASE WHEN up.gender = 0 AND fl.man_external_id IS NULL AND a.action_type IN ('GET_AUDIO_MAIL','GET_AUDIO_MAIL_NEW','GET_IMAGE_MAIL','GET_VIDEO_MAIL','GET_VIDEO_MAIL_NEW','READ_MAIL','SENT_AUDIO_MAIL','SENT_IMAGE_MAIL','SENT_MAIL','SENT_MAIL_FIRST','SENT_MAIL_SECOND','SENT_VIDEO_MAIL')
            THEN a.action_price ELSE 0 END) AS Letter_nofraud,
        SUM(CASE WHEN up.gender = 0 AND fl.man_external_id IS NULL AND a.action_type = 'MAKE_ORDER'
            THEN a.action_price ELSE 0 END) AS Gift_nofraud,
        -- No fraud women stats
        SUM(CASE WHEN up.gender = 1 AND fl.man_external_id IS NULL AND a.action_type IN ('GET_AUDIO','GET_AUDIO_NEW','GET_VIDEO','GET_VIDEO_NEW','GET_VIDEO_SHOW','SENT_AUDIO','SENT_IMAGE','SENT_STICKER','SENT_TEXT','SENT_VIDEO','SENT_LIKE','SENT_WINK','GET_CONTACT','GET_MEETING','SENT_VIRTUAL_GIFT')
            THEN a.action_price ELSE 0 END) AS Chat_nofraud_women,
        SUM(CASE WHEN up.gender = 1 AND fl.man_external_id IS NULL AND a.action_type IN ('GET_AUDIO_MAIL','GET_AUDIO_MAIL_NEW','GET_IMAGE_MAIL','GET_VIDEO_MAIL','GET_VIDEO_MAIL_NEW','READ_MAIL','SENT_AUDIO_MAIL','SENT_IMAGE_MAIL','SENT_MAIL','SENT_MAIL_FIRST','SENT_MAIL_SECOND','SENT_VIDEO_MAIL')
            THEN a.action_price ELSE 0 END) AS Letter_nofraud_women,
        SUM(CASE WHEN up.gender = 1 AND fl.man_external_id IS NULL AND a.action_type = 'MAKE_ORDER'
            THEN a.action_price ELSE 0 END) AS Gift_nofraud_women
    FROM actions_union a
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_frod_list fl ON a.external_id = fl.man_external_id
    LEFT JOIN filtered_profiles up ON up.external_id = a.external_id
    GROUP BY 1
)

SELECT 
    m1.dd::date as dd,
    m1.price,
    m1.price_nofraud,
    m2.Chat * (m1.price/NULLIF(m1.amount, 0)) AS Chat,
    m2.Chat_nofraud * (m1.price_nofraud/NULLIF(m1.amount_nofraud, 0)) AS Chat_nofraud,
    m2.Letter * (m1.price/NULLIF(m1.amount, 0)) AS Letter,
    m2.Letter_nofraud * (m1.price_nofraud/NULLIF(m1.amount_nofraud, 0)) AS Letter_nofraud,
    m2.Gift * (m1.price/NULLIF(m1.amount, 0)) AS Gift,
    m2.Gift_nofraud * (m1.price_nofraud/NULLIF(m1.amount_nofraud, 0)) AS Gift_nofraud,
    m1.price_women,
    m1.price_nofraud_women,
    m2.Chat_women * (m1.price_women/NULLIF(m1.amount_women, 0)) AS Chat_women,
    m2.Chat_nofraud_women * (m1.price_nofraud_women/NULLIF(m1.amount_nofraud_women, 0)) AS Chat_nofraud_women,
    m2.Letter_women * (m1.price_women/NULLIF(m1.amount_women, 0)) AS Letter_women,
    m2.Letter_nofraud_women * (m1.price_nofraud_women/NULLIF(m1.amount_nofraud_women, 0)) AS Letter_nofraud_women,
    m2.Gift_women * (m1.price_women/NULLIF(m1.amount_women, 0)) AS Gift_women,
    m2.Gift_nofraud_women * (m1.price_nofraud_women/NULLIF(m1.amount_nofraud_women, 0)) AS Gift_nofraud_women
FROM purchase_stats m1
LEFT JOIN action_stats m2 ON m2.dd = m1.dd


-- refresh MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day