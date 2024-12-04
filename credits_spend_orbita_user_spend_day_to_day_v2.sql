CREATE MATERIALIZED VIEW prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day_v2
DISTSTYLE KEY DISTKEY(dd) SORTKEY(dd)
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
    GROUP BY 1
),

actions_union AS (
    SELECT 
        DATEADD(hour, 2, mpa.date)::date AS dd,
        mpa.male_external_id AS external_id,
        mpa.action_type,
        mpa.action_price,
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
    FROM credits_spend_orbita.man_paid_actions mpa
    JOIN filtered_profiles up ON up.external_id = mpa.male_external_id
    
    UNION ALL
    
    SELECT 
        DATEADD(hour, 2, o.date_created)::date AS dd,
        o.user_external_id AS external_id,
        'MAKE_ORDER' AS action_type,
        o.price AS action_price,
        'Gift' AS category
    FROM redshift_analytics_db.prodmysqldatabase.v2_order o
    JOIN filtered_profiles up ON up.external_id = o.user_external_id
),

action_stats AS (
    SELECT 
        a.dd,
        a.category,
        SUM(CASE WHEN up.gender = 0 THEN a.action_price ELSE 0 END) AS spend,
        SUM(CASE WHEN up.gender = 1 THEN a.action_price ELSE 0 END) AS spend_women,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 0 THEN a.action_price ELSE 0 END) AS spend_nofraud,
        SUM(CASE WHEN fl.man_external_id IS NULL AND up.gender = 1 THEN a.action_price ELSE 0 END) AS spend_nofraud_women
    FROM actions_union a
    LEFT JOIN redshift_analytics_db.prodmysqldatabase.v2_frod_list fl ON a.external_id = fl.man_external_id
    LEFT JOIN filtered_profiles up ON up.external_id = a.external_id
    GROUP BY 1, 2
)

SELECT 
    a.dd,
    a.category,
    p.price,
    p.price_nofraud,
    a.spend * (p.price/NULLIF(p.amount, 0)) AS spend,
    a.spend_nofraud * (p.price_nofraud/NULLIF(p.amount_nofraud, 0)) AS spend_nofraud,
    p.price_women,
    p.price_nofraud_women,
    a.spend_women * (p.price_women/NULLIF(p.amount_women, 0)) AS spend_women,
    a.spend_nofraud_women * (p.price_nofraud_women/NULLIF(p.amount_nofraud_women, 0)) AS spend_nofraud_women
FROM action_stats a
LEFT JOIN purchase_stats p ON p.dd = a.dd
;

-- select * from prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day_v2 order by 1 desc;
-- refresh MATERIALIZED view prod_analytic_db.credits_spend_orbita.mv_user_spend_day_to_day_v2;