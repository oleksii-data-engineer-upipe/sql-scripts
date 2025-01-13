SELECT DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date`,
    n.netw `netw`,
    m.name `manager`,
    up.site_id,
    case
        when d.os LIKE "%Android%" then 'Android'
        when d.os LIKE "%iOS%" then 'iOS'
        when d.os LIKE "%Windows%" then 'Windows'
        when d.os LIKE "%Mac%" then 'MacOS'
        ELSE 'other'
    end AS `os`,
    case
        when c.id IN (13, 38, 154, 224, 225) then c.`country_name`
        ELSE 'other'
    end AS `country`,
    COUNT(if(up.gender = 0, ph.external_id, NULL)) `paid`,
    count(
        if(
            (
                up.`age` >= 45
                OR up.`age` IS NULL
            )
            AND up.gender = 0,
            ph.external_id,
            NULL
        )
    ) `paid_45`,
    count(
        if(
            MONTH(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = MONTH(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev'))
            AND year(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = year(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev'))
            AND up.gender = 0,
            ph.external_id,
            NULL
        )
    ) `paid_reg`,
    count(
        if(
            MONTH(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = MONTH(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev'))
            AND year(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = year(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev'))
            AND (
                up.`age` >= 45
                OR up.`age` IS NULL
            )
            AND up.gender = 0,
            ph.external_id,
            NULL
        )
    ) `paid_45_reg`,
    COUNT(if(up.gender = 1, ph.external_id, NULL)) `paid_women`,
    count(
        if(
            (
                up.`age` >= 45
                OR up.`age` IS NULL
            )
            AND up.gender = 1,
            ph.external_id,
            NULL
        )
    ) `paid_45_women`,
    count(
        if(
            MONTH(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = MONTH(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev'))
            AND year(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = year(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev'))
            AND up.gender = 1,
            ph.external_id,
            NULL
        )
    ) `paid_reg_women`,
    count(
        if(
            MONTH(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = MONTH(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev'))
            AND year(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = year(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev'))
            AND (
                up.`age` >= 45
                OR up.`age` IS NULL
            )
            AND up.gender = 1,
            ph.external_id,
            NULL
        )
    ) `paid_45_reg_women`
FROM v2_purchase_history ph
    left join v2_utm u ON ph.external_id = u.external_id
    LEFT JOIN v2_frod_list fl ON ph.external_id = fl.man_external_id
    LEFT JOIN v3_networks n ON u.network_id = n.id
    LEFT JOIN v3_sources s ON n.parent_id = s.id
    LEFT JOIN v3_managers m ON s.parent_id = m.id
    LEFT JOIN user_profile up ON ph.external_id = up.external_id
    LEFT JOIN v3_user_register_device d ON ph.external_id = d.external_id
    LEFT JOIN country c ON up.`country` = c.id
WHERE DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) BETWEEN -- date(DATE_FORMAT(CONVERT_TZ(NOW(), 'UTC', 'Europe/Kiev'), '%Y-%m-01'))-INTERVAL 11 MONTH 
    '2023-01-01' and DATE(CONVERT_TZ(NOW(), 'UTC', 'Europe/Kiev')) - INTERVAL 1 day
    AND ph.first_package = 1
    AND fl.man_external_id IS NULL
    AND up.id NOT IN (
        SELECT up.id
        FROM user_profile up
        where up.name LIKE '%test%'
            OR (
                up.email LIKE '%test%'
                AND up.email NOT LIKE '%delete%'
            )
            OR up.email LIKE '%+%'
            OR up.tester = 1
            OR up.country = 222
            OR up.email LIKE '%upiple%'
            OR up.email LIKE '%irens%'
            OR up.email LIKE '%galaktica%'
            OR up.email LIKE '%i.ua%'
    )
    AND ph.external_id NOT IN (
        SELECT t1.*
        FROM (
                SELECT cr.user_external_id
                FROM v3_cross_marketing cr
                WHERE cr.parent_external_id IS NOT NULL
            ) t1
            LEFT JOIN v2_purchase_history ph ON t1.`user_external_id` = ph.external_id
        WHERE ph.first_package = 1
    )
GROUP BY `date`,
    `netw`,
    `manager`,
    `site_id`,
    `os`,
    `country`