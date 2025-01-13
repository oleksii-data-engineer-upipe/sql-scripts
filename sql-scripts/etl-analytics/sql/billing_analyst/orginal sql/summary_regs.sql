SELECT q1.`date`,
    q1.netw,
    q1.`manager`,
    q1.site_id,
    q1.`os`,
    q1.`country`,
    count(
        if (
            q1.`gender` = 0
            AND (
                (
                    q1.age >= 45
                    AND q1.age < 90
                )
                OR q1.age IS NULL
            )
            AND q1.country_la IN (
                'United States',
                'Canada',
                'United Kingdom',
                'Australia',
                'New Zealand',
                'Denmark',
                'Sweden',
                'Norway'
            ),
            q1.`external_id`,
            NULL
        )
    ) `regs_45`,
    count(
        if (
            q1.`gender` = 1
            AND (
                (
                    q1.age >= 45
                    AND q1.age < 90
                )
                OR q1.age IS NULL
            )
            AND q1.country_la IN (
                'United States',
                'Canada',
                'United Kingdom',
                'Australia',
                'New Zealand',
                'Denmark',
                'Sweden',
                'Norway'
            ),
            q1.`external_id`,
            NULL
        )
    ) `regs_45_women`,
    COUNT(if(q1.`gender` = 0, q1.`external_id`, NULL)) `regs`,
    COUNT(if(q1.`gender` = 1, q1.`external_id`, NULL)) `regs_women`
from (
        SELECT DATE(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev')) `date`,
            n.netw,
            m.name `manager`,
            up.site_id,
            up.gender,
            case
                when d.os LIKE "%Android%" then 'Android'
                when d.os LIKE "%iOS%" then 'iOS'
                when d.os LIKE "%Windows%" then 'Windows'
                when d.os LIKE "%Mac%" then 'MacOS'
                ELSE 'other'
            END AS `os`,
            case
                when c.id IN (13, 38, 154, 224, 225) then c.`country_name`
                ELSE 'other'
            end AS `country`,
            case
                when la.country IN (
                    'United States',
                    'Canada',
                    'United Kingdom',
                    'Australia',
                    'New Zealand',
                    'Denmark',
                    'Sweden',
                    'Norway'
                ) then la.country
                ELSE 'other'
            end AS `country_la`,
            t1.`external_id`,
            t1.`age`
        From (
                SELECT DISTINCT(up.last_ip) IP,
                    up.external_id,
                    up.age
                FROM user_profile up
                    LEFT JOIN v3_last_activity la ON up.external_id = la.external_id
                    LEFT JOIN v2_frod_list fl ON up.external_id = fl.man_external_id
                    LEFT JOIN v2_utm u ON u.external_id = up.external_id
                WHERE DATE(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev')) = date(now()) - interval 1 day
                    AND up.name not LIKE "%test%"
                    AND up.email NOT LIKE "%+%"
                    AND fl.man_external_id IS NULL
                    AND u.tail not LIKE "%external_id%"
                GROUP BY IP
            ) t1
            LEFT JOIN user_profile up ON t1.`external_id` = up.external_id
            LEFT JOIN v2_utm u ON up.external_id = u.external_id
            LEFT join v3_networks n ON u.network_id = n.id
            LEFT JOIN v3_sources s ON n.parent_id = s.id
            LEFT JOIN v3_managers m ON s.parent_id = m.id
            LEFT JOIN v3_user_register_device d ON up.external_id = d.external_id
            LEFT JOIN country c ON up.country = c.id
            LEFT JOIN v3_last_activity la ON up.external_id = la.external_id
    ) q1
GROUP BY q1.`date`,
    q1.netw,
    q1.`manager`,
    q1.site_id,
    q1.`os`,
    q1.`country`