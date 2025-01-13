SELECT t5.`2pc` `date`,
    t5.`netw`,
    t5.`site_id`,
    t5.`os`,
    t5.`country`,
    -- COUNT(t5.`2pc`) `pac`,
    COUNT(if(t5.`gender` = 0, t5.`2pc`, NULL)) `pac`,
    COUNT(if(t5.`gender` = 1, t5.`2pc`, NULL)) `pac_women`,
    COUNT(t5.`2_pack_day_to_day`) `pack_day_to_day`,
    COUNT(t5.`2_pack_day_to_day по дате реги`) `pack_day_to_day_reg`,
    COUNT(t5.`2_pack_day_to_day шлейф 3 дня`) `pack_3days_delay`,
    COUNT(t5.`2_pack_day_to_day шлейф 3 дня по дате реги`) `pack_3days_delay_reg`,
    COUNT(t5.`2_pack_day_to_day_women`) `pack_day_to_day_women`,
    COUNT(t5.`2_pack_day_to_day по дате реги_women`) `pack_day_to_day_reg_women`,
    COUNT(t5.`2_pack_day_to_day шлейф 3 дня_women`) `pack_3days_delay_women`,
    COUNT(
        t5.`2_pack_day_to_day шлейф 3 дня по дате реги_women`
    ) `pack_3days_delay_reg_women`
from (
        SELECT t4.*,
            n.netw,
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
            if(
                t4.`date_1st_ph` = t4.`2pc`
                AND up.gender = 0,
                t4.`external_id`,
                NULL
            ) `2_pack_day_to_day`,
            if(
                t4.`date_1st_ph` = t4.`2pc`
                AND up.gender = 1,
                t4.`external_id`,
                NULL
            ) `2_pack_day_to_day_women`,
            if(
                month(t4.`date_reg`) = month(t4.`2pc`)
                AND year(t4.`date_reg`) = year(t4.`2pc`)
                AND up.gender = 0,
                t4.`external_id`,
                NULL
            ) `2_pack_day_to_day по дате реги`,
            if(
                month(t4.`date_reg`) = month(t4.`2pc`)
                AND year(t4.`date_reg`) = year(t4.`2pc`)
                AND up.gender = 1,
                t4.`external_id`,
                NULL
            ) `2_pack_day_to_day по дате реги_women`,
            if(
                datediff(t4.`date_1st_ph`, t4.`2pc`) <= 3
                AND up.gender = 0,
                t4.`external_id`,
                NULL
            ) `2_pack_day_to_day шлейф 3 дня`,
            if(
                datediff(t4.`date_1st_ph`, t4.`2pc`) <= 3
                AND up.gender = 1,
                t4.`external_id`,
                NULL
            ) `2_pack_day_to_day шлейф 3 дня_women`,
            if(
                datediff(t4.`date_1st_ph`, t4.`2pc`) <= 3
                AND month(t4.`date_reg`) = month(t4.`2pc`)
                AND year(t4.`date_reg`) = year(t4.`2pc`)
                AND up.gender = 0,
                t4.`external_id`,
                NULL
            ) `2_pack_day_to_day шлейф 3 дня по дате реги`,
            if(
                datediff(t4.`date_1st_ph`, t4.`2pc`) <= 3
                AND month(t4.`date_reg`) = month(t4.`2pc`)
                AND year(t4.`date_reg`) = year(t4.`2pc`)
                AND up.gender = 1,
                t4.`external_id`,
                NULL
            ) `2_pack_day_to_day шлейф 3 дня по дате реги_women`
        from (
                SELECT t3.*,
                    MIN(
                        DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev'))
                    ) `3pc`
                from (
                        SELECT -- t2.*,
                            t2.`external_id`,
                            t2.`gender`,
                            t2.`date_1st_ph`,
                            t2.`date_reg`,
                            MIN(
                                DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev'))
                            ) `2pc`
                        from (
                                SELECT t1.*,
                                    DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph`
                                from (
                                        sELECT case
                                                when fl.man_external_id IS NOT NULL then fl.parent_external_id
                                                when ph.external_id IN (
                                                    SELECT cr.user_external_id
                                                    FROM v3_cross_marketing cr
                                                    WHERE cr.user_external_id IN (
                                                            select ph.external_id
                                                            FROM v2_purchase_history ph
                                                            WHERE ph.first_package = 1
                                                        )
                                                        AND cr.parent_external_id IS NOT NULL
                                                ) then cr.global_parent_external_id
                                                ELSE ph.external_id
                                            END AS `external_id`,
                                            up.gender,
                                            DATE(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev')) `date_reg`,
                                            DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date`
                                        FROM v2_purchase_history ph
                                            LEFT JOIN user_profile up ON ph.external_id = up.external_id
                                            LEFT JOIN v2_frod_list fl ON ph.external_id = fl.man_external_id
                                            LEFT JOIN v3_cross_marketing cr ON ph.external_id = cr.user_external_id
                                        WHERE ph.first_package = 1
                                            and up.id NOT IN (
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
                                            )
                                            AND DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) BETWEEN '2023-01-01' AND DATE(CONVERT_TZ(NOW(), 'UTC', 'Europe/Kiev')) - INTERVAL 1 DAY
                                            AND fl.man_external_id IS NULL
                                        GROUP BY `external_id`
                                    ) t1
                                    LEFT JOIN v2_purchase_history ph ON t1.`external_id` = ph.external_id
                                WHERE ph.first_package = 1
                                    AND month(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = MONTH(t1.`date`)
                                    AND year(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) = year(t1.`date`)
                            ) t2
                            LEFT JOIN v2_purchase_history ph ON t2.`external_id` = ph.external_id
                            AND ph.first_package = 0
                        GROUP BY t2.`external_id`
                    ) t3
                    LEFT JOIN v2_purchase_history ph ON t3.`external_id` = ph.external_id
                    AND ph.first_package = 0
                    AND date(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) > t3.`2pc`
                GROUP BY t3.`external_id`
            ) t4
            LEFT JOIN user_profile up ON t4.`external_id` = up.external_id
            LEFT JOIN v2_utm u ON up.external_id = u.external_id
            LEFT JOIN v3_networks n ON u.network_id = n.id
            LEFT JOIN v3_user_register_device d ON up.external_id = d.external_id
            LEFT JOIN country c ON up.country = c.id
    ) t5
WHERE t5.`2pc` IS NOT NULL
    AND MONTH(t5.`2pc`) = MONTH(t5.`date_1st_ph`)
    AND year(t5.`2pc`) = YEAR(t5.`date_1st_ph`)
GROUP BY `date`,
    t5.`netw`,
    t5.`site_id`,
    t5.`os`,
    t5.`country`