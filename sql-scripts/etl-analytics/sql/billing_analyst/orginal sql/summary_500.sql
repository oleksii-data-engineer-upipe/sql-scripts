SELECT t11.`date_to_500` AS `date`,
    t11.`netw`,
    t11.`site_id`,
    t11.`os`,
    t11.`country`,
    COUNT(t11.`500_day_to_day`) `500_day_to_day`,
    COUNT(t11.`500_day_to_day_reg`) `500_day_to_day_reg`,
    COUNT(t11.`500_nakop`) `500_nakop`,
    COUNT(t11.`500_nakop_reg`) `500_nakop_reg`,
    COUNT(t11.`500_delay_3_day`) `500_delay_3_day`,
    COUNT(t11.`500_delay_3_day_reg`) `500_delay_3_day_reg`,
    COUNT(t11.`500_day_to_day_women`) `500_day_to_day_women`,
    COUNT(t11.`500_day_to_day_reg_women`) `500_day_to_day_reg_women`,
    COUNT(t11.`500_nakop_women`) `500_nakop_women`,
    COUNT(t11.`500_nakop_reg_women`) `500_nakop_reg_women`,
    COUNT(t11.`500_delay_3_day_women`) `500_delay_3_day_women`,
    COUNT(t11.`500_delay_3_day_reg_women`) `500_delay_3_day_reg_women`
from (
        SELECT t10.*,
            if(
                t10.`date_to_500` = t10.`date_1st_ph`
                AND t10.`gender` = 0,
                t10.`external_id`,
                NULL
            ) `500_day_to_day`,
            if(
                t10.`date_to_500` = t10.`date_reg`
                AND t10.`gender` = 0,
                t10.`external_id`,
                NULL
            ) `500_day_to_day_reg`,
            if(t10.`gender` = 0, t10.`external_id`, NULL) `500_nakop`,
            if(
                month(t10.`date_to_500`) = month(t10.`date_reg`)
                AND t10.`gender` = 0,
                t10.`external_id`,
                NULL
            ) `500_nakop_reg`,
            if(
                t10.`date_to_500` <= t10.`date_1st_ph` + INTERVAL 3 DAY
                AND t10.`gender` = 0,
                t10.`external_id`,
                NULL
            ) `500_delay_3_day`,
            if(
                t10.`date_to_500` <= t10.`date_reg` + INTERVAL 3 DAY
                AND t10.`gender` = 0,
                t10.`external_id`,
                NULL
            ) `500_delay_3_day_reg`,
            if(
                t10.`date_to_500` = t10.`date_1st_ph`
                AND t10.`gender` = 1,
                t10.`external_id`,
                NULL
            ) `500_day_to_day_women`,
            if(
                t10.`date_to_500` = t10.`date_reg`
                AND t10.`gender` = 1,
                t10.`external_id`,
                NULL
            ) `500_day_to_day_reg_women`,
            if(t10.`gender` = 1, t10.`external_id`, NULL) `500_nakop_women`,
            if(
                month(t10.`date_to_500`) = month(t10.`date_reg`)
                AND t10.`gender` = 1,
                t10.`external_id`,
                NULL
            ) `500_nakop_reg_women`,
            if(
                t10.`date_to_500` <= t10.`date_1st_ph` + INTERVAL 3 DAY
                AND t10.`gender` = 1,
                t10.`external_id`,
                NULL
            ) `500_delay_3_day_women`,
            if(
                t10.`date_to_500` <= t10.`date_reg` + INTERVAL 3 DAY
                AND t10.`gender` = 1,
                t10.`external_id`,
                NULL
            ) `500_delay_3_day_reg_women`
        from (
                SELECT t9.*,
                    DATE(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev')) `date_reg`,
                    n.netw,
                    up.site_id,
                    up.gender,
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
                    end AS `country`
                from (
                        SELECT t8.`external_id`,
                            t8.`date_1st_ph`,
                            min(t8.`date_to_500`) `date_to_500`
                        from (
                                SELECT t7.*,
                                    if(t7.`spend_nakop` >= 500, t7.`date_ph`, NULL) `date_to_500`
                                from (
                                        SELECT t6.*,
                                            SUM(t6.`spend`) OVER (
                                                PARTITION BY t6.`external_id`
                                                ORDER by t6.`date_ph`
                                            ) `spend_nakop`
                                        from (
                                                SELECT case
                                                        when t5.`parent_external_id` IS NOT NULL then t5.`parent_external_id`
                                                        when t5.`global_external_id` IS NOT NULL then t5.`global_external_id`
                                                        ELSE t5.`external_id`
                                                    END AS `external_id`,
                                                    case
                                                        when t5.`parent_external_id` IS NOT NULL then t5.`date_1st_ph_fraud`
                                                        when t5.`global_external_id` IS NOT NULL then t5.`date_1st_ph_global`
                                                        ELSE t5.`date_1st_ph`
                                                    END AS `date_1st_ph`,
                                                    t5.`date_ph`,
                                                    t5.`spend`
                                                from (
                                                        SELECT t4.*,
                                                            DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_ph`,
                                                            SUM(ph.price) `spend`
                                                        from (
                                                                SELECT t3.*
                                                                FROM (
                                                                        SELECT t2.*,
                                                                            DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph_global`
                                                                        from (
                                                                                SELECT t1.*,
                                                                                    DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph_fraud`,
                                                                                    if(
                                                                                        t1.`external_id` IN (
                                                                                            SELECT t1.*
                                                                                            from (
                                                                                                    SELECT cr.user_external_id
                                                                                                    FROM v3_cross_marketing cr
                                                                                                    WHERE cr.parent_external_id IS NOT NULL
                                                                                                ) t1
                                                                                            WHERE t1.`user_external_id` IN (
                                                                                                    SELECT ph.external_id
                                                                                                    FROM v2_purchase_history ph
                                                                                                    WHERE ph.first_package = 1
                                                                                                )
                                                                                        ),
                                                                                        cr.global_parent_external_id,
                                                                                        NULL
                                                                                    ) `global_external_id`
                                                                                from (
                                                                                        SELECT ph.external_id,
                                                                                            DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph`,
                                                                                            fl.parent_external_id
                                                                                        FROM v2_purchase_history ph
                                                                                            LEFT JOIN user_profile up ON ph.external_id = up.external_id
                                                                                            LEFT JOIN v2_frod_list fl ON ph.external_id = fl.man_external_id
                                                                                        WHERE DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) BETWEEN -- date(DATE_FORMAT(CONVERT_TZ(NOW(), 'UTC', 'Europe/Kiev'), '%Y-%m-01'))-INTERVAL 11 MONTH 
                                                                                            '2023-01-01' and DATE(CONVERT_TZ(NOW(), 'UTC', 'Europe/Kiev')) - INTERVAL 1 day
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
                                                                                            AND ph.first_package = 1
                                                                                    ) t1
                                                                                    LEFT JOIN v2_purchase_history ph ON t1.`parent_external_id` = ph.external_id
                                                                                    AND ph.first_package = 1
                                                                                    LEFT JOIN v3_cross_marketing cr ON ph.external_id = cr.user_external_id
                                                                                GROUP BY t1.`external_id`
                                                                            ) t2
                                                                            LEFT JOIN v2_purchase_history ph ON t2.`global_external_id` = ph.external_id
                                                                            AND ph.first_package = 1
                                                                        GROUP BY t2.`external_id`
                                                                    ) t3
                                                                WHERE (
                                                                        t3.`parent_external_id` IS NULL
                                                                        AND t3.`global_external_id` is NULL
                                                                    )
                                                                    OR (
                                                                        (
                                                                            MONTH(t3.`date_1st_ph_fraud`) = MONTH(`date_1st_ph`)
                                                                            AND year(t3.`date_1st_ph_fraud`) = year(`date_1st_ph`)
                                                                        )
                                                                        OR (
                                                                            MONTH(t3.`date_1st_ph_global`) = MONTH(`date_1st_ph`)
                                                                            AND year(t3.`date_1st_ph_global`) = year(`date_1st_ph`)
                                                                        )
                                                                    )
                                                            ) t4
                                                            LEFT JOIN v2_purchase_history ph ON t4.`external_id` = ph.external_id
                                                            AND MONTH(
                                                                DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev'))
                                                            ) = MONTH(t4.`date_1st_ph`)
                                                            AND year(
                                                                DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev'))
                                                            ) = year(t4.`date_1st_ph`)
                                                        GROUP BY t4.`external_id`,
                                                            `date_ph`
                                                    ) t5
                                            ) t6
                                        GROUP BY t6.`external_id`,
                                            t6.`date_1st_ph`,
                                            t6.`date_ph`
                                    ) t7
                            ) t8
                        WHERE t8.`date_to_500` IS NOT NULL
                        GROUP BY t8.`external_id`
                    ) t9
                    LEFT JOIN user_profile up ON t9.`external_id` = up.external_id
                    LEFT JOIN v2_utm u ON up.external_id = u.external_id
                    LEFT JOIN v3_networks n ON u.network_id = n.id
                    LEFT JOIN v3_user_register_device d ON t9.external_id = d.external_id
                    LEFT JOIN country c ON up.`country` = c.id
            ) t10
    ) t11
GROUP BY `date`,
    t11.`netw`,
    t11.`site_id`,
    t11.`os`,
    t11.`country`