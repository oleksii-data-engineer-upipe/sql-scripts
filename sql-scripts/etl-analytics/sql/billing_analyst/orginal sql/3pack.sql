SELECT t6.`date_ph` `date`,
    t6.`netw`,
    t6.`site_id`,
    t6.`os`,
    t6.`country`,
    count(
        if(
            t6.`date_ph` = t6.`date_1st_ph`
            AND t6.`gender` = 0,
            t6.`external_id`,
            NULL
        )
    ) `3pack_day_to_day`,
    count(
        if(
            DATEDIFF(t6.`date_ph`, t6.`date_1st_ph`) <= 3
            AND t6.`gender` = 0,
            t6.`external_id`,
            NULL
        )
    ) `3pack_3day`,
    COUNT(if(t6.`gender` = 0, t6.`external_id`, NULL)) `3pack_nakop`,
    count(
        if(
            t6.`date_ph` = t6.`date_1st_ph`
            AND month(t6.`date_1st_ph`) = month(t6.`date_reg`)
            AND year(t6.`date_1st_ph`) = year(t6.`date_reg`)
            AND t6.`gender` = 0,
            t6.`external_id`,
            NULL
        )
    ) `3pack_day_to_day_reg`,
    count(
        if(
            DATEDIFF(t6.`date_ph`, t6.`date_1st_ph`) <= 3
            AND month(t6.`date_ph`) = month(t6.`date_reg`)
            AND year(t6.`date_ph`) = year(t6.`date_reg`)
            AND t6.`gender` = 0,
            t6.`external_id`,
            NULL
        )
    ) `3pack_3day_reg`,
    count(
        if(
            t6.`date_ph` = t6.`date_1st_ph`
            AND t6.`gender` = 1,
            t6.`external_id`,
            NULL
        )
    ) `3pack_day_to_day_women`,
    count(
        if(
            DATEDIFF(t6.`date_ph`, t6.`date_1st_ph`) <= 3
            AND t6.`gender` = 1,
            t6.`external_id`,
            NULL
        )
    ) `3pack_3day_women`,
    COUNT(if(t6.`gender` = 1, t6.`external_id`, NULL)) `3pack_nakop_women`,
    count(
        if(
            t6.`date_ph` = t6.`date_1st_ph`
            AND month(t6.`date_1st_ph`) = month(t6.`date_reg`)
            AND year(t6.`date_1st_ph`) = year(t6.`date_reg`)
            AND t6.`gender` = 1,
            t6.`external_id`,
            NULL
        )
    ) `3pack_day_to_day_reg_women`,
    count(
        if(
            DATEDIFF(t6.`date_ph`, t6.`date_1st_ph`) <= 3
            AND month(t6.`date_ph`) = month(t6.`date_reg`)
            AND year(t6.`date_ph`) = year(t6.`date_reg`)
            AND t6.`gender` = 1,
            t6.`external_id`,
            NULL
        )
    ) `3pack_3day_reg_women`
from (
        SELECT t5.*
        from (
                SELECT t4.*,
                    ROW_NUMBER() OVER (PARTITION BY t4.`external_id`) `n`
                from (
                        SELECT -- if(t3.`date_1st_ph_fraud` IS NULL,t3.`external_id`,t3.`fraud`) `external_id`,
                            case
                                when t3.`cr` is not NULL then t3.`cr`
                                when t3.`date_1st_ph_fraud` IS NULL then t3.`external_id`
                                ELSE t3.`fraud`
                            END AS `external_id`,
                            t3.`gender`,
                            t3.`date_1st_ph`,
                            t3.`date_reg`,
                            t3.`netw`,
                            t3.`site_id`,
                            t3.`os`,
                            t3.`country`,
                            DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_ph`,
                            ph.id
                        from (
                                SELECT tt3.*
                                from (
                                        SELECT t3.*,
                                            DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph_cr`
                                        from (
                                                SELECT t2.*,
                                                    if(
                                                        cr.user_external_id IN (
                                                            SELECT *
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
                                                    ) `cr`
                                                from (
                                                        SELECT t1.*,
                                                            DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph_fraud`
                                                        from (
                                                                SELECT ph.external_id,
                                                                    DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph`,
                                                                    DATE(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev')) `date_reg`,
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
                                                                        fl.man_external_id IS NOT NULL,
                                                                        fl.parent_external_id,
                                                                        NULL
                                                                    ) `fraud`,
                                                                    up.gender
                                                                FROM v2_purchase_history ph
                                                                    LEFT JOIN user_profile up ON ph.external_id = up.external_id
                                                                    LEFT JOIN v2_utm u ON ph.external_id = u.external_id
                                                                    LEFT JOIN v3_networks n ON u.network_id = n.id
                                                                    LEFT JOIN v2_frod_list fl ON ph.external_id = fl.man_external_id
                                                                    LEFT JOIN v3_user_register_device d ON ph.external_id = d.external_id
                                                                    LEFT JOIN country c ON up.`country` = c.id
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
                                                            ) t1
                                                            LEFT JOIN v2_purchase_history ph ON t1.`fraud` = ph.external_id
                                                            AND ph.first_package = 1
                                                        GROUP BY t1.`external_id`
                                                    ) t2
                                                    LEFT JOIN v3_cross_marketing cr ON t2.`external_id` = cr.user_external_id
                                            ) t3
                                            LEFT JOIN v2_purchase_history ph ON t3.`cr` = ph.external_id
                                            AND ph.first_package = 1
                                        GROUP BY t3.`external_id`
                                    ) tt3
                                WHERE tt3.`fraud` IS NULL
                                    OR (
                                        MONTH(tt3.`date_1st_ph`) = MONTH(tt3.`date_1st_ph_fraud`)
                                        AND YEAR(tt3.`date_1st_ph`) = YEAR(tt3.`date_1st_ph_fraud`)
                                    )
                                    OR (
                                        MONTH(tt3.`date_1st_ph`) = MONTH(tt3.`date_1st_ph_cr`)
                                        AND YEAR(tt3.`date_1st_ph`) = YEAR(tt3.`date_1st_ph_cr`)
                                    )
                            ) t3
                            LEFT JOIN v2_purchase_history ph ON t3.`external_id` = ph.external_id
                    ) t4
            ) t5
        HAVING t5.`n` = 3
            AND month(t5.`date_1st_ph`) = MONTH(t5.`date_ph`)
            AND year(t5.`date_1st_ph`) = year(t5.`date_ph`)
    ) t6
GROUP BY `date`,
    t6.`netw`,
    t6.`site_id`,
    t6.`os`,
    t6.`country`