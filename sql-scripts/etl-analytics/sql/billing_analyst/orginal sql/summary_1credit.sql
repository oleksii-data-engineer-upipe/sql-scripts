SELECT p1.*,
    p2.`spend_1st_credit`,
    p3.`spend_18_credits`,
    p2.`spend_1st_credit_women`,
    p3.`spend_18_credits_women`,
    s.domain
from (
        SELECT t4.`register_date_kiev` as `date`,
            t4.`netw`,
            t4.`manager`,
            t4.`site_id`,
            t4.`os`,
            t4.`country`,
            COUNT(if(t4.`gender` = 0, t4.`id`, NULL)) `regs`,
            COUNT(if(t4.`gender` = 1, t4.`id`, NULL)) `regs_women`
        from (
                SELECT t3.*,
                    n.netw,
                    m.name `manager`,
                    pr.site_id,
                    pr.gender,
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
                        select up.id,
                            up.register_date_kiev,
                            up.last_ip,
                            MONTH(up.register_date_kiev) `month`
                        FROM profiles up
                            LEFT JOIN last_activity la ON up.external_id = la.external_id
                            LEFT JOIN fraud_list fl ON up.external_id = fl.man_external_id
                        WHERE date(up.register_date_kiev) >= '2023-01-01'
                            AND up.name not LIKE "%test%"
                            AND up.email NOT LIKE "%+%"
                            AND (
                                (
                                    ROUND((DATEDIFF(DATE(NOW()), up.date_birth)) / 365, 0) >= 45
                                    AND ROUND((DATEDIFF(DATE(NOW()), up.date_birth)) / 365, 0) < 90
                                )
                                OR up.date_birth IS NULL
                            )
                            AND la.country IN (
                                'United States',
                                'Canada',
                                'United Kingdom',
                                'Australia',
                                'New Zealand',
                                'Denmark',
                                'Sweden',
                                'Norway'
                            )
                            AND fl.man_external_id IS null
                        GROUP BY up.last_ip,
                            `month`
                    ) t3
                    LEFT JOIN utm u ON t3.`id` = u.id
                    LEFT JOIN profiles pr ON u.id = pr.id
                    LEFT JOIN networks n ON u.network_id = n.id
                    LEFT JOIN managers m ON n.parent_manager = m.id
                    LEFT JOIN user_register_device d ON u.external_id = d.external_id
                    LEFT JOIN countries c ON pr.country = c.id
            ) t4
        GROUP BY `date`,
            t4.`netw`,
            t4.`manager`,
            t4.`site_id`,
            t4.`os`,
            t4.`country`
    ) p1
    LEFT JOIN (
        SELECT t4.`date_spend_1st_credit` as `date`,
            t4.`netw`,
            t4.`manager`,
            t4.`site_id`,
            t4.`os`,
            t4.`country`,
            COUNT(if(t4.`gender` = 0, t4.`user_id`, NULL)) `spend_1st_credit`,
            COUNT(if(t4.`gender` = 1, t4.`user_id`, NULL)) `spend_1st_credit_women`
        from (
                SELECT t3.*,
                    n.netw,
                    m.name `manager`,
                    u.site_id,
                    pr.gender,
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
                        SELECT t2.*,
                            MIN(ufs.date_created_kiev) `date_spend_1st_credit`
                        from (
                                SELECT ufg.user_id,
                                    ufg.date_created_kiev `date_got_credits`
                                FROM f_users_free_given ufg
                                WHERE ufg.date_created_kiev >= '2023-01-01'
                            ) t2
                            LEFT JOIN f_users_free_spent ufs ON t2.`user_id` = ufs.user_id
                        GROUP BY t2.`user_id`,
                            t2.`date_got_credits`
                    ) t3
                    LEFT JOIN utm u ON t3.`user_id` = u.id
                    LEFT JOIN profiles pr ON u.id = pr.id
                    LEFT JOIN networks n ON u.network_id = n.id
                    LEFT JOIN managers m ON n.parent_manager = m.id
                    LEFT JOIN user_register_device d ON u.external_id = d.external_id
                    LEFT JOIN countries c ON pr.country = c.id
            ) t4
        GROUP BY `date`,
            t4.`netw`,
            t4.`manager`,
            t4.`site_id`,
            t4.`os`,
            t4.`country`
    ) p2 ON p1.`date` = p2.`date`
    AND p1.`netw` = p2.`netw`
    AND p1.`manager` = p2.`manager`
    AND p1.`site_id` = p2.`site_id`
    AND p1.`os` = p2.`os`
    AND p1.`country` = p2.`country`
    LEFT JOIN (
        SELECT f.`date`,
            f.`netw`,
            f.`manager`,
            f.`site_id`,
            f.`os`,
            f.`country`,
            COUNT(if(f.gender = 0, f.`user_id`, NULL)) `spend_18_credits`,
            COUNT(if(f.gender = 1, f.`user_id`, NULL)) `spend_18_credits_women`
        from (
                SELECT s.`date_spend_18_credits` AS `date`,
                    s.`user_id`,
                    n.netw,
                    m.name `manager`,
                    u.site_id,
                    pr.gender,
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
                        SELECT a2.*,
                            MAX(a2.`t`) `date_spend_18_credits`
                        from (
                                SELECT a1.*,
                                    if(a1.`sum_spend_nakop` >= 18, a1.`date_spend`, null) `t`
                                from (
                                        SELECT a.*,
                                            SUM(a.`sum_spend`) OVER (
                                                PARTITION BY a.`user_id`
                                                ORDER BY a.`date_spend`
                                            ) `sum_spend_nakop`
                                        from (
                                                SELECT t2.*,
                                                    ufs.date_created_kiev `date_spend`,
                                                    sum(ufs.free_spent) `sum_spend`
                                                from (
                                                        SELECT ufg.user_id,
                                                            ufg.date_created_kiev `date_got_credits`
                                                        FROM f_users_free_given ufg
                                                        WHERE ufg.date_created_kiev >= '2023-01-01'
                                                    ) t2
                                                    LEFT JOIN f_users_free_spent ufs ON t2.`user_id` = ufs.user_id
                                                WHERE ufs.date_created_kiev IS NOT null
                                                GROUP BY t2.`user_id`,
                                                    t2.`date_got_credits`,
                                                    `date_spend`
                                            ) a
                                    ) a1
                            ) a2
                        WHERE a2.`t` IS NOT null
                        GROUP BY a2.`user_id`
                    ) s
                    LEFT JOIN utm u ON s.`user_id` = u.id
                    LEFT JOIN profiles pr ON u.id = pr.id
                    LEFT JOIN networks n ON u.network_id = n.id
                    LEFT JOIN managers m ON n.parent_manager = m.id
                    LEFT JOIN user_register_device d ON u.external_id = d.external_id
                    LEFT JOIN countries c ON pr.country = c.id
                WHERE month(s.`date_spend_18_credits`) = MONTH(s.date_got_credits)
                    AND year(s.`date_spend_18_credits`) = year(s.date_got_credits)
            ) f
        GROUP BY f.`date`,
            f.`netw`,
            f.`manager`,
            f.`site_id`,
            f.`os`,
            f.`country`
    ) p3 ON p1.`date` = p3.`date`
    AND p1.`netw` = p3.`netw`
    AND p1.`manager` = p3.`manager`
    AND p1.`site_id` = p3.`site_id`
    AND p1.`os` = p3.`os`
    AND p1.`country` = p3.`country`
    LEFT JOIN sites s ON p1.`site_id` = s.id