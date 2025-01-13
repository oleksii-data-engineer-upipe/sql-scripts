SELECT a1.`date_1st_ph` AS `date`,
    a1.`os`,
    a1.`netw`,
    a1.`manager`,
    a1.`domain`,
    a1.`new_users`,
    a1.`new_users_women`,
    a1.`day`,
    a2.`spend`,
    a2.`spend_women`,
    ROW_NUMBER() OVER (
        PARTITION BY a1.`date_1st_ph`,
        a1.`os`,
        a1.`netw`,
        a1.`manager`,
        a1.`domain`
    ) `n`,
    SUM(a2.`spend`) OVER (
        PARTITION BY a1.`date_1st_ph`,
        a1.`os`,
        a1.`netw`,
        a1.`manager`,
        a1.`domain`
        order BY a1.`date_1st_ph`,
            a1.`os`,
            a1.`netw`,
            a1.`manager`,
            a1.`domain`,
            a1.`day`
    ) `spend nakop`,
    SUM(a2.`spend_women`) OVER (
        PARTITION BY a1.`date_1st_ph`,
        a1.`os`,
        a1.`netw`,
        a1.`manager`,
        a1.`domain`
        order BY a1.`date_1st_ph`,
            a1.`os`,
            a1.`netw`,
            a1.`manager`,
            a1.`domain`,
            a1.`day`
    ) `spend nakop women`
from (
        SELECT t1.*,
            n.id -1 `day`
        from (
                SELECT DATE(ph.date_added) `date_1st_ph`,
                    d.os,
                    n.netw,
                    m.name `manager`,
                    s.domain,
                    count(if(up.gender = 0, ph.external_id, NULL)) `new_users`,
                    count(if(up.gender = 1, ph.external_id, NULL)) `new_users_women`
                FROM v2_purchase_history ph
                    LEFT JOIN v2_frod_list fl ON ph.external_id = fl.man_external_id
                    LEFT JOIN user_profile up ON ph.external_id = up.external_id
                    LEFT JOIN v3_user_register_device d ON ph.external_id = d.external_id
                    LEFT JOIN v2_utm u ON ph.external_id = u.external_id
                    LEFT JOIN v3_networks n ON u.network_id = n.id
                    LEFT JOIN v3_managers m ON n.parent_manager = m.id
                    LEFT JOIN v3_site s ON up.site_id = s.id
                WHERE ph.first_package = 1
                    AND fl.man_external_id IS null
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
                            OR up.email LIKE '%.ua%'
                    )
                    AND DATE(ph.date_added) BETWEEN DATE(NOW()) - INTERVAL 40 DAY
                    AND DATE(NOW())
                GROUP BY `date_1st_ph`,
                    d.os,
                    n.netw,
                    m.name,
                    s.domain
            ) t1
            LEFT JOIN v3_networks n ON 1 = 1
            AND n.id < 40 + 1
            /*глубина просмотра*/
    ) a1
    LEFT JOIN (
        SELECT q1.`date_1st_ph`,
            q1.`os`,
            q1.`netw`,
            q1.`manager`,
            q1.`domain`,
            q1.`day`,
            SUM(q1.`sum`) `spend`,
            SUM(q1.`sum_women`) `spend_women`
        from (
                SELECT p1.*,
                    DATE(ph.date_added) `date_ph`,
                    SUM(if(p1.`gender` = 0, ph.price, 0)) `sum`,
                    SUM(if(p1.`gender` = 1, ph.price, 0)) `sum_women`,
                    DATEDIFF(DATE(ph.date_added), p1.`date_1st_ph`) `day`
                from (
                        SELECT ph.id,
                            DATE(ph.date_added) `date_1st_ph`,
                            d.os,
                            n.netw,
                            m.name `manager`,
                            s.domain,
                            ph.external_id,
                            up.gender
                        FROM v2_purchase_history ph
                            LEFT JOIN v2_frod_list fl ON ph.external_id = fl.man_external_id
                            LEFT JOIN user_profile up ON ph.external_id = up.external_id
                            LEFT JOIN v3_user_register_device d ON ph.external_id = d.external_id
                            LEFT JOIN v2_utm u ON ph.external_id = u.external_id
                            LEFT JOIN v3_networks n ON u.network_id = n.id
                            LEFT JOIN v3_managers m ON n.parent_manager = m.id
                            LEFT JOIN v3_site s ON up.site_id = s.id
                        WHERE ph.first_package = 1
                            AND fl.man_external_id IS null
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
                                    OR up.email LIKE '%.ua%'
                            )
                            AND DATE(ph.date_added) BETWEEN DATE(NOW()) - INTERVAL 40 DAY
                            AND DATE(NOW())
                    ) p1
                    LEFT JOIN v2_purchase_history ph ON p1.`external_id` = ph.external_id
                GROUP BY p1.`id`,
                    `date_ph`
            ) q1
        GROUP BY q1.`date_1st_ph`,
            q1.`os`,
            q1.`netw`,
            q1.`manager`,
            q1.`domain`,
            q1.`day`
    ) a2 ON a1.`date_1st_ph` = a2.`date_1st_ph`
    AND a1.`os` = a2.`os`
    AND a1.`netw` = a2.`netw`
    AND a1.`manager` = a2.`manager`
    AND a1.`domain` = a2.`domain`
    AND a1.`day` = a2.`day`