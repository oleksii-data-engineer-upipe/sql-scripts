SELECT t1.`date`,
    t1.`тратили новые` AS `spend_new`,
    t1.`тратили старые` AS `spend_old`,
    t2.`покупали new` AS `buy_new`,
    t2.`price new`,
    t2.`amount new` AS `amount_new`,
    t2.`покупали old` AS `buy_old`,
    t2.`price old`,
    t2.`amount old`,
    t3.`spend new`,
    t3.`new users`,
    t3.`spend old`,
    t3.`old users`
from (
        SELECT q3.`date`,
            COUNT(DISTINCT(q3.`new`)) `тратили новые`,
            COUNT(DISTINCT(q3.`old`)) `тратили старые`
        from (
                SELECT q2.*,
                    if(
                        MONTH(q2.`date`) = MONTH(q2.`date_1st_ph`)
                        AND year(q2.`date`) = year(q2.`date_1st_ph`),
                        q2.`user_id`,
                        NULL
                    ) `new`,
                    if(
                        MONTH(q2.`date`) != MONTH(q2.`date_1st_ph`)
                        or year(q2.`date`) != year(q2.`date_1st_ph`),
                        q2.`user_id`,
                        NULL
                    ) `old`
                FROM (
                        SELECT q1.*,
                            DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph`
                        from (
                                SELECT DATE(CONVERT_TZ(l.date_created, 'UTC', 'Europe/Kiev')) `date`,
                                    l.user_id
                                FROM log l
                                    LEFT JOIN user_profile up ON l.user_id = up.id
                                    LEFT JOIN v2_frod_list fl ON up.external_id = fl.man_external_id
                                WHERE l.is_male = 1
                                    AND fl.man_external_id IS null
                                    AND l.reward_status IN (1, 3)
                                    AND l.action_price != 0
                                    AND l.agency_id != 0
                                    AND l.action_type NOT LIKE "%BONUS%"
                                    AND l.action_type NOT LIKE "%ACCOUNT%"
                                    AND l.action_type NOT LIKE "%FOLLOW%"
                                    AND DATE(CONVERT_TZ(l.date_created, 'UTC', 'Europe/Kiev')) >= date(now()) - interval 1 day
                                GROUP BY `date`,
                                    l.user_id
                            ) q1
                            LEFT JOIN v2_purchase_history ph ON q1.`user_id` = ph.user_id
                            AND ph.first_package = 1
                    ) q2
            ) q3
        GROUP BY q3.`date`
    ) t1
    LEFT JOIN (
        SELECT date(q1.`date_ph`) AS `date`,
            count(
                distinct(
                    if(
                        q1.`fraud` = 0
                        AND MONTH(q1.`date_ph`) = MONTH(q1.`date_1st_ph`),
                        q1.`external_id`,
                        NULL
                    )
                )
            ) `покупали new`,
            sum(
                if(
                    MONTH(q1.`date_ph`) = MONTH(q1.`date_1st_ph`),
                    q1.`price`,
                    NULL
                )
            ) `price new`,
            sum(
                if(
                    MONTH(q1.`date_ph`) = MONTH(q1.`date_1st_ph`),
                    q1.`amount`,
                    NULL
                )
            ) `amount new`,
            count(
                distinct(
                    if(
                        q1.`fraud` = 0
                        AND MONTH(q1.`date_ph`) != MONTH(q1.`date_1st_ph`),
                        q1.`external_id`,
                        NULL
                    )
                )
            ) `покупали old`,
            sum(
                if(
                    MONTH(q1.`date_ph`) != MONTH(q1.`date_1st_ph`),
                    q1.`price`,
                    NULL
                )
            ) `price old`,
            sum(
                if(
                    MONTH(q1.`date_ph`) != MONTH(q1.`date_1st_ph`),
                    q1.`amount`,
                    NULL
                )
            ) `amount old`
        from (
                SELECT w1.*,
                    DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph`
                from (
                        SELECT ph.external_id,
                            ph.user_id,
                            if(fl.man_external_id IS NOT NULL, 1, 0) `fraud`,
                            CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev') `date_ph`,
                            ph.first_package,
                            ph.price,
                            ph.amount
                        FROM v2_purchase_history ph
                            LEFT JOIN v2_frod_list fl ON ph.external_id = fl.man_external_id
                            LEFT JOIN user_profile up ON ph.external_id = up.external_id
                        WHERE DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) >= date(now()) - interval 1 day
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
                            )
                    ) w1
                    LEFT JOIN v2_purchase_history ph ON ph.external_id = w1.`external_id`
                    AND ph.first_package = 1
                GROUP BY w1.`external_id`,
                    w1.`user_id`,
                    w1.`fraud`,
                    w1.`date_ph`,
                    w1.`first_package`,
                    w1.`price`,
                    w1.`amount`
            ) q1
        GROUP BY `date`
    ) t2 ON t1.`date` = t2.`date`
    LEFT JOIN (
        select w1.`date`,
            SUM(w1.`spend new`) `spend new`,
            COUNT(DISTINCT(w1.`new users`)) `new users`,
            SUM(w1.`spend old`) `spend old`,
            COUNT(DISTINCT(w1.`old users`)) `old users`
        from (
                SELECT a1.`date` `date`,
                    if(a1.`new` = 1, a1.`spend`, null) `spend new`,
                    if(a1.`new` = 1, a1.`user_id`, null) `new users`,
                    if(a1.`new` = 0, a1.`spend`, null) `spend old`,
                    if(a1.`new` = 0, a1.`user_id`, null) `old users`
                from (
                        SELECT qq1.*,
                            if(
                                MONTH(qq1.`date`) = MONTH(qq1.`date_1st_ph`)
                                AND year(qq1.`date`) = year(qq1.`date_1st_ph`),
                                1,
                                0
                            ) `new`
                        from (
                                select q1.*,
                                    DATE(CONVERT_TZ(ph.date_added, 'UTC', 'Europe/Kiev')) `date_1st_ph`
                                from (
                                        SELECT date(CONVERT_TZ(l.date_created, 'UTC', 'Europe/Kiev')) `date`,
                                            l.user_id,
                                            SUM(l.action_price) * 0.22 `spend`
                                        FROM log l
                                        WHERE l.is_male = 1
                                            AND l.reward_status IN (1, 3)
                                            AND l.action_price != 0
                                            AND l.agency_id != 0
                                            AND l.action_type NOT LIKE "%BONUS%"
                                            AND l.action_type NOT LIKE "%ACCOUNT%"
                                            AND l.action_type NOT LIKE "%FOLLOW%"
                                            AND DATE(CONVERT_TZ(l.date_created, 'UTC', 'Europe/Kiev')) >= date(now()) - interval 1 day
                                        GROUP BY `date`,
                                            l.user_id
                                    ) q1
                                    LEFT JOIN v2_purchase_history ph ON q1.`user_id` = ph.user_id
                                    AND ph.first_package = 1
                                GROUP BY q1.`date`,
                                    q1.`user_id`,
                                    q1.`spend`
                            ) qq1
                    ) a1
            ) w1
        GROUP BY w1.`date`
    ) t3 ON t1.`date` = t3.`date`