SELECT *
FROM (
        SELECT m1.*,
            SUM(ph.price) running_total,
            if(ph.first_package = 1, ph.date_added, NULL) converted
        FROM (
                SELECT fp.date date_added,
                    fp.male_external_id external_id,
                    up.gender,
                    up.site_id,
                    SUM(fp.action_price) * 0.22 amount,
                    if(
                        pm.date_heigh_role <= fp.date,
                        2,
                        IF(pm.date_maybe_height <= fp.date, 1, 0)
                    ) type,
                    date(up.register_date) registered,
                    IF(
                        fp.action_type IN ('GET_MEETING', 'MAKE_ORDER_APPROVE'),
                        'Gift',
                        'Balance'
                    ) Action
                FROM f_man_paid_actions fp
                    LEFT JOIN profiles up ON up.external_id = fp.male_external_id
                    LEFT JOIN paid_user_marked pm ON pm.external_id = fp.male_external_id
                WHERE up.name NOT LIKE '%test%'
                    AND up.email NOT LIKE '%test%'
                    AND up.email NOT LIKE '%delete%'
                    AND up.email NOT LIKE '%+%'
                    AND up.tester = 0
                    AND up.email NOT LIKE '%upiple%'
                    AND up.email NOT LIKE '%irens %' -- And up.site_id in (1,2,5,6)
                GROUP BY fp.date,
                    fp.male_external_id,
                    Action
            ) m1
            LEFT JOIN purchase_history ph ON ph.external_id = m1.external_id
            AND ph.date_added <= m1.date_added
        GROUP BY m1.date_added,
            m1.external_id
        UNION ALL
        SELECT m1.*,
            'Пополнение' Action,
            SUM(ph.price) running_total,
            if(ph.first_package = 1, ph.date_added, NULL) converted
        FROM (
                SELECT ph.date_added,
                    ph.external_id,
                    up.gender,
                    up.site_id,
                    SUM(ph.price) amount,
                    if(
                        pm.date_heigh_role <= ph.date_added,
                        2,
                        IF(pm.date_maybe_height <= ph.date_added, 1, 0)
                    ) type,
                    date(up.register_date) registered
                FROM purchase_history ph
                    LEFT JOIN paid_user_marked pm ON pm.external_id = ph.external_id
                    LEFT JOIN profiles up ON up.external_id = ph.external_id
                WHERE up.name NOT LIKE '%test%'
                    AND up.email NOT LIKE '%test%'
                    AND up.email NOT LIKE '%delete%'
                    AND up.email NOT LIKE '%+%'
                    AND up.tester = 0 -- And up.site_id in (1,2,5,6)
                    AND up.email NOT LIKE '%upiple%'
                    AND up.email NOT LIKE '%irens %'
                GROUP BY ph.date_added,
                    ph.external_id
            ) m1
            LEFT JOIN purchase_history ph ON ph.external_id = m1.external_id
            AND ph.date_added <= m1.date_added
        GROUP BY m1.date_added,
            m1.external_id
    ) t1
WHERE date_added = DATE(NOW()) - INTERVAL 1 DAY