SELECT t.*,
    @num_ph := IF(
        @prev_ext_id = external_id
        AND @prev_date = DATE
        AND @prev_check = `check`,
        IF(
            /*@prev_pm != payment_method,*/
            @prev_reason != 'approved',
            @num_ph + 1,
            @num_ph
        ),
        1
    ) AS num_payment_method,
    @num_pack := if(
        @prev_ext_id = external_id,
        if(
            @prev_reason = 'approved',
            @num_pack + 1,
            @num_pack
        ),
        1
    ) AS `num_pack`,
    @prev_ext_id := external_id `st1`,
    @prev_date := DATE `st2`,
    /*@prev_pm := payment_method `st3`,*/
    @prev_reason := reason `st3`,
    @prev_check := `check` `st4`
from (
        SELECT cp.id,
            if(
                cp.payment_method = 'PASTABANK'
                OR cp.payment_method = 'PASTABANK_APPLEPAY',
                DATE(cp.date_created),
                DATE(
                    CONVERT_TZ(cp.date_created, 'UTC', 'Europe/Kiev')
                )
            ) `date`,
            left(
                replace(
                    if(
                        cp.payment_method = 'PASTABANK'
                        OR cp.payment_method = 'PASTABANK_APPLEPAY',
                        cp.date_created,
                        CONVERT_TZ(cp.date_created, 'UTC', 'Europe/Kiev')
                    ),
                    '.',
                    '-'
                ),
                19
            ) `time`,
            cp.external_id,
            cp.payment_method,
            cp.`state`,
            cp.`reason`,
            cp.amount,
            t.`time_first_ph`,
            if(
                t.`time_first_ph` >= left(
                    replace(
                        if(
                            cp.payment_method = 'PASTABANK'
                            OR cp.payment_method = 'PASTABANK_APPLEPAY',
                            cp.date_created,
                            CONVERT_TZ(cp.date_created, 'UTC', 'Europe/Kiev')
                        ),
                        '.',
                        '-'
                    ),
                    19
                ),
                0,
                1
            ) `check`,
            concat(up.name, ', ', up.`age`) `user`,
            if(fl.man_external_id IS NULL, 0, 1) `fraud`,
            if(up.site_id = 1, 'sofiadate.com', s.domain) `domain`,
            up.gender
        FROM v2_center_payment cp
            LEFT JOIN user_profile up ON cp.external_id = up.external_id
            LEFT JOIN v2_frod_list fl ON cp.external_id = fl.man_external_id
            LEFT JOIN v3_site s ON up.site_id = s.id
            LEFT JOIN (
                SELECT cp.external_id,
                    min(
                        left(
                            replace(
                                if(
                                    cp.payment_method = 'PASTABANK'
                                    OR cp.payment_method = 'PASTABANK_APPLEPAY',
                                    cp.date_created,
                                    CONVERT_TZ(cp.date_created, 'UTC', 'Europe/Kiev')
                                ),
                                '.',
                                '-'
                            ),
                            19
                        )
                    ) `time_first_ph`
                FROM v2_center_payment cp
                WHERE cp.reason = 'approved'
                GROUP BY 1
            ) t ON cp.external_id = t.`external_id`
        WHERE cp.reason NOT LIKE "%bandone%"
            AND `cp`.reason NOT LIKE "%Cancelled by customer%"
            /*AND `cp`.reason NOT LIKE "%sufficient funds%"
             AND `cp`.reason NOT LIKE "%AUTH Error (code = 60022)%"
             AND `cp`.reason NOT LIKE "%— (code = —)%"
             AND `cp`.reason NOT LIKE "%Stolen Card%"
             AND `cp`.reason NOT LIKE "%Your card has expired. (code = expired_card)%"
             AND `cp`.reason NOT LIKE "%Expired Card%"
             AND `cp`.reason NOT LIKE "%(code = 60022)%"*/
            AND cp.external_id != 0
            AND up.name not LIKE '%test%'
            and up.email not LIKE '%test%'
            AND up.email NOT LIKE '%delete%'
            and up.email not LIKE '%+%'
            and up.tester = 0
            and up.country != 222
            and up.email not LIKE '%upiple%'
            and up.email not LIKE '%irens%'
            AND up.email NOT LIKE '%galaktica%' -- AND year(cp.created_at)= 2024 
            AND cp.external_id IN (
                SELECT DISTINCT(cp.external_id) `external_id`
                FROM v2_center_payment cp
                WHERE (
                        if(
                            cp.payment_method = 'PASTABANK'
                            OR cp.payment_method = 'PASTABANK_APPLEPAY',
                            DATE(cp.date_created),
                            DATE(
                                CONVERT_TZ(cp.date_created, 'UTC', 'Europe/Kiev')
                            )
                        )
                    ) >= DATE(NOW()) - INTERVAL 1 year
            )
        ORDER BY cp.external_id,
            `time` -- LIMIT 1000
    ) t,
    (
        SELECT @num_ph := 0,
            @prev_ext_id := NULL,
            @prev_date := NULL,
            @num_pack := 1,
            /*@prev_pm := NULL, */
            @prev_reason := NULL,
            @prev_check := 0
    ) vars;