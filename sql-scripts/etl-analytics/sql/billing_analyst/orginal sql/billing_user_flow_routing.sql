SELECT q.*,
    @num_ph := IF(
        @prev_ext_id = external_id
        AND @prev_date = DATE
        AND @prev_check = `check`,
        IF(@prev_reason != 'approved', @num_ph + 1, @num_ph),
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
    @prev_reason := reason `st3`,
    @prev_check := `check` `st4`
from (
        SELECT t1.*,
            t.`time_1st_ph`,
            if(t1.`time` > t.`time_1st_ph`, 1, 0) `check`,
            CONCAT(up.name, ', ', up.`age`) AS `user`,
            IF(fl.man_external_id IS NULL, 0, 1) AS `fraud`,
            CASE
                WHEN `site`.`domain` = 'www.sofiadate.com' THEN 'sofiadate.com'
                when up.site_id is null then null
                ELSE `site`.`domain`
            END AS `domain`,
            up.gender
        from (
                SELECT e.id,
                    date(STR_TO_DATE(e.createdAt, '%Y-%m-%d %H:%i:%s')) `date`,
                    STR_TO_DATE(e.createdAt, '%Y-%m-%d %H:%i:%s') `time`,
                    e.userExternalId `external_id`,
                    convert(
                        CASE
                            WHEN e.merchantId = 4
                            OR e.merchantId = 5 THEN 'ACQUIRING'
                            WHEN m.bank LIKE '%UNLIMINT%' THEN 'CARDPAY'
                            ELSE m.bank
                        END,
                        CHAR
                    ) AS `payment_method`,
                    convert(
                        IF(
                            e.`type` = 'PAYMENT_SUCCEDED',
                            'DEPOSITED',
                            'DECLINED'
                        ),
                        CHAR
                    ) AS `state`,
                    convert(
                        IF(
                            e.`type` = 'PAYMENT_SUCCEDED',
                            'approved',
                            JSON_UNQUOTE(
                                SUBSTRING_INDEX(
                                    JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                                    'Error:',
                                    -1
                                )
                            )
                        ),
                        CHAR
                    ) AS `reason`,
                    CAST(
                        JSON_EXTRACT(e.details, '$.paymentResponse.order.amount') AS FLOAT
                    ) AS `amount`,
                    JSON_UNQUOTE(
                        JSON_EXTRACT(e.`details`, '$.paymentResponse.order.cardBin')
                    ) AS `card`
                FROM sphera.`Event` e
                    LEFT JOIN sphera.Merchant m ON e.merchantId = m.id
                    LEFT JOIN user_profile up ON e.userExternalId = up.external_id
                WHERE e.`type` IN ('PAYMENT_FAILED', 'PAYMENT_SUCCEDED')
                    AND up.name NOT LIKE '%test%'
                    AND up.email NOT LIKE '%test%'
                    AND up.email NOT LIKE '%delete%'
                    AND up.email NOT LIKE '%+%'
                    AND up.tester = 0
                    AND up.country != 222
                    AND up.email NOT LIKE '%upiple%'
                    AND up.email NOT LIKE '%irens%'
                    AND up.email NOT LIKE '%galaktica%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%bandone%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%ancelled by customer%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%sufficient%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%AUTH Error (code = 60022)%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%— (code = —)%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%Stolen Card%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%Your card has expired. (code = expired_card)%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%Expired Card%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%(code = 60022)%'
                    AND JSON_UNQUOTE(
                        SUBSTRING_INDEX(
                            JSON_EXTRACT(e.details, '$.paymentResponse.message'),
                            'Error:',
                            -1
                        )
                    ) NOT LIKE '%MAC 02: Policy%'
                GROUP BY 1
                ORDER BY 4,
                    2,
                    3
            ) t1
            LEFT JOIN user_profile up ON t1.`external_id` = up.external_id
            LEFT JOIN v2_utm u ON t1.`external_id` = u.external_id
            LEFT JOIN v2_frod_list fl ON t1.`external_id` = fl.man_external_id
            LEFT JOIN v3_networks n oN u.network_id = n.id
            LEFT JOIN v3_sources s ON n.parent_id = s.id
            LEFT JOIN v3_managers m ON s.parent_id = m.id
            LEFT JOIN country c ON up.country = c.id
            LEFT JOIN v3_user_register_device d ON t1.`external_id` = d.external_id
            LEFT JOIN v3_site `site` ON `site`.`id` = `up`.`site_id`
            LEFT JOIN (
                SELECT e.userExternalId `external_id`,
                    min(STR_TO_DATE(e.createdAt, '%Y-%m-%d %H:%i:%s')) `time_1st_ph`
                FROM sphera.`Event` e
                WHERE e.`type` = 'PAYMENT_SUCCEDED'
                GROUP BY 1
            ) t ON t1.`external_id` = t.`external_id`
        GROUP BY 1
        ORDER BY t1.`external_id`,
            t1.`time`
    ) q,
    (
        SELECT @num_ph := 0,
            @prev_ext_id := NULL,
            @prev_date := NULL,
            @num_pack := 1,
            @prev_reason := NULL,
            @prev_check := 0
    ) vars;