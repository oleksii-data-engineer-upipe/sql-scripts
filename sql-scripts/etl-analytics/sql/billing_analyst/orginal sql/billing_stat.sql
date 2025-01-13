SELECT pp1.*,
    if (
        pp1.`ph` = 1,
        ROW_NUMBER() OVER (
            PARTITION BY pp1.`external_id`,
            pp1.`ph`
            ORDER BY pp1.`time`
        ),
        NULL
    ) `num_ph`,
    if (pp1.`time` > pp1.`time_1st_ph`, 1, 0) `check`
FROM (
        SELECT t1.*,
            CONCAT(up.name, ', ', up.`age`) AS `user`,
            IF(fl.man_external_id IS NULL, 0, 1) AS `fraud`,
            ROW_NUMBER() OVER (
                PARTITION BY t1.`external_id`,
                t1.`payment_method`
                ORDER BY t1.`time`
            ) `num_try`,
            MIN(t1.`time`) OVER (PARTITION BY t1.`external_id`) `time_first_try`,
            min(if(t1.`reason` = 'approved', date(t1.`time`), NULL)) OVER (PARTITION BY t1.`external_id`) `date_1st_ph`,
            min(if(t1.`reason` = 'approved', t1.`time`, NULL)) OVER (PARTITION BY t1.`external_id`) `time_1st_ph`,
            LAG(t1.`state`) OVER (
                PARTITION BY t1.`external_id`
                /*, t1.`payment_method`*/
                ORDER BY t1.`external_id`,
                    t1.`time`
            ) AS `lag state`,
            LEAD(t1.`state`) OVER (
                PARTITION BY t1.`external_id`
                /*, t1.`payment_method`*/
                ORDER BY t1.`external_id`,
                    t1.`time`
            ) AS `next state`,
            LAG(t1.`payment_method`) OVER(
                PARTITION BY t1.`external_id`
                ORDER BY t1.`external_id`,
                    t1.`time`
            ) AS `lag payment_method`,
            LEAD(t1.`payment_method`) OVER(
                PARTITION BY t1.`external_id`
                ORDER BY t1.`external_id`,
                    t1.`time`
            ) AS `next payment_method`,
            LAG(t1.`reason`) OVER (
                PARTITION BY t1.`external_id`,
                t1.`payment_method`
                ORDER BY t1.`external_id`,
                    t1.`time`
            ) AS `lag reason`,
            if (t1.`reason` = 'approved', 1, NULL) `ph`,
            n.netw,
            s.name `source`,
            m.name `manager`,
            if (up.country IS NULL, NULL, c.country_name) `country`,
            d.os,
            CASE
                WHEN `site`.`domain` = 'www.sofiadate.com' THEN 'sofiadate.com'
                when up.site_id is null then null
                ELSE `site`.`domain`
            END AS `domain`,
            DATE(CONVERT_TZ(up.created_at, 'UTC', 'Europe/Kiev')) `date_reg`,
            up.age,
            up.abtest
        from (
                SELECT cp.id,
                    IF(
                        cp.payment_method = 'PASTABANK'
                        OR cp.payment_method = 'PASTABANK_APPLEPAY',
                        DATE(cp.date_created),
                        DATE(
                            CONVERT_TZ(cp.date_created, 'UTC', 'Europe/Kiev')
                        )
                    ) AS `date`,
                    LEFT(
                        REPLACE(
                            IF(
                                cp.payment_method = 'PASTABANK'
                                OR cp.payment_method = 'PASTABANK_APPLEPAY',
                                cp.date_created,
                                CONVERT_TZ(cp.date_created, 'UTC', 'Europe/Kiev')
                            ),
                            '.',
                            '-'
                        ),
                        19
                    ) AS `time`,
                    cp.external_id,
                    up.gender,
                    convert(cp.payment_method, CHAR) `payment_method`,
                    convert(cp.`state`, CHAR) `state`,
                    convert(cp.`reason`, CHAR) `reason`,
                    cp.amount,
                    convert(cp.`mid`, CHAR) `mid`,
                    cp.order_id,
                    cp.description,
                    cp.card_number `card`,
                    0 as `pci`
                FROM v2_center_payment cp
                    LEFT JOIN user_profile up ON cp.external_id = up.external_id
                WHERE cp.reason NOT LIKE '%bandone%'
                    AND cp.reason NOT LIKE '%Cancelled by customer%'
                    AND cp.reason NOT LIKE '%sufficient funds%'
                    AND cp.reason NOT LIKE '%AUTH Error (code = 60022)%'
                    AND cp.reason NOT LIKE '%— (code = —)%'
                    AND cp.reason NOT LIKE '%Stolen Card%'
                    AND cp.reason NOT LIKE '%Your card has expired. (code = expired_card)%'
                    AND cp.reason NOT LIKE '%Expired Card%'
                    AND cp.reason NOT LIKE '%(code = 60022)%'
                    AND cp.reason NOT LIKE '%MAC 02: Policy%'
                    AND cp.external_id != 0
                    AND up.name NOT LIKE '%test%'
                    AND up.email NOT LIKE '%test%'
                    AND up.email NOT LIKE '%delete%'
                    AND up.email NOT LIKE '%+%'
                    AND up.tester = 0
                    AND up.country != 222
                    AND up.email NOT LIKE '%upiple%'
                    AND up.email NOT LIKE '%irens%'
                    AND up.email NOT LIKE '%galaktica%'
                    AND cp.`description` NOT LIKE "Sphera%"
                    AND cp.external_id IN (
                        SELECT DISTINCT(cp.external_id)
                        FROM v2_center_payment cp
                        WHERE IF(
                                cp.payment_method = 'PASTABANK'
                                OR cp.payment_method = 'PASTABANK_APPLEPAY',
                                DATE(cp.date_created),
                                DATE(
                                    CONVERT_TZ(cp.date_created, 'UTC', 'Europe/Kiev')
                                )
                            ) >= DATE(NOW()) - INTERVAL 1 year
                    )
                UNION ALL
                SELECT e.id,
                    date(STR_TO_DATE(e.createdAt, '%Y-%m-%d %H:%i:%s')) `date`,
                    STR_TO_DATE(e.createdAt, '%Y-%m-%d %H:%i:%s') `time`,
                    e.userExternalId `external_id`,
                    up.gender,
                    convert(
                        CASE
                            WHEN e.merchantId = 4
                            OR e.merchantId = 5 THEN 'ACQUIRING'
                            when m.bank = 'UNLIMINT'
                            AND m.type = 'GOOGLE_PAY' then 'CARDPAY_GOOGLE_PAY'
                            WHEN m.bank LIKE '%UNLIMINT%' THEN 'CARDPAY'
                            when m.bank = 'PASTABANK'
                            AND m.type = 'APPLE_PAY' then 'PASTABANK_APPLEPAY'
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
                    NULL AS `mid`,
                    NULL AS `order_id`,
                    NULL as `description`,
                    case
                        when m.bank = 'UNIVERSEPAY'
                        AND LENGTH(e.details) < 1000 then CONCAT(
                            JSON_UNQUOTE(
                                JSON_EXTRACT(e.details, '$.paymentResponse.order.cardBin')
                            ),
                            SUBSTRING(
                                JSON_UNQUOTE(
                                    JSON_EXTRACT(e.details, '$.paymentResponse.order.cardMask')
                                ),
                                7
                            )
                        )
                        when m.bank = 'UNIVERSEPAY' then CONCAT(
                            JSON_UNQUOTE(
                                JSON_EXTRACT(
                                    e.details,
                                    '$.paymentResponse.response.payment_method.bin'
                                )
                            ),
                            SUBSTRING(
                                JSON_UNQUOTE(
                                    JSON_EXTRACT(e.details, '$.paymentResponse.order.cardMask')
                                ),
                                7
                            )
                        )
                        when e.details LIKE "%masked_pan%"
                        and m.bank = 'UNLIMINT' then JSON_UNQUOTE(
                            JSON_EXTRACT(
                                JSON_UNQUOTE(
                                    JSON_EXTRACT(e.details, '$.paymentResponse.response')
                                ),
                                '$.card_account.masked_pan'
                            )
                        )
                        when m.bank = 'UNLIMINT'
                        /*cardpay*/
                        then CONCAT(
                            JSON_UNQUOTE(
                                JSON_EXTRACT(e.details, '$.paymentResponse.order.cardBin')
                            ),
                            right(
                                JSON_UNQUOTE(
                                    JSON_EXTRACT(e.details, '$.paymentResponse.order.cardMask')
                                ),
                                10
                            )
                        )
                        when e.details LIKE "%Number%" then JSON_UNQUOTE(
                            JSON_EXTRACT(
                                JSON_UNQUOTE(
                                    JSON_EXTRACT(e.details, '$.paymentResponse.response')
                                ),
                                '$.Card.Number'
                            )
                        )
                        when e.details LIKE "%masked_pan%" then JSON_UNQUOTE(
                            JSON_EXTRACT(
                                JSON_UNQUOTE(
                                    JSON_EXTRACT(e.details, '$.paymentResponse.response')
                                ),
                                '$.card_account.masked_pan'
                            )
                        )
                        when m.bank = 'PASTABANK'
                        AND LENGTH(e.details) < 1200 then CONCAT(
                            JSON_UNQUOTE(
                                JSON_EXTRACT(e.details, '$.paymentResponse.order.cardBin')
                            ),
                            SUBSTRING(
                                JSON_UNQUOTE(
                                    JSON_EXTRACT(e.details, '$.paymentResponse.order.cardMask')
                                ),
                                7
                            )
                        )
                        ELSE NULL
                    end AS `card`,
                    1 as `pci`
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
        ORDER BY t1.`external_id`,
            t1.`time`
    ) pp1
ORDER BY pp1.`external_id`,
    pp1.`time`