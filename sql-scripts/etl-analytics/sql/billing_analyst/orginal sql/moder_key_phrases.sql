SELECT t1.*
from (
        SELECT t1.*,
            up.register_date_kiev,
            s.domain
        from (
                SELECT cm.id,
                    cm.date_created_kiev `date`,
                    cm.sender_external_id `girl_sender`,
                    cm.recipient_external_id `man_receiver`,
                    cm.message_content,
                    case
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%photos%" then 'send + photos'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%letters%" then 'send + letters'
                        when cm.message_content LIKE "%vibration%" then 'vibration'
                        when cm.message_content LIKE "%buzzing%"
                        AND cm.message_content LIKE "%letters%" then 'buzzing + letters'
                        when cm.message_content LIKE "%fast%"
                        AND cm.message_content LIKE "%letters%" then 'fast + letters'
                        when cm.message_content LIKE "%vibraate%" then 'vibraate'
                        when cm.message_content LIKE "%vibrate%" then 'vibrate'
                        when cm.message_content LIKE "%vibes%" then 'vibes'
                        when cm.message_content LIKE "%put%"
                        AND cm.message_content LIKE "%phone%"
                        aND cm.message_content LIKE "%cat%" then 'put + phone + cat'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%quick%"
                        aND cm.message_content LIKE "%letter%" then 'send + quick + letter'
                        when cm.message_content LIKE "%put%"
                        AND cm.message_content LIKE "%pussy%" then 'put + pussy'
                        when cm.message_content LIKE "%quick%"
                        AND cm.message_content LIKE "%email%" then 'quick email'
                        when cm.message_content LIKE "%vibration%"
                        AND cm.message_content LIKE "%pussy%" then 'vibration + pussy'
                        when cm.message_content LIKE "%phone%"
                        AND cm.message_content LIKE "%vibrat%" then 'phone + vibrat'
                        when cm.message_content LIKE "%vibrates%" then 'vibrates'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%photo%"
                        aND cm.message_content LIKE "%times%" then 'send + photo + times'
                        when cm.message_content LIKE "%way%"
                        AND cm.message_content LIKE "%leave%"
                        aND cm.message_content LIKE "%here%" then 'way + leave + here'
                        when cm.message_content LIKE "%give%"
                        AND cm.message_content LIKE "%letters%" then 'give + letters'
                        when cm.message_content LIKE "%number%"
                        AND cm.message_content LIKE "%video%" then 'number + video'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%number%"
                        aND cm.message_content LIKE "%letter%" then 'send + number + letter'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%symbols%" then 'send + symbols'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%digits%" then 'send + digits'
                        when cm.message_content LIKE "%call me%" then 'call me'
                        when cm.message_content LIKE "%necklace%" then 'necklace'
                        when cm.message_content LIKE "%chain%" then 'chain'
                        when cm.message_content LIKE "%program%" then 'program'
                        when cm.message_content LIKE "%problem with my laptop%" then 'problem with my laptop'
                        when cm.message_content LIKE "%talk to me off the site%" then 'talk to me off the site'
                        when cm.message_content LIKE "%English%"
                        AND cm.message_content LIKE "%courses%" then 'English + courses'
                        when cm.message_content LIKE "%number%"
                        AND cm.message_content LIKE "%letter%" then 'number + letter'
                        when cm.message_content LIKE "%money%"
                        AND cm.message_content LIKE "%return%" then 'money + return'
                        when cm.message_content LIKE "%number%"
                        AND cm.message_content LIKE "%record%" then 'number + record'
                        when cm.message_content LIKE "%record%"
                        AND cm.message_content LIKE "%mail%" then 'record + mail'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%image%"
                        aND cm.message_content LIKE "%times%" then 'send + image + times'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%letter%"
                        aND cm.message_content LIKE "%times%" then 'send + letter + times'
                        when cm.message_content LIKE "%send%"
                        AND cm.message_content LIKE "%message%"
                        aND cm.message_content LIKE "%times%" then 'send + message + times'
                        ELSE NULL
                    END AS `word`
                FROM chat_message cm
                WHERE cm.is_male = 0
                    AND cm.date_created_kiev = DATE(NOW()) - INTERVAL 1 DAY
                    AND cm.message_type = 'SENT_TEXT'
            ) t1
            LEFT JOIN profiles up on t1.`man_receiver` = up.external_id
            LEFT JOIN sites s ON s.id = up.site_id
        where t1.`word` IS NOT NULL
    ) t1
group BY t1.`id`