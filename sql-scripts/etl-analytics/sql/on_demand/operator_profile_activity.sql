create schema if not exists operator_profile_activity;

CREATE TABLE if not exists operator_profile_activity.gold__report (
    date DATE ENCODE delta,                             
    operator_id INTEGER ENCODE az64,                    
    profile_external_id BIGINT ENCODE az64,            
    recieved_view SMALLINT ENCODE az64,                
    recieved_text SMALLINT ENCODE az64,
    recieved_view_photos SMALLINT ENCODE az64,
    recieved_like SMALLINT ENCODE az64,          
    recieved_wink SMALLINT ENCODE az64,               
    recieved_image SMALLINT ENCODE az64,
    recieved_sticker SMALLINT ENCODE az64,
    recieved_video SMALLINT ENCODE az64,
    recieved_audio SMALLINT ENCODE az64,
    sent_image SMALLINT ENCODE az64,                     
    sent_text SMALLINT ENCODE az64,
    sent_like SMALLINT ENCODE az64,                    
    sent_sticker SMALLINT ENCODE az64,
    sent_video SMALLINT ENCODE az64,
    sent_audio SMALLINT ENCODE az64,
    sent_personal_invites_chat SMALLINT ENCODE az64,
    sent_personal_invites_letter SMALLINT ENCODE az64,
    sent_letters SMALLINT ENCODE az64,                   
    recieved_letters SMALLINT ENCODE az64,
    sent_invites_letter SMALLINT ENCODE az64,
    sent_invites_chat SMALLINT ENCODE az64
)
DISTSTYLE KEY DISTKEY(profile_external_id)           
SORTKEY(date, operator_id);


CREATE MATERIALIZED VIEW operator_profile_activity.gold__report_mv
AS
WITH chats AS (
    SELECT 
        DATE(cm.date_created) AS date,
        cm.operator_id,
        CASE WHEN cm.is_male = 0 THEN cm.sender_external_id ELSE cm.recipient_external_id END 	AS profile_external_id,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_VIEW' THEN 1 ELSE 0 END) 		AS recieved_view,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_TEXT' THEN 1 ELSE 0 END) 		AS recieved_text,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_VIEW_PHOTOS' THEN 1 ELSE 0 END) AS recieved_view_photos,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_LIKE' THEN 1 ELSE 0 END) 		AS recieved_like,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_WINK' THEN 1 ELSE 0 END) 		AS recieved_wink,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_IMAGE' THEN 1 ELSE 0 END) 		AS recieved_image,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_STICKER' THEN 1 ELSE 0 END) 	AS recieved_sticker,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_VIDEO' THEN 1 ELSE 0 END) 		AS recieved_video,
        SUM(CASE WHEN cm.is_male = 1 AND cm.message_type = 'SENT_AUDIO' THEN 1 ELSE 0 END) 		AS recieved_audio,
        SUM(CASE WHEN cm.is_male = 0 AND cm.message_type = 'SENT_IMAGE' THEN 1 ELSE 0 END) 		AS sent_image,
        SUM(CASE WHEN cm.is_male = 0 AND cm.message_type = 'SENT_TEXT' THEN 1 ELSE 0 END) 		AS sent_text,
        SUM(CASE WHEN cm.is_male = 0 AND cm.message_type = 'SENT_LIKE' THEN 1 ELSE 0 END) 		AS sent_like,
        SUM(CASE WHEN cm.is_male = 0 AND cm.message_type = 'SENT_STICKER' THEN 1 ELSE 0 END) 	AS sent_sticker,
        SUM(CASE WHEN cm.is_male = 0 AND cm.message_type = 'SENT_VIDEO' THEN 1 ELSE 0 END) 		AS sent_video,
        SUM(CASE WHEN cm.is_male = 0 AND cm.message_type = 'SENT_AUDIO' THEN 1 ELSE 0 END) 		AS sent_audio,
        COUNT(DISTINCT CASE WHEN cm.sender_type = 1 THEN cm.recipient_external_id ELSE NULL END) AS sent_personal_invites_chat
    FROM prod_shatal_db.prodmysqldatabase.v2_chat_message cm
    WHERE cm.date_created BETWEEN DATEADD(week, -1, GETDATE()) AND DATEADD(day, -1, GETDATE())
        AND cm.message_content != ''
    GROUP BY 1, 2, 3
),
mail AS (
    SELECT 
        DATE(um.date_created) AS date,
        um.operator_id,
        wi.external_id AS profile_external_id,
        SUM(CASE WHEN um.operator = 1 THEN 1 ELSE 0 END) AS sent_letters,
        SUM(CASE WHEN um.operator = 0 THEN 1 ELSE 0 END) AS recieved_letters,
        COUNT(DISTINCT CASE WHEN um.sender_type = 1 THEN um.recipient_id ELSE NULL END) AS sent_personal_invites_letter
    FROM prod_shatal_db.prodmysqldatabase.v2_user_mail um
    INNER JOIN prod_shatal_db.prodmysqldatabase.v2_woman_information wi ON wi.id = um.woman_id
    WHERE um.date_created BETWEEN DATEADD(week, -1, GETDATE()) AND DATEADD(day, -1, GETDATE())
    GROUP BY 1, 2, 3
),
invites AS (
    SELECT 
        DATE(sh.created_at) AS date,
        sh.operator_id,
        sh.woman_external_id AS profile_external_id,
        SUM(CASE WHEN sh.sender_type = 'Chat' THEN 1 ELSE 0 END) AS sent_invites_chat,
        SUM(CASE WHEN sh.sender_type = 'Letter' THEN 1 ELSE 0 END) AS sent_invites_letter
    FROM prod_shatal_db.prodmysqldatabase.v2_sender_history sh
    WHERE sh.created_at BETWEEN DATEADD(week, -1, GETDATE()) AND DATEADD(day, -1, GETDATE())
    GROUP BY 1, 2, 3
)
SELECT 
    chats.*,
    mail.sent_personal_invites_letter,
    mail.sent_letters,
    mail.recieved_letters,
    invites.sent_invites_letter,
    invites.sent_invites_chat
FROM chats
LEFT JOIN mail ON mail.date = chats.date 
    AND mail.operator_id = chats.operator_id 
    AND mail.profile_external_id = chats.profile_external_id
LEFT JOIN invites ON invites.date = chats.date 
    AND invites.operator_id = chats.operator_id 
    AND invites.profile_external_id = chats.profile_external_id;

   
CREATE OR REPLACE PROCEDURE operator_profile_activity.refresh()
AS 
$$
BEGIN
	REFRESH MATERIALIZED VIEW operator_profile_activity.gold__report_mv;
	COMMIT;	
	INSERT INTO operator_profile_activity.gold__report
		SELECT *
		FROM operator_profile_activity.gold__report_mv mv
		WHERE NOT EXISTS (
		    SELECT 1
		    FROM operator_profile_activity.gold__report main
		    WHERE main.date = mv.date
		      AND main.operator_id = mv.operator_id
		      AND main.profile_external_id = mv.profile_external_id
		);
	COMMIT;
	ANALYZE operator_profile_activity.gold__report;
	COMMIT;
END;
$$ 
LANGUAGE plpgsql;



call operator_profile_activity.refresh();

vacuum operator_profile_activity.gold__report;


select 
	date , count(*)
from operator_profile_activity.gold__report
group by date
order by 1 desc
;

