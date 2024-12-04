    SELECT 
        "userExternalId" AS external_id,
        MIN("createdAt"::TIMESTAMP) AS time_1st_ph
    FROM redshift_analytics_db.sphera."Event"
    WHERE type = 'PAYMENT_SUCCEDED'
    GROUP BY 1