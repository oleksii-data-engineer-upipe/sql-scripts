

CREATE TABLE dwh.date_dim AS
WITH date_series AS (
    SELECT 
        DATEADD(day, seq, '2023-01-01'::date) AS date_actual
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY a.id) - 1 AS seq
        FROM (SELECT 0 AS id UNION ALL SELECT 1) a,
             (SELECT 0 AS id UNION ALL SELECT 1) b,
             (SELECT 0 AS id UNION ALL SELECT 1) c,
             (SELECT 0 AS id UNION ALL SELECT 1) d,
             (SELECT 0 AS id UNION ALL SELECT 1) e,
             (SELECT 0 AS id UNION ALL SELECT 1) f,
             (SELECT 0 AS id UNION ALL SELECT 1) g
    ) seq
    WHERE DATEADD(day, seq, '2022-01-01'::date) <= '2033-01-01'::date
)
SELECT
    date_actual::date,
    EXTRACT(YEAR FROM date_actual) AS year,
    EXTRACT(MONTH FROM date_actual) AS month,
    EXTRACT(DAY FROM date_actual) AS day
FROM date_series;

-- 1. Матеріалізоване представлення для базової інформації
CREATE MATERIALIZED VIEW svodnie.silver__traffic_mv_basis
DISTSTYLE KEY DISTKEY(netw)
SORTKEY(date, netw, site_id) AS
SELECT 
    d.date_actual AS date,
    n.netw,
    m.name AS manager,
    s.id AS site_id,
    CASE WHEN s.domain = 'www.sofiadate.com' THEN 'sofiadate.com' ELSE s.domain END AS domain
FROM 		dwh.date_dim d
CROSS JOIN 	prod_shatal_db.prodmysqldatabase.v3_networks n
LEFT JOIN 	prod_shatal_db.prodmysqldatabase.v3_sources so 	ON n.parent_id = so.id
LEFT JOIN 	prod_shatal_db.prodmysqldatabase.v3_managers m 	ON so.parent_id = m.id
LEFT JOIN 	prod_shatal_db.prodmysqldatabase.v3_site s 		ON n.site_id = s.id
WHERE d.date_actual >= '2022-01-01'
AND s.id IS NOT NULL;

----- select * from svodnie.silver__traffic_mv_basis order by date  limit 5

-- 2. Матеріалізоване представлення для реєстрацій
CREATE MATERIALIZED VIEW svodnie.silver__traffic_mv_regs
DISTSTYLE KEY DISTKEY(netw)
SORTKEY(date, netw, site_id) AS
SELECT 
    sr.date,
    sr.netw,
    sr.manager,
    s.domain,
    s.id AS site_id,
    SUM(sr.regs_45) AS regs_45,
    SUM(sr.regs) AS regs
FROM summary_regs_2 sr
LEFT JOIN sites s ON sr.site_id = s.id
WHERE sr.date >= '2022-01-01'
GROUP BY sr.date, sr.netw, sr.manager, s.domain, s.id;

-- 3. Матеріалізоване представлення для нових платних користувачів
CREATE MATERIALIZED VIEW svodnie.silver__traffic_mv_new_paid_users
DISTSTYLE KEY DISTKEY(netw)
SORTKEY(date, netw, site_id) AS
SELECT 
    sr.date,
    sr.netw,
    sr.manager,
    s.id AS site_id,
    s.domain,
    SUM(sr.paid) AS paid,
    SUM(sr.paid_45) AS paid_45,
    SUM(sr.paid_reg) AS paid_by_reg_date,
    SUM(sr.paid_45_reg) AS paid_45_by_reg_date
FROM summary_new_paid_users sr
LEFT JOIN sites s ON sr.site_id = s.id
WHERE sr.date >= '2023-01-01'
GROUP BY sr.date, sr.netw, sr.manager, s.id, s.domain;

-- 4. Матеріалізоване представлення для витрат на трафік
CREATE MATERIALIZED VIEW svodnie.silver__traffic_mv_traffic_spend
DISTSTYLE KEY DISTKEY(netw)
SORTKEY(date, netw, site_id) AS
SELECT 
    d.date_actual AS date,
    n.netw,
    m.name AS manager,
    site.id AS site_id,
    CASE WHEN site.domain = 'www.sofiadate.com' THEN 'sofiadate.com' ELSE site.domain END AS domain,
    SUM(ms.amount) AS amount
FROM v3_marketing_spendings ms
JOIN date_dim d ON d.date_actual = DATE(ms.period_start)
LEFT JOIN v3_networks n ON ms.network_id = n.id
LEFT JOIN v3_sources s ON n.parent_id = s.id
LEFT JOIN v3_managers m ON s.parent_id = m.id
LEFT JOIN v3_site site ON n.site_id = site.id
WHERE d.date_actual >= '2023-01-01'
GROUP BY d.date_actual, n.netw, m.name, site.id, site.domain;

-- 5. Матеріалізоване представлення для ROMI (спрощена версія)
CREATE MATERIALIZED VIEW svodnie.silver__traffic_mv_romi
DISTSTYLE KEY DISTKEY(netw)
SORTKEY(date, netw, site_id) AS
SELECT 
    d.date_actual AS date,
    n.netw,
    m.name AS manager,
    up.site_id,
    SUM(ph.price) AS revenue,
    COUNT(DISTINCT CASE WHEN ph.first_package = 1 THEN ph.external_id END) AS new_users
FROM v2_purchase_history ph
JOIN date_dim d ON d.date_actual = DATE(ph.date_added)
JOIN user_profile up ON ph.external_id = up.external_id
JOIN v2_utm u ON up.external_id = u.external_id
JOIN v3_networks n ON u.network_id = n.id
JOIN v3_managers m ON n.parent_manager = m.id
WHERE d.date_actual >= '2023-01-01'
  AND up.name NOT LIKE '%test%'
  AND up.email NOT LIKE '%test%'
  AND up.email NOT LIKE '%upiple%'
  AND up.email NOT LIKE '%galaktica%'
  AND up.tester = 0
  AND up.country != 222
GROUP BY d.date_actual, n.netw, m.name, up.site_id;

-- Фінальний запит, який об'єднує всі матеріалізовані представлення

CREATE MATERIALIZED VIEW svodnie.gold__mv_traffic_spend_report
DISTSTYLE KEY
DISTKEY(netw)
SORTKEY(date, netw, site_id)
AS
SELECT 
    b.date,
    b.netw,
    b.manager,
    b.site_id,
    b.domain,
    COALESCE(r.regs_45, 0) AS regs_45,
    COALESCE(r.regs, 0) AS regs,
    COALESCE(n.paid, 0) AS paid,
    COALESCE(n.paid_45, 0) AS paid_45,
    COALESCE(n.paid_by_reg_date, 0) AS paid_by_reg_date,
    COALESCE(n.paid_45_by_reg_date, 0) AS paid_45_by_reg_date,
    COALESCE(t.amount, 0) AS traffic_spend,
    COALESCE(ro.revenue, 0) AS revenue,
    COALESCE(ro.new_users, 0) AS new_users
FROM mv_basis b
LEFT JOIN mv_regs r ON b.date = r.date AND b.netw = r.netw AND b.manager = r.manager AND b.site_id = r.site_id
LEFT JOIN mv_new_paid_users n ON b.date = n.date AND b.netw = n.netw AND b.manager = n.manager AND b.site_id = n.site_id
LEFT JOIN mv_traffic_spend t ON b.date = t.date AND b.netw = t.netw AND b.manager = t.manager AND b.site_id = t.site_id
LEFT JOIN mv_romi ro ON b.date = ro.date AND b.netw = ro.netw AND b.manager = ro.manager AND b.site_id = ro.site_id
WHERE b.date BETWEEN '2023-01-01' AND CURRENT_DATE - INTERVAL '1 day';


-- Команди для оновлення матеріалізованих представлень
REFRESH MATERIALIZED VIEW  mv_basis;
REFRESH MATERIALIZED VIEW  mv_regs;
REFRESH MATERIALIZED VIEW  mv_new_paid_users;
COMMIT;

REFRESH MATERIALIZED VIEW  mv_romi;
COMMIT;

REFRESH MATERIALIZED VIEW  mv_traffic_spend;
COMMIT;

