CREATE TABLE ga4.google_analyitcs(
    date DATE ENCODE delta, -- Дата події, ефективне кодування для дат
    campaign_id VARCHAR(255) ENCODE zstd, -- Унікальний ідентифікатор кампанії
    source_medium VARCHAR(255) ENCODE zstd, -- Джерело і канал трафіку
    country VARCHAR(100) ENCODE zstd, -- Країна користувача
    page_path VARCHAR(4095) ENCODE zstd, -- Шлях до сторінки
    landing_page VARCHAR(4095) ENCODE zstd, -- Лендінг сторінка
    referrer VARCHAR(4095) ENCODE zstd, -- URL реферера
    os VARCHAR(100) ENCODE zstd, -- Операційна система користувача
    wbraid VARCHAR(255) ENCODE zstd, -- Рекламний ідентифікатор wbraid
    gbraid VARCHAR(255) ENCODE zstd, -- Рекламний ідентифікатор gbraid
    gclid VARCHAR(255) ENCODE zstd, -- Рекламний ідентифікатор Google Ads
    email VARCHAR(255) ENCODE zstd, -- Email користувача
    fbclid VARCHAR(255) ENCODE zstd, -- Рекламний ідентифікатор Facebook
    aff_cd VARCHAR(255) ENCODE zstd, -- Код афіліата
    utm_medium VARCHAR(255) ENCODE zstd, -- Значення UTM-параметра medium
    page_views INT ENCODE az64, -- Кількість переглядів сторінки
    bounce_rate FLOAT ENCODE RAW, -- Показник відмов (%)
    publisher_ad_impressions FLOAT ENCODE RAW, -- Враження реклами
    publisher_ad_clicks FLOAT ENCODE RAW, -- Кліки по рекламі
    total_revenue FLOAT ENCODE RAW, -- Загальний дохід
    domain VARCHAR(255) ENCODE zstd -- Домен джерела даних
)
DISTSTYLE KEY                      -- Використовуємо ключ для оптимального розподілу
DISTKEY (domain)                   -- Розподіл по домену для балансування даних
SORTKEY (date, source_medium);     -- Сортування за датою і джерелом для прискорення запитів
