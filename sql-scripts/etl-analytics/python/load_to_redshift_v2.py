import os
import time
import pandas as pd
from datetime import datetime
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
import psycopg2
from io import StringIO

# Налаштування креденшелів Google
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/mac/Downloads/Redshift integration-bdb44d4849b7.json"

# Налаштування відображення pandas
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

REDSHIFT_CREDS = {
    "host": "analytics-redshift.ct1gti9unztm.us-east-2.redshift.amazonaws.com",
    "port": "5439",
    "database": "prod_analytic_db",
    "user": "oleksiyde",
    "password": "25HFyfq^4vh5gcI1U0bhjcR"
}

def get_csv_from_df(df):
    """Конвертує DataFrame в CSV в пам'яті"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep='|')
    csv_buffer.seek(0)
    return csv_buffer

def copy_to_redshift(df, schema, table, conn):
    """Завантажує DataFrame в Redshift використовуючи копіювання даних"""
    cursor = conn.cursor()
    csv_data = get_csv_from_df(df)
    
    try:
        cursor.execute("BEGIN")
        # Видаляємо старі дані за ці дати
        cursor.execute(f"DELETE FROM {schema}.{table} WHERE date >= %s", (df['date'].min(),))
        
        # Копіюємо нові дані
        cursor.copy_expert(
            sql=f"""
            COPY {schema}.{table}
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER '|', HEADER TRUE)
            """,
            file=csv_data
        )
        
        conn.commit()
        print(f"Successfully loaded {len(df)} rows to {schema}.{table}")
    
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        csv_data.close()

def fetch_paginated_data(client, request, page_size=10000):
    rows = []
    offset = 0
    while True:
        request.offset = offset
        request.limit = page_size
        response = client.run_report(request)
        rows.extend(response.rows)
        if len(response.rows) < page_size:
            break
        offset += page_size
    return rows

def extract_ad_params(url):
    params = {k: None for k in ['wbraid', 'gbraid', 'gclid', 'email', 'fbclid', 'aff_cd', 'utm_medium']}
    if not url or not isinstance(url, str):
        return params
    for part in url.split('&'):
        for param in params:
            if f"{param}=" in part:
                try:
                    params[param] = part.split(f"{param}=")[1]
                except:
                    continue
    return params

def prepare_data_types(df):
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    varchar_limits = {
        'campaign_id': 255, 'source_medium': 255, 'country': 100,
        'page_path': 4095, 'landing_page': 4095, 'referrer': 4095,
        'os': 100, 'wbraid': 255, 'gbraid': 255, 'gclid': 255,
        'email': 255, 'fbclid': 255, 'aff_cd': 255, 'utm_medium': 255,
        'domain': 255
    }
    
    for col, limit in varchar_limits.items():
        df[col] = df[col].astype(str).str[:limit].fillna('')
    
    df['page_views'] = df['page_views'].astype('int32')
    for col in ['bounce_rate', 'publisher_ad_impressions', 'publisher_ad_clicks', 'total_revenue']:
        df[col] = df[col].astype('float64')
    
    return df

def save_to_redshift(df, schema="ga4", table="google_analyitcs"):
    try:
        df['loaded_at'] = datetime.now()
        df = prepare_data_types(df)
        
        conn = psycopg2.connect(**REDSHIFT_CREDS)
        copy_to_redshift(df, schema, table, conn)
        conn.close()
        
    except Exception as e:
        print(f"Error loading to Redshift: {e}")
        if 'conn' in locals():
            conn.close()
        raise

def get_ga4_data(property_id: str, domain: str, days_ago: int = 30):
    start_time = time.time()
    client = BetaAnalyticsDataClient()
    
    try:
        dimensions = [
            "date", "sessionCampaignId", "sessionSourceMedium",
            "country", "pagePath", "landingPage", "pageReferrer",
            "operatingSystemWithVersion"
        ]
        
        metrics = [
            "sessions", "publisherAdImpressions", "publisherAdClicks",
            "totalRevenue", "screenPageViews", "bounceRate"
        ]
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=f"{days_ago}daysAgo", end_date="today")],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics]
        )

        rows = fetch_paginated_data(client, request)
        
        if not rows:
            print(f"No data for {property_id}")
            return pd.DataFrame()

        data = []
        for row in rows:
            d = row.dimension_values
            m = row.metric_values
            ad_params = extract_ad_params(d[6].value)
            
            data.append({
                'date': pd.to_datetime(d[0].value).strftime('%Y-%m-%d'),
                'domain': domain,
                'campaign_id': d[1].value,
                'source_medium': d[2].value,
                'country': d[3].value,
                'page_path': d[4].value,
                'landing_page': d[5].value,
                'referrer': d[6].value,
                'os': d[7].value,
                **ad_params,
                'page_views': int(float(m[4].value)),
                'bounce_rate': float(m[5].value),
                'publisher_ad_impressions': float(m[1].value),
                'publisher_ad_clicks': float(m[2].value),
                'total_revenue': float(m[3].value)
            })

        df = pd.DataFrame(data)
        print(f"Processed {len(df)} rows in {time.time() - start_time:.2f}s")
        return df

    except Exception as e:
        print(f"GA4 API error: {e}")
        raise

sites_data = {
    # "online-dating-review.net": 450191495,
    "avodate.com": 350536871,
    "datempire.com": 358067421,
    "feelflame.com": 358106858,
    "latidate.com": 358050088,
    "myspecialdates.com": 322504563,
    "okamour.com": 350538354,
    "sakuradate.com": 358590047,
    "sofiadate.com": 322587243,
    "loveforheart.com": 322569296
}

for domain, ga_id in sites_data.items():
    try:
        print(f"\nProcessing {domain}")
        df = get_ga4_data(str(ga_id), domain, days_ago=2)
        if not df.empty:
            save_to_redshift(df)
    except Exception as e:
        print(f"Error processing {domain}: {e}")