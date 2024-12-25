import os
import time
import pandas as pd
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (DateRange, Dimension, Metric, RunReportRequest)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/mac/Downloads/Redshift integration-bdb44d4849b7.json"

print(1)

# Налаштування відображення pandas
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)


def fetch_paginated_data(client, request, page_size=10000):
    rows = []
    current_offset = 0
    
    while True:
        request.offset = current_offset
        request.limit = page_size
        
        response = client.run_report(request)
        rows.extend(response.rows)
        
        if len(response.rows) < page_size: break
        current_offset += page_size  
    return rows


def extract_ad_params(url):
    params = {
        'wbraid': None,
        'gbraid': None,
        'gclid': None,
        'email': None,
        'fbclid': None, 
        'aff_cd': None, 
        'utm_medium': None
    }
    
    if not url or not isinstance(url, str): return params 
    url_parts = url.split('&')
    for part in url_parts:
        for param in params.keys():
            if f"{param}=" in part:
                try:
                    value = part.split(f"{param}=")[1]
                    params[param] = value
                except: continue          
    return params

print(2)


def get_ga4_data_uno(property_id: str, days_ago: int = 30, filter_ads: int = 0):
    start_time = time.time()
    client = BetaAnalyticsDataClient()
    
    try:
        request2 = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=f"{days_ago}daysAgo", end_date="today")],
            dimensions=[
                Dimension(name=d) for d in [
                    "date", "sessionCampaignId", "sessionSourceMedium",
                    "country", "pagePath", "landingPage", "pageReferrer", "operatingSystemWithVersion"
                ]
            ],
            metrics=[
                Metric(name=m) for m in ["sessions", "publisherAdImpressions", "publisherAdClicks", "totalRevenue", "screenPageViews", "bounceRate"]
            ]
        )

        response2_rows = fetch_paginated_data(client, request2)


        # Перевіряємо, чи є дані
        if not response2_rows :
            print(f"No data found for property {property_id}\n")
            return pd.DataFrame()


        # Обробка другого набору даних
        data2 = []
        for row in response2_rows:
            d = row.dimension_values
            m = row.metric_values
            ad_params = extract_ad_params(d[6].value)  # Витягуємо параметри з referrer
            data2.append({
                'date': pd.to_datetime(d[0].value).strftime('%Y-%m-%d hh:mm:ss'),
                'campaign_id': d[1].value,
                'source_medium': d[2].value,
                'country': d[3].value,
                'page_path': d[4].value,
                'landing_page': d[5].value,
                'referrer': d[6].value,
                'os': d[7].value,
                'wbraid': ad_params['wbraid'],
                'gbraid': ad_params['gbraid'],
                'gclid': ad_params['gclid'],
                'email': ad_params['email'],
                'fbclid': ad_params['fbclid'],
                'aff_cd': ad_params['aff_cd'],
                'utm_medium': ad_params['utm_medium'],
                'page_views': int(float(m[4].value)),
                'bounce_rate': float(m[5].value),
                'publisher_ad_impressions': float(m[1].value),
                'publisher_ad_clicks': float(m[2].value),
                'total_revenue': float(m[3].value)
            })

        df3 = pd.DataFrame(data2)
        
        # Фільтруємо за рекламними параметрами, якщо потрібно
        if filter_ads == 1:
            df3 = df3[df3[['wbraid', 'gbraid', 'gclid']].notna().any(axis=1)]

        count_ad_rows = df3[['wbraid', 'gbraid', 'gclid']].notna().any(axis=1).sum()
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"Rows processed: \t\t\t{len(df3):,}".replace(',', '_') )
        print(f"Rows with id (wbraid, gbraid, gclid): \t{count_ad_rows:,}".replace(',', '_'))
        print(f"Percentage: \t\t\t\t{int(count_ad_rows/len(df3)*1000)/10}%")
        print(f"Execution time: \t\t\t{elapsed_time:.2f} seconds\n")
        
        return df3

    except Exception as e:
        print(f"Error accessing GA4: {str(e)}\n")
        raise

print("3. functions loaded")



sites_data = {
    # "online-dating-review.net": 450191495,
    "avodate.com": 350536871,
    # "datempire.com": 358067421,
    # "feelflame.com": 358106858,
    # "latidate.com": 358050088,
    # "myspecialdates.com": 322504563,
    # "okamour.com": 350538354,
    # "sakuradate.com": 358590047,
    # "sofiadate.com": 322587243,
    "loveforheart.com": 322569296
}


from datetime import datetime

current_time = datetime.now().strftime("%d%m%y_%H%M%S")
result_dir = f"ga4_result_{current_time}"

if not os.path.exists(result_dir): os.makedirs(result_dir)

for domain, ga_id in sites_data.items():
    try:
        print(f"Domain: {domain}")
        df = get_ga4_data_uno(str(ga_id), days_ago=2, filter_ads=0)
        # Зберігаємо файл у створену папку
        output_file = os.path.join(result_dir, f"ga4_data_{domain.split('.')[0]}_wide.csv")
        df.to_csv(output_file, index=False)
        print(f"File saved to {output_file}")
    except Exception as e:
        print(f"Error with {domain}: {e}")