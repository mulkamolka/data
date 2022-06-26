### Scraping daily wholesale data
### To help writing scraping dag script

import requests
import json
import pandas as pd
from datetime import date, datetime, timedelta
import itertools
from tqdm import tqdm

# parameter setting
url = 'http://www.kamis.or.kr/service/price/xml.do?action=dailyPriceByCategoryList'
p_product_cls_code = '02' # wholesale only
item_code = ['100','200','300','400','500','600'] # list of item codes
p_country_code = '1101' # seoul only
today_date = datetime.now().strftime("%Y-%m-%d")

def get_wholesale_price(url, p_product_cls_code, p_item_category_code, p_country_code, scrap_data):
  params ={
    'p_cert_key' : 'your_key' ,
    'p_cert_id' : 'your_id',
    'p_returntype' : 'json', 
    'p_product_cls_code' : p_product_cls_code,
    'p_item_category_code' : p_item_category_code,
    'p_country_code' : p_country_code,
    'p_regday' : scrap_data, # on a daily basis
    'p_convert_kg_yn' : 'N',
    }
    
  response = requests.get(url, params=params)
  content = response.text
  j_content = json.loads(content)
  if(type(j_content['data']) == list): return []
  wholesale_price = j_content['data']['item']
  return wholesale_price


wholesale_price_data= [] # data for all item codes for a day

for item in tqdm(item_code):
    wholesale_price = get_wholesale_price(url, p_product_cls_code, item, p_country_code, today_date)
    if(wholesale_price == []) : continue
    for i in range(len(wholesale_price)):
      tmp= dict(itertools.islice(wholesale_price[i].items(), 9)) # data for each product
      tmp['day1'] = today_date
      # tmp['m_code'] = 'W'+p_country_code # in case of scraping other region
      wholesale_price_data.append(tmp)
    print("\nfinished scrapping I%s data"%item)

p = pd.DataFrame(wholesale_price_data)
p.to_csv(f'./data/daily_scraping/wholesale_{today_date}.csv') # make csv file per item