# importing libraries
import requests
import json
import pandas as pd
from datetime import date, datetime, timedelta
import itertools
from tqdm import tqdm

# parameter setting
url = 'http://www.kamis.or.kr/service/price/xml.do?action=dailyPriceByCategoryList'
p_product_cls_code = '02' # 도매 only
item_code = ['100','200','300','400','500','600'] # list of item codes
p_country_code = '1101' # seoul only
today_date = datetime.now().strftime("%Y-%m-%d")

# set crawling range
now = datetime.now()
scrap_span = 365 # data for 5 years
scrap_dates = [(now-timedelta(days=i)).strftime("%Y-%m-%d") for i in range(scrap_span)]

def get_wholesale_price(url, p_product_cls_code, p_item_category_code, p_country_code, start_date):
  params ={
    'p_cert_key' : 'your_key' ,  # api 인증키
    'p_cert_id' : 'your_id', # 요청자 id
    'p_returntype' : 'json', 
    'p_product_cls_code' : p_product_cls_code, # 도소매 구분. 01=소매, 02=도매
    'p_item_category_code' : p_item_category_code, # 100:식량작물, 200:채소류, 300:특용작물 ...
    'p_country_code' : p_country_code, # 지역코드
    'p_regday' : start_date, # 기준 일자
    'p_convert_kg_yn' : 'N', # kg 변환여부. default = N
    }
    
  response = requests.get(url, params=params)
  content = response.text
  j_content = json.loads(content)
  if(type(j_content['data']) == list): return []
  wholesale_price = j_content['data']['item']
  return wholesale_price


wholesale_price_data= []

for item in tqdm(item_code): # looping item code
  for day in tqdm(scrap_dates): # looping through 5 years
    wholesale_price = get_wholesale_price(url, p_product_cls_code, item, p_country_code, day)
    if(wholesale_price == []) : continue
    for i in range(len(wholesale_price)):
      tmp= dict(itertools.islice(wholesale_price[i].items(), 9)) # wholesale_price에서 앞 item 9개만 slicing
      tmp['day1'] = day
      # tmp['m_code'] = 'W'+p_country_code
      wholesale_price_data.append(tmp) # 리스트에 각 항목을 dict로 append
  print("\nfinished scrapping I%s data"%item)
p = pd.DataFrame(wholesale_price_data)
# p= p.set_index('day1') # 오늘 날짜 = 수집일을 인덱스로
p.to_csv(f'./data/wholesale_{today_date}.csv') # make csv file per item