### Preprocessing daily wholesale data
### To help writing preprocessing dag script

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from datetime import datetime

today_date = datetime.now().strftime("%Y-%m-%d")
data = pd.read_csv('./data/daily_scraping/wholesale_{}.csv'.format(today_date),
                                                encoding='utf-8', index_col=0)
encoder = LabelEncoder()

# Generate kind_name_new column
def make_product_name(item_name, kind_name):
    if "깐마늘" in item_name: # ok
        result = kind_name.split("(")[0] + '(국산)(' + kind_name.split("(")[1] + '('+kind_name.split("(")[2]        
    elif "피마늘" in item_name:
        result = '피마늘(' + kind_name[:2] + ")" + kind_name[2:]
    else:
        if item_name in kind_name: # item name이 kind name안에 있을 경우 
            result = kind_name # 그냥 kind name 그대로           
        else:
            result = item_name + "(" + kind_name.split("(")[0] + ")(" + kind_name.split("(")[1]
    return result

def preprocess_data(data):

    # 1. Replace "-" with np.NaN
    data['dpr1'].replace("-", np.NaN, inplace=True)
    data['dpr2'].replace("-", np.NaN, inplace=True)

    # 2. Generate kind_name_new column with item_name & kind_name
    data['kind_name_new'] = data.apply(lambda x: make_product_name(x['item_name'], x['kind_name']), axis=1)
    
    # 3. Generate category code
    data['category_cd'] = data['item_code'].map(lambda x: "LW" if 500<=x<600 else ("SW" if x>=600 else "AW"))
    
    # 4. Generate kind_name_rank with kind_name_new & rank
    data['kind_name_rank'] = data['kind_name_new'] + data['rank']
    
    # 5. Generate kind_name_rank_label = Label encoded kind_name_rank
    # encoder = LabelEncoder()
    data['kind_name_rank_label'] = pd.Series(encoder.fit_transform(data['kind_name_rank'])).map(lambda x: "A"+str(x).zfill(5))
    
    # 6. Generate product code
    data['p_code'] = data['category_cd'] + "_" + data['kind_name_rank_label']

    # 7. Feature selection
    data.rename(columns = {'item_name':'p_group', 'day1':'date', 'dpr1':'price_1', 'dpr2':'price_2'}, inplace = True)
    data['category_code'] = data['p_code']
    data = data[['category_code', 'p_code', 'p_group', 'kind_name_new', 'rank', 'unit', 'price_1', 'price_2']]

    # 8. Save as csv file
    # data.to_csv('./data/daily_preprocessing/wholesale_{}_preprocessed.csv'.format(today_date), encoding = 'utf-8')
    data.to_csv('./data/daily_preprocessing/wholesale_2022-06-23_preprocessed.csv', encoding = 'utf-8')
    print("Preprocessing succeeded!")

preprocess_data(data)