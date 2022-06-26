### preprocessing dag prototype

# https://www.youtube.com/watch?v=eZfD6x9FJ4E&ab_channel=BIInsightsInc

# 실제라면 scraping dag를 통해 수집한 데이터를 postres(or 다른 DB)에 여러 테이블로 나누어 저장하고
# 그 테이블들을 각각 불러와서 전처리를 해야하지만
# 프로토타입이므로 걍 기존에 작성했던 전처리 코드 때려넣어봄

from asyncio import Task
import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

########### Extraction tasks ###########
# E_1
@task()
def get_src_table():
    hook = MsSqlHook(mssql_conn_id="sqlserver")
    sql = """select t.name as table_name
    from sys.tables t where t.name in ('agricproduct_wholesale', 'product', 'price')"""
    df = hook.get_pandas_df(sql)
    tbl_dict = df.to_dict('dict')
    return tbl_dict
# E_2
@task()
def load_src_data(tbl_dict: dict):
    conn = BaseHook.get_connection("postgres")
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    all_tbl_name = []
    start_time = time.time()
    # access the table_name element in dictionaries
    for k,v in tbl_dict['table_name'].items():
        all_tbl_name.append(v)
        rows_imported = 0
        sql = f'select * from {v}'
        hook = MsSqlHook(mssql_conn_id="sqlserver")
        df = hook.get_pandas_df(sql)
        print(f'importing {rows_imported+len(df)} rows for table {v}')
        df.to_sql(f'src_{v}', engine, if_exists = 'replace', index = False)
        rows_imported += len(df)
        print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    print('Data imported successful.')
    return all_tbl_name

########### Transformation tasks ###########

def make_product_name(item_name, kind_name):
    if "깐마늘" in item_name: # ok
        result = kind_name.split("(")[0] + '(국산)(' + kind_name.split("(")[1] + '('+kind_name.split("(")[2]        
    elif "피마늘" in item_name:
        result = '피마늘(' + kind_name[:2] + ")" + kind_name[2:]
    else:
        if item_name in kind_name:
            result = kind_name
        else:
            result = item_name + "(" + kind_name.split("(")[0] + ")(" + kind_name.split("(")[1]
    return result

def preprocess_data(data): # 이대로 괜찮은가 or 각 과정을 하나의 task로 분할해서 수행해야 하는가

    # 1. Replace "-" with np.NaN
    data['dpr1'].replace("-", np.NaN, inplace=True)

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
    data.rename(columns = {'item_name':'p_group', 'day1':'date', 'dpr1':'price'}, inplace = True)
    data['category_code'] = data['p_code']
    data = data[['category_code', 'p_code', 'p_group', 'kind_name_new', 'rank', 'unit', 'price']]

    return data

# T_1
@task()
def transform_srcProduct():
    conn = BaseHook.get_connection("posgres")
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    wholesale = pd.read_sql_query('SELECT * FROM public_api."agricproduct_wholesale', engine)
    revised = preprocess_data(wholesale)
    revised.to_sql(f'pp_wholesale', engine, if_exists='replace', index=False)
    return {"Table(s) processed": "Data imported successful"}

########### Load tasks ###########
# table join하는 과정인 것 같아서 일단 보류
# L_1
@task()
def prdWholesale_model():
    conn = BaseHook.get_connection("posgres")
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pp_wholesale = pd.read_sql_query('SELECT * FROM public_api."pp_wholesale"', engine)
    pp_wholesale.to_sql(f'pp_wholesale_loaded', engine, if_exists='replace', index=False)
    return {"Table(s) processed": "Data loaded successful"}

with DAG(
    dag_id = "wholesale_etl_dag",
    schedule_interval = "@daily",
    start_date = datetime(2022,6,30),
    catchup = False,
    tags = ["dag prototype"]
) as dag:

    with TaskGroup(
        "Extract Wholesale Data",
        tooltip = "Extract and load soure data") as extract_load_src:
        src_wholesale_tbls = get_src_table()
        load_wholesale = load_src_data(src_wholesale_tbls)

        # Define order
        src_wholesale_tbls >> load_wholesale

    with TaskGroup(
        "Transform Wholesale Data",
        tooltip = "Transform and stage data") as transform_src_wholesale:
        transform_srcWholesale = transform_srcProduct()

        # Define order
        transform_srcWholesale

    with TaskGroup(
        "Load Wholesale Data",
        tooltip = "Final Wholesale model") as load_wholesale_model:
        prd_Wholesale_model = prdWholesale_model()

        # Define order
        prd_Wholesale_model

    extract_load_src >> transform_src_wholesale >> load_wholesale_model