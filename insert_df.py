from clickhouse_driver import Client
import pandas as pd
import re

client = Client('clickhouse://db_host') # 改成要讀取DB的Host, DB金鑰4rGMCUjFp1zR9emoeb2V8Q

def read_sql(sql):
    data, columns = client.execute(
    sql, columnar=True, with_column_types=True)
    df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})

    return df

def get_type_dict(tb_name):
    sql = f"select name, type from system.columns where table='{tb_name}';"
    df = pd.read_sql(sql,conn)
    df = df.set_index('name')
    type_dict = df.to_dict('dict')['type']
    
    return type_dict

def to_sql(df, tb_name):
    type_dict = get_type_dict(tb_name)
    columns = list(type_dict.keys())
    print(type_dict)
    
    # 類型處理
    for i in range(len(columns)):
        col_name = columns[i]
        col_type = type_dict[col_name]
        if 'Date' in col_type:
            df[col_name] = pd.to_datetime(df[col_name])
        elif 'Int' in col_type:
            df[col_name] = df[col_name].astype('int')
        elif 'Float' in col_type:
            if df[col_name].dtype == 'O':
                try:
                    df[col_name] = df[col_name].str.replace(',','').astype('float')
                except:
                    df[col_name] = df[col_name].astype('float')
            else:
                df[col_name] = df[col_name].astype('float')
        elif col_type == 'String':
            df[col_name] = df[col_name].astype('str').fillna('')
    # df數據存入clickhouse
    cols = ','.join(columns)
    data = df.to_dict('records')
    cursor.execute(f"INSERT INTO recomm.{tb_name} ({cols}) VALUES "+ ",".join(["(%s)" % ",".join(["'%s'" %v for v in list(d.values())]) for d in ware.head(2).to_dict('records')]))
