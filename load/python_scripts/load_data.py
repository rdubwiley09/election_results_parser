from server_config import user,ip, pwd
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

def load_table(df, tableName, replace):
    engine = create_engine(
            'postgresql://%s:%s@%s/%s' %(user,pwd,ip,user),
            client_encoding='utf8',echo=False)
    conn = engine.connect()
    Base = declarative_base()
    if replace:
        df.to_sql(tableName, conn, if_exists='replace', index=False)
    else:
        df.to_sql(tableName, conn, if_exists='append', index=False)

def load_all_tables(tableDict, replace):
    for key in tableDict.keys():
        print("loading table %s" %(str(key)))
        load_table(tableDict[key], key, replace)

def remove_duplicates(tableName):
    engine = create_engine(
            'postgresql://%s:%s@%s/%s' %(user,pwd,ip,user),
            client_encoding='utf8',echo=False)
    conn = engine.connect()
    Base = declarative_base()
    query = "SELECT * FROM %s;" %(tableName)
    df = pd.read_sql(query, conn)
    df.drop_duplicates(inplace=True)
    df.to_sql(tableName, conn, if_exists='replace', index=False)
