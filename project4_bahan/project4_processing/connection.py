import os
import json
import psycopg2
import hdfs

from sqlalchemy import create_engine

# function untuk membaca file 'config.json'
def config(connection_db):
    path = os.getcwd()
    with open(path+'/'+'config.json') as file:
        conf =json.load(file)[connection_db]
    return conf

# function untuk membuat koneksi ke PostgreSQL 
def psql_conn(conf, name_conn):
    try:
        conn = psycopg2.connect(
            host=conf['host'],
            database=conf['db'],
            user=conf['user'],
            password=conf['password'],
            port=conf['port']
        )
        print(f'[INFO] Success connect PostgreSQL {name_conn}')
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/{conf['db']}")
        return conn, engine
    except Exception as e:
        print(f"[INFO] Can't connect PostgreSQL {name_conn}")
        print(str(e))

   
# function untuk membuat koneksi ke hadoop                              
def hadoop_conn(conf):
    client = conf['client']
    try:
        conn = hdfs.InsecureClient(client)
        print("bisa konek")
        return conn
    except:
        print("tak bisa konek")