from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime

def insert_ETLSync():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd =  'admin'
        db = 'fmcmdbstg'

        con_db_stg = Db_Connection(type, host, port, user, pwd, db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging database")

        repert=ses_db_stg.execute('INSERT INTO ETL_PRO values ()')
        r=ses_db_stg.execute('SELECT PRO_ID FROM ETL_PRO ORDER BY PRO_ID DESC LIMIT 1').scalar()
        print(r)
        return r

              
    except:
         traceback.print_exc()
    finally:
        pass
