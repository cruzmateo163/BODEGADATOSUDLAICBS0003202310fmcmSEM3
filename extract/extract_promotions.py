from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_promotions():
    try:
        #Variables
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
      
         #DICTIONARY FOR VALUES OF promotions
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        #Reading the CVS file
        promotions_csv=pd.read_csv("csvs/promotions.csv")
        print(promotions_csv)
        #Processing the CVS file content
        if not promotions_csv.empty:
            for prom_id, prom_name, prom_cost, prom_begindate, prom_enddate\
                in zip(promotions_csv['PROMO_ID'], promotions_csv['PROMO_NAME'],promotions_csv['PROMO_COST'], 
                promotions_csv['PROMO_BEGIN_DATE'],promotions_csv['PROMO_END_DATE']):
                promotions_dict['promo_id'].append(prom_id)
                promotions_dict["promo_name"].append(prom_name)
                promotions_dict["promo_cost"].append(prom_cost)
                promotions_dict["promo_begin_date"].append(prom_begindate)
                promotions_dict["promo_end_date"].append(prom_enddate)
                
        if promotions_dict["promo_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE promotions_ext")
            df_promotions_ext=pd.DataFrame(promotions_dict)
            df_promotions_ext.to_sql('promotions_ext', ses_db_stg, if_exists='append', index=False)


    except:
         traceback.print_exc()
    finally:
        pass
