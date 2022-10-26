from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime
from transform.transformations import obt_date

def tra_promotions(etlpro_id):
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
      
         #DICTIONARY FOR VALUES OF PROMOTIONS_TRA
        promotions_tra_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[],
            "etlpro_id":[]
        }
        #Reading the ext table 
        promotions_ext=pd.read_sql("SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE FROM promotions_ext", ses_db_stg)
        
        #Processing the rows
        if not promotions_ext.empty:
            for prom_id, prom_name, prom_cost, prom_begindate, prom_enddate\
                in zip(promotions_ext['PROMO_ID'], promotions_ext['PROMO_NAME'],promotions_ext['PROMO_COST'], 
                promotions_ext['PROMO_BEGIN_DATE'],promotions_ext['PROMO_END_DATE']):
                promotions_tra_dict['promo_id'].append(prom_id)
                promotions_tra_dict["promo_name"].append(prom_name)
                promotions_tra_dict["promo_cost"].append(prom_cost)
                promotions_tra_dict["promo_begin_date"].append(obt_date(prom_begindate))
                promotions_tra_dict["promo_end_date"].append(obt_date(prom_enddate))
                promotions_tra_dict["etlpro_id"].append(etlpro_id)
        if promotions_tra_dict["promo_id"]:
            df_promotions_tra=pd.DataFrame(promotions_tra_dict)
            df_promotions_tra.to_sql('promotions_tra', ses_db_stg, if_exists='append', index=False)
            ses_db_stg.dispose()


    except:
         traceback.print_exc()
    finally:
        pass
