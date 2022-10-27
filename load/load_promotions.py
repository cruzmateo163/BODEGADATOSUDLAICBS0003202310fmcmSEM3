from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime



def load_promotions(etlpro_id):
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

        #Variables
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd =  'admin'
        db = 'fmcmdbsor'

        con_db_sor = Db_Connection(type, host, port, user, pwd, db)
        ses_db_sor = con_db_sor.start()
        if ses_db_sor == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_sor == -2:
            raise Exception("Error trying to connect to the b2b_dwh_sor database")



            
      
         #DICTIONARY FOR VALUES OF DIM_CHANNELS
        dim_promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
            #"etlpro_id":[]
        }
        #Reading the ext table 
        promotions_dim=pd.read_sql("SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_tra " , ses_db_stg)
        
        #Processing the rows
        if not promotions_dim.empty:
            for prom_id, prom_name, prom_cost, prom_begindate, prom_enddate\
                in zip(promotions_dim['PROMO_ID'], promotions_dim['PROMO_NAME'],promotions_dim['PROMO_COST'], 
                promotions_dim['PROMO_BEGIN_DATE'],promotions_dim['PROMO_END_DATE']):
                dim_promotions_dict['promo_id'].append(prom_id)
                dim_promotions_dict["promo_name"].append(prom_name)
                dim_promotions_dict["promo_cost"].append(prom_cost)
                dim_promotions_dict["promo_begin_date"].append(prom_begindate)
                dim_promotions_dict["promo_end_date"].append(prom_enddate)
                #dim_promotions_dict["etlpro_id"].append(etlpro_id)
        if dim_promotions_dict["promo_id"]:
            df_promotions_dim=pd.DataFrame(dim_promotions_dict)
            df_promotions_dim.to_sql('dim_promotions', ses_db_sor, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass