from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def load_sales(etlpro_id):
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
        dim_sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
            #"etlpro_id":[]
        }
        #Reading the ext table 
        sales_dim=pd.read_sql("SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_tra " , ses_db_stg)
        
        #Processing the rows
        if not sales_dim.empty:
            for prodid, custid, timeid, channelid, promoid, quantityid, amountid \
               in zip(sales_dim['PROD_ID'], sales_dim['CUST_ID'],
                sales_dim['TIME_ID'], sales_dim['CHANNEL_ID'],sales_dim['PROMO_ID'], sales_dim['QUANTITY_SOLD'],sales_dim['AMOUNT_SOLD']):
                dim_sales_dict['prod_id'].append(prodid)
                dim_sales_dict["cust_id"].append(custid)
                dim_sales_dict["time_id"].append(timeid)
                dim_sales_dict["channel_id"].append(channelid)
                dim_sales_dict["promo_id"].append(promoid)
                dim_sales_dict["quantity_sold"].append(quantityid)
                dim_sales_dict["amount_sold"].append(amountid)
                #dim_sales_dict["etlpro_id"].append(etlpro_id)
        if dim_sales_dict["prod_id"]:
            df_sales_dim=pd.DataFrame(dim_sales_dict)
            df_sales_dim.to_sql('dim_sales', ses_db_sor, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass