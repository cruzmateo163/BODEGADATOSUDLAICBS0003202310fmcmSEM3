from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime
from transform.transformations import obt_date

def tra_sales(etlpro_id):
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
      
         #DICTIONARY FOR VALUES OF CHANNELS_TRA
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
            "etlpro_id":[]
        }
        #Reading the ext table 
        sales_ext=pd.read_sql("SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_ext", ses_db_stg)
        
        #Processing the rows
        if not sales_ext.empty:
            for prodid, custid, timeid, channelid, promoid, quantityid, amountid \
               in zip(sales_ext['PROD_ID'], sales_ext['CUST_ID'],
                sales_ext['TIME_ID'], sales_ext['CHANNEL_ID'],sales_ext['PROMO_ID'], sales_ext['QUANTITY_SOLD'],sales_ext['AMOUNT_SOLD']):
                sales_dict['prod_id'].append(prodid)
                sales_dict["cust_id"].append(custid)
                sales_dict["time_id"].append(obt_date(timeid))
                sales_dict["channel_id"].append(channelid)
                sales_dict["promo_id"].append(promoid)
                sales_dict["quantity_sold"].append(quantityid)
                sales_dict["amount_sold"].append(amountid)
                sales_dict["etlpro_id"].append(etlpro_id)
        if sales_dict["prod_id"]:
            df_sales_tra=pd.DataFrame(sales_dict)
            df_sales_tra.to_sql('sales_tra',  ses_db_stg, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass