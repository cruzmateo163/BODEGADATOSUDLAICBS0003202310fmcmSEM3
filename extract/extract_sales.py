from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_sales():
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
      
         #DICTIONARY FOR VALUES OF SALES
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
        }
        #Reading the CVS file
        sales_csv=pd.read_csv("csvs/sales.csv")
        print(sales_csv)
        #Processing the CVS file content
        if not sales_csv.empty:
            for prodid, custid, timeid, channelid, promoid, quantityid, amountid \
                in zip(sales_csv['PROD_ID'], sales_csv['CUST_ID'],
                sales_csv['TIME_ID'], sales_csv['CHANNEL_ID'],sales_csv['PROMO_ID'], sales_csv['QUANTITY_SOLD'],sales_csv['AMOUNT_SOLD']):
                sales_dict['prod_id'].append(prodid)
                sales_dict["cust_id"].append(custid)
                sales_dict["time_id"].append(timeid)
                sales_dict["channel_id"].append(channelid)
                sales_dict["promo_id"].append(promoid)
                sales_dict["quantity_sold"].append(quantityid)
                sales_dict["amount_sold"].append(amountid)
        if sales_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE sales")
            df_sales_ext=pd.DataFrame(sales_dict)
            df_sales_ext.to_sql('sales', ses_db_stg, if_exists='append', index=False)


    except:
         traceback.print_exc()
    finally:
        pass
