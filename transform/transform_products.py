from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def tra_products(etlpro_id):
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
      
         #DICTIONARY FOR VALUES OF PRODUCTS_TRA
        products_dict = {
            "prod_id":[],
            "prod_name":[],
            "prod_desc":[],
            "prod_category":[],
            "prod_category_id":[],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[],
            "etlpro_id":[]
        }
        #Reading the ext table 
        products_ext=pd.read_sql("SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS, SUPPLIER_ID, PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE  FROM products_ext", ses_db_stg)
        
        #Processing the rows
        if not products_ext.empty:
            for prodid, prod_nam, prod_des, prod_cat,prod_catid, prod_catdesc,prod_weighclass, sup_id, prod_stat, prod_listprice, prod_minprice \
                in zip(products_ext['PROD_ID'], products_ext['PROD_NAME'], products_ext['PROD_DESC'], products_ext['PROD_CATEGORY'],
                products_ext['PROD_CATEGORY_ID'], products_ext['PROD_CATEGORY_DESC'],products_ext['PROD_WEIGHT_CLASS'],products_ext['SUPPLIER_ID'],
                products_ext['PROD_STATUS'],products_ext['PROD_LIST_PRICE'],products_ext['PROD_MIN_PRICE']):
                products_dict['prod_id'].append(prodid)
                products_dict["prod_name"].append(prod_nam)
                products_dict["prod_desc"].append(prod_des)
                products_dict["prod_category"].append(prod_cat)
                products_dict["prod_category_id"].append(prod_catid)
                products_dict["prod_category_desc"].append(prod_catdesc)
                products_dict["prod_weight_class"].append(prod_weighclass)
                products_dict["supplier_id"].append(sup_id)
                products_dict["prod_status"].append(prod_stat)
                products_dict["prod_list_price"].append(prod_listprice)
                products_dict["prod_min_price"].append(prod_minprice)
                products_dict["etlpro_id"].append(etlpro_id)
        if products_dict["prod_id"]:
            df_products_tra=pd.DataFrame(products_dict)
            df_products_tra.to_sql('products_tra',  ses_db_stg, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass
