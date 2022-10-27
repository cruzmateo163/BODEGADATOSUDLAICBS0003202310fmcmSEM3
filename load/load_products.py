from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def load_products(etlpro_id):
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
        dim_products_dict = {
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
            "prod_min_price":[]
            #"etlpro_id":[]
        }
        #Reading the ext table 
        products_dim=pd.read_sql("SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS, SUPPLIER_ID, PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE FROM products_tra " , ses_db_stg)
        
        #Processing the rows
        if not products_dim.empty:
            for prodid, prod_nam, prod_des, prod_cat,prod_catid, prod_catdesc,prod_weighclass, sup_id, prod_stat, prod_listprice, prod_minprice \
                in zip(products_dim['PROD_ID'], products_dim['PROD_NAME'], products_dim['PROD_DESC'], products_dim['PROD_CATEGORY'],
                products_dim['PROD_CATEGORY_ID'], products_dim['PROD_CATEGORY_DESC'],products_dim['PROD_WEIGHT_CLASS'],products_dim['SUPPLIER_ID'],
                products_dim['PROD_STATUS'],products_dim['PROD_LIST_PRICE'],products_dim['PROD_MIN_PRICE']):
                dim_products_dict['prod_id'].append(prodid)
                dim_products_dict["prod_name"].append(prod_nam)
                dim_products_dict["prod_desc"].append(prod_des)
                dim_products_dict["prod_category"].append(prod_cat)
                dim_products_dict["prod_category_id"].append(prod_catid)
                dim_products_dict["prod_category_desc"].append(prod_catdesc)
                dim_products_dict["prod_weight_class"].append(prod_weighclass)
                dim_products_dict["supplier_id"].append(sup_id)
                dim_products_dict["prod_status"].append(prod_stat)
                dim_products_dict["prod_list_price"].append(prod_listprice)
                dim_products_dict["prod_min_price"].append(prod_minprice)
                #dim_products_dict["etlpro_id"].append(etlpro_id)
        if dim_products_dict["prod_id"]:
            df_products_dim=pd.DataFrame(dim_products_dict)
            df_products_dim.to_sql('dim_products', ses_db_sor, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass