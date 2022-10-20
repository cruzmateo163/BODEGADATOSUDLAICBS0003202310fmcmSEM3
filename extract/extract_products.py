from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback


def ext_products():
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
      
         #DICTIONARY FOR VALUES OF products
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
            "prod_min_price":[]
        }
        #Reading the CVS file
        products_csv=pd.read_csv("csvs/products.csv")
        print(products_csv)
        #Processing the CVS file content
        if not products_csv.empty:
            for prodid, prod_nam, prod_des, prod_cat,prod_catid, prod_catdesc,prod_weighclass, sup_id, prod_stat, prod_listprice, prod_minprice \
                in zip(products_csv['PROD_ID'], products_csv['PROD_NAME'], products_csv['PROD_DESC'], products_csv['PROD_CATEGORY'],
                products_csv['PROD_CATEGORY_ID'], products_csv['PROD_CATEGORY_DESC'],products_csv['PROD_WEIGHT_CLASS'],products_csv['SUPPLIER_ID'],
                products_csv['PROD_STATUS'],products_csv['PROD_LIST_PRICE'],products_csv['PROD_MIN_PRICE']):
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
        if products_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE products")
            df_products_ext=pd.DataFrame(products_dict)
            df_products_ext.to_sql('products', ses_db_stg, if_exists='append', index=False)


    except:
         traceback.print_exc()
    finally:
        pass
