from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def tra_countries(etlpro_id):
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
      
         #DICTIONARY FOR VALUES OF COUNTRIES_TRA
        countries_tra_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            "etlpro_id":[]
        }
        #Reading the ext table 
        countries_ext=pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION,COUNTRY_REGION_ID FROM countries_ext", ses_db_stg)
        
        #Processing the rows
        if not countries_ext.empty:
            for countryid, countryname, countryregion, countryregionid \
                in zip(countries_ext['COUNTRY_ID'], countries_ext['COUNTRY_NAME'],
                countries_ext['COUNTRY_REGION'], countries_ext['COUNTRY_REGION_ID']):
                countries_tra_dict['country_id'].append(countryid)
                countries_tra_dict["country_name"].append(countryname)
                countries_tra_dict["country_region"].append(countryregion)
                countries_tra_dict["country_region_id"].append(countryregionid)
                countries_tra_dict["etlpro_id"].append(etlpro_id)
        if countries_tra_dict["country_id"]:
            df_countries_tra=pd.DataFrame(countries_tra_dict)
            df_countries_tra.to_sql('countries_tra', ses_db_stg, if_exists='append', index=False)
            ses_db_stg.dispose()


    except:
         traceback.print_exc()
    finally:
        pass
