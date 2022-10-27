from pickle import FALSE
from util.db_connection import Db_Connection
import pandas as pd
import traceback
from datetime import datetime


def load_countries(etlpro_id):
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
        dim_countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            #"etlpro_id":[]
        }
        #Reading the ext table 
        countries_dim=pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION,COUNTRY_REGION_ID FROM countries_tra " , ses_db_stg)
        
        #Processing the rows
        if not countries_dim.empty:
            for countryid, countryname, countryregion, countryregionid \
                in zip(countries_dim['COUNTRY_ID'], countries_dim['COUNTRY_NAME'],
                countries_dim['COUNTRY_REGION'], countries_dim['COUNTRY_REGION_ID']):
                dim_countries_dict['country_id'].append(countryid)
                dim_countries_dict["country_name"].append(countryname)
                dim_countries_dict["country_region"].append(countryregion)
                dim_countries_dict["country_region_id"].append(countryregionid)
                #dim_countries_dict["etlpro_id"].append(etlpro_id)
        if dim_countries_dict["country_id"]:
            df_countries_dim=pd.DataFrame(dim_countries_dict)
            df_countries_dim.to_sql('dim_countries', ses_db_sor, if_exists='append', index=False)
            ses_db_stg.dispose()


            
    except:
         traceback.print_exc()
    finally:
        pass
